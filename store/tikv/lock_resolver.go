// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync"
)

// ResolvedCacheSize is max number of cached txn status.
const ResolvedCacheSize = 2048

// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
const bigTxnThreshold = 16

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	store Storage
	mu    struct {
		sync.RWMutex
		// resolved caches resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]TxnStatus
		recentResolved *list.List
	}
}

func newLockResolver(store Storage) *LockResolver {
	r := &LockResolver{
		store: store,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.recentResolved = list.New()
	return r
}

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
func NewLockResolver(etcdAddrs []string) (*LockResolver, error) {
	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	spkv, err := NewEtcdSafePointKV(etcdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(), false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus struct {
	ttl      uint64
	commitTS uint64
	action   kvrpcpb.Action
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return uint64(s.commitTS) }

// By default, locks after 3000ms is considered unusual (the client created the
// lock might be dead). Other client may cleanup this kind of lock.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// TODO: Consider if it's appropriate.
var maxLockTTL uint64 = 120000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// Lock represents a lock from tikv server.
type Lock struct {
	Key      []byte
	Primary  []byte
	TxnID    uint64
	TTL      uint64
	TxnSize  uint64
	LockType kvrpcpb.Op
}

func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	prettyWriteKey(buf, l.Key)
	buf.WriteString(", primary: ")
	prettyWriteKey(buf, l.Primary)
	return fmt.Sprintf("%s, txnStartTS: %d, ttl: %d, type: %s", buf.String(), l.TxnID, l.TTL, l.LockType)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:     l.GetKey(),
		Primary: l.GetPrimaryLock(),
		TxnID:   l.GetLockVersion(),
		TTL:     l.GetLockTtl(),
	}
}

func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
// 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
//    old are considered orphan locks and will be handled later. If all locks
//    are expired then all locks will be resolved so the returned `ok` will be
//    true, otherwise caller should sleep a while before retry.
// 2) For each lock, query the primary key to get txn(which left the lock)'s
//    commit status.
// 3) Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
//    the same transaction.
func (lr *LockResolver) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	var msBeforeTxnExpired txnExpireTime
	if len(locks) == 0 {
		return msBeforeTxnExpired.value(), nil, nil
	}

	var pushFail bool
	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	pushed := make([]uint64, 0, len(locks))
	for _, l := range locks {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS)
		if err != nil {
			msBeforeTxnExpired.update(0)
			err = errors.Trace(err)
			return msBeforeTxnExpired.value(), nil, err
		}

		if status.ttl == 0 {
			// If the lock is committed or rollbacked, resolve lock.
			cleanRegions, exists := cleanTxns[l.TxnID]
			if !exists {
				cleanRegions = make(map[RegionVerID]struct{})
				cleanTxns[l.TxnID] = cleanRegions
			}

			err = lr.resolveLock(bo, l, status, cleanRegions)
			if err != nil {
				msBeforeTxnExpired.update(0)
				err = errors.Trace(err)
				return msBeforeTxnExpired.value(), nil, err
			}
		} else {
			// Update the txn expire time.
			msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl)
			msBeforeTxnExpired.update(msBeforeLockExpired)
			// In the write conflict scenes, callerStartTS is set to 0 to avoid unnecessary push minCommitTS operation.
			if callerStartTS > 0 {
				pushFail = true
				continue
			}
		}
	}
	if pushFail {
		// If any of the lock fails to push minCommitTS, don't return the pushed array.
		pushed = nil
	}

	return msBeforeTxnExpired.value(), pushed, nil
}

type txnExpireTime struct {
	initialized bool
	txnExpire   int64
}

func (t *txnExpireTime) update(lockExpire int64) {
	if lockExpire <= 0 {
		lockExpire = 0
	}
	if !t.initialized {
		t.txnExpire = lockExpire
		t.initialized = true
		return
	}
	if lockExpire < t.txnExpire {
		t.txnExpire = lockExpire
	}
}

func (t *txnExpireTime) value() int64 {
	if !t.initialized {
		return 0
	}
	return t.txnExpire
}

// GetTxnStatus queries tikv-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error) {
	var status TxnStatus
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	currentTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, true)
}

func (lr *LockResolver) getTxnStatusFromLock(bo *Backoffer, l *Lock, callerStartTS uint64) (TxnStatus, error) {
	var currentTS uint64
	var err error
	var status TxnStatus
	currentTS, err = lr.store.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return TxnStatus{}, err
	}

	rollbackIfNotExist := false
	for {
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS, rollbackIfNotExist)
		if err == nil {
			return status, nil
		}
		return TxnStatus{}, err
	}
}

// getTxnStatus sends the CheckTxnStatus request to the TiKV server.
// When rollbackIfNotExist is false, the caller should be careful with the txnNotFoundErr error.
func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool) (TxnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	// CheckTxnStatus may meet the following cases:
	// 1. LOCK
	// 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
	// 1.2 Lock TTL -- active transaction holding the lock.
	// 2. NO LOCK
	// 2.1 Txn Committed
	// 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
	// 2.3 No lock -- concurrence prewrite.

	var status TxnStatus
	req := tikvrpc.NewRequest(tikvrpc.CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             txnID,
		CallerStartTs:      callerStartTS,
		CurrentTs:          currentTS,
		RollbackIfNotExist: rollbackIfNotExist,
	})
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return status, errors.Trace(err)
		}
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return status, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return status, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return status, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return status, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected err: %s, tid: %v", keyErr, txnID)
			logutil.BgLogger().Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
		status.action = cmdResp.Action
		if cmdResp.LockTtl != 0 {
			status.ttl = cmdResp.LockTtl
		} else {
			status.commitTS = cmdResp.CommitVersion
			lr.saveResolved(txnID, status)
		}
		return status, nil
	}
}

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, cleanRegions map[RegionVerID]struct{}) error {
	cleanWholeRegion := l.TxnSize >= bigTxnThreshold
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		lreq := &kvrpcpb.ResolveLockRequest{
			StartVersion: l.TxnID,
		}
		if status.IsCommitted() {
			lreq.CommitVersion = status.CommitTS()
		}
		if l.TxnSize < bigTxnThreshold {
			// Only resolve specified keys when it is a small transaction,
			// prevent from scanning the whole region in this case.
			lreq.Keys = [][]byte{l.Key}
			if !status.IsCommitted() {
				logutil.BgLogger().Info("resolveLock rollback", zap.String("lock", l.String()))
			}
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq)
		resp, err := lr.store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return err
		}
		if cleanWholeRegion {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}
