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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *tikvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	lockKeys  [][]byte
	lockedMap map[string]struct{}
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *kv.Variables
	committer *twoPhaseCommitter

	valid bool
	dirty bool
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTikvTxnWithStartTS(store, startTS, store.nextReplicaReadSeed())
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *tikvStore, startTS uint64, replicaReadSeed uint32) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver, replicaReadSeed)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
		lockedMap: map[string]struct{}{},
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      kv.DefaultVars,
	}, nil
}

func (txn *tikvTxn) SetVars(vars *kv.Variables) {
	txn.vars = vars
	txn.snapshot.vars = vars
}

// SetCap sets the transaction's MemBuffer capability, to reduce memory allocations.
func (txn *tikvTxn) SetCap(cap int) {
	txn.us.SetCap(cap)
}

// Reset reset tikvTxn's membuf.
func (txn *tikvTxn) Reset() {
	txn.us.Reset()
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = txn.store.CheckVisibility(txn.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txn.setCnt++

	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
	switch opt {
	case kv.Priority:
		txn.snapshot.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		txn.snapshot.notFillCache = val.(bool)
	case kv.SyncLog:
		txn.snapshot.syncLog = val.(bool)
	case kv.KeyOnly:
		txn.snapshot.keyOnly = val.(bool)
	case kv.SnapshotTS:
		txn.snapshot.setSnapshotTS(val.(uint64))
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	defer txn.close()

	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	// connID is used for log.
	var connID uint64
	val := ctx.Value(sessionctx.ConnID)
	if val != nil {
		connID = val.(uint64)
	}

	var err error
	committer := txn.committer
	if committer == nil {
		committer, err = newTwoPhaseCommitter(txn, connID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if err := committer.initKeysAndMutations(); err != nil {
		return errors.Trace(err)
	}
	if len(committer.keys) == 0 {
		return nil
	}

	err = committer.execute(ctx)
	return errors.Trace(err)
}

func (txn *tikvTxn) close() {
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	txn.close()
	logutil.BgLogger().Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	return nil
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	// Exclude keys that are already locked.
	keys := make([][]byte, 0, len(keysInput))
	txn.mu.Lock()
	for _, key := range keysInput {
		if _, ok := txn.lockedMap[string(key)]; !ok {
			keys = append(keys, key)
		}
	}
	txn.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}
	txn.mu.Lock()
	txn.lockKeys = append(txn.lockKeys, keys...)
	for _, key := range keys {
		txn.lockedMap[string(key)] = struct{}{}
	}
	txn.dirty = true
	txn.mu.Unlock()
	return nil
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	return txn.valid
}

func (txn *tikvTxn) Len() int {
	return txn.us.Len()
}

func (txn *tikvTxn) Size() int {
	return txn.us.Size()
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.us.GetMemBuffer()
}
