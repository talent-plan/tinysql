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

package mocktikv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

const requestMaxSize = 8 * 1024 * 1024

func checkGoContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         locked.Key.Raw(),
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
				TxnSize:     locked.TxnSize,
				LockType:    locked.LockType,
			},
		}
	}
	if alreadyExist, ok := errors.Cause(err).(*ErrKeyAlreadyExist); ok {
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: alreadyExist.Key,
			},
		}
	}
	if writeConflict, ok := errors.Cause(err).(*ErrConflict); ok {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				Key:              writeConflict.Key,
				ConflictTs:       writeConflict.ConflictTS,
				ConflictCommitTs: writeConflict.ConflictCommitTS,
				StartTs:          writeConflict.StartTS,
			},
		}
	}
	if retryable, ok := errors.Cause(err).(ErrRetryable); ok {
		return &kvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	if expired, ok := errors.Cause(err).(*ErrCommitTSExpired); ok {
		return &kvrpcpb.KeyError{
			CommitTsExpired: &expired.CommitTsExpired,
		}
	}
	if tmp, ok := errors.Cause(err).(*ErrTxnNotFound); ok {
		return &kvrpcpb.KeyError{
			TxnNotFound: &tmp.TxnNotFound,
		}
	}
	return &kvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*kvrpcpb.KeyError {
	var keyErrors = make([]*kvrpcpb.KeyError, 0)
	for _, err := range errs {
		if err != nil {
			keyErrors = append(keyErrors, convertToKeyError(err))
		}
	}
	return keyErrors
}

func convertToPbPairs(pairs []Pair) []*kvrpcpb.KvPair {
	kvPairs := make([]*kvrpcpb.KvPair, 0, len(pairs))
	for _, p := range pairs {
		var kvPair *kvrpcpb.KvPair
		if p.Err == nil {
			kvPair = &kvrpcpb.KvPair{
				Key:   p.Key,
				Value: p.Value,
			}
		} else {
			kvPair = &kvrpcpb.KvPair{
				Error: convertToKeyError(p.Err),
			}
		}
		kvPairs = append(kvPairs, kvPair)
	}
	return kvPairs
}

// rpcHandler mocks tikv's side handler behavior. In general, you may assume
// TiKV just translate the logic from Go to Rust.
type rpcHandler struct {
	cluster   *Cluster
	mvccStore MVCCStore

	// storeID stores id for current request
	storeID uint64
	// startKey is used for handling normal request.
	startKey []byte
	endKey   []byte
	// rawStartKey is used for handling coprocessor request.
	rawStartKey []byte
	rawEndKey   []byte
	// isolationLevel is used for current request.
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
}

func (h *rpcHandler) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != h.storeID {
		return &errorpb.Error{
			Message:       *proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := h.cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	var storePeer, leaderPeer *metapb.Peer
	for _, p := range region.Peers {
		if p.GetStoreId() == h.storeID {
			storePeer = p
		}
		if p.GetId() == leaderID {
			leaderPeer = p
		}
	}
	// The Store does not contain a Peer of the Region.
	if storePeer == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	if leaderPeer == nil {
		return &errorpb.Error{
			Message: *proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the Store is not leader.
	if storePeer.GetId() != leaderPeer.GetId() {
		return &errorpb.Error{
			Message: *proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		nextRegion, _ := h.cluster.GetRegionByKey(region.GetEndKey())
		currentRegions := []*metapb.Region{region}
		if nextRegion != nil {
			currentRegions = append(currentRegions, nextRegion)
		}
		return &errorpb.Error{
			Message: *proto.String("epoch not match"),
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: currentRegions,
			},
		}
	}
	h.startKey, h.endKey = region.StartKey, region.EndKey
	h.isolationLevel = ctx.IsolationLevel
	h.resolvedLocks = ctx.ResolvedLocks
	return nil
}

func (h *rpcHandler) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (h *rpcHandler) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	if err := h.checkRequestContext(ctx); err != nil {
		return err
	}
	return h.checkRequestSize(size)
}

func (h *rpcHandler) checkKeyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, []byte(NewMvccKey(key)))
}

func (h *rpcHandler) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvGet: key not in region")
	}

	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (h *rpcHandler) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	endKey := MvccKey(h.endKey).Raw()
	var pairs []Pair
	if !req.Reverse {
		if !h.checkKeyInRegion(req.GetStartKey()) {
			panic("KvScan: startKey not in region")
		}
		if len(req.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.EndKey), h.endKey) < 0) {
			endKey = req.EndKey
		}
		pairs = h.mvccStore.Scan(req.GetStartKey(), endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	} else {
		// TiKV use range [end_key, start_key) for reverse scan.
		// Should use the req.EndKey to check in region.
		if !h.checkKeyInRegion(req.GetEndKey()) {
			panic("KvScan: startKey not in region")
		}

		// TiKV use range [end_key, start_key) for reverse scan.
		// So the req.StartKey actually is the end_key.
		if len(req.StartKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.StartKey), h.endKey) < 0) {
			endKey = req.StartKey
		}

		pairs = h.mvccStore.ReverseScan(req.EndKey, endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	}

	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPrewrite: key not in region")
		}
	}
	errs := h.mvccStore.Prewrite(req)
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvCommit: key not in region")
		}
	}
	var resp kvrpcpb.CommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) handleKvCheckTxnStatus(req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusResponse {
	if !h.checkKeyInRegion(req.PrimaryKey) {
		panic("KvCheckTxnStatus: key not in region")
	}
	var resp kvrpcpb.CheckTxnStatusResponse
	ttl, commitTS, action, err := h.mvccStore.CheckTxnStatus(req.GetPrimaryKey(), req.GetLockTs(), req.GetCallerStartTs(), req.GetCurrentTs(), req.GetRollbackIfNotExist())
	if err != nil {
		resp.Error = convertToKeyError(err)
	} else {
		resp.LockTtl, resp.CommitVersion, resp.Action = ttl, commitTS, action
	}
	return &resp
}

func (h *rpcHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.ResolveLock(startKey, endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawGetResponse{
			Error: "not implemented",
		}
	}
	return &kvrpcpb.RawGetResponse{
		Value: rawKV.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawPutResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (h *rpcHandler) handleKvRawScan(req *kvrpcpb.RawScanRequest) *kvrpcpb.RawScanResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		errStr := "not implemented"
		return &kvrpcpb.RawScanResponse{
			RegionError: &errorpb.Error{
				Message: errStr,
			},
		}
	}

	var pairs []Pair
	upperBound := h.endKey
	pairs = rawKV.RawScan(
		req.StartKey,
		upperBound,
		int(req.GetLimit()),
	)

	return &kvrpcpb.RawScanResponse{
		Kvs: convertToPbPairs(pairs),
	}
}

// RPCClient sends kv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at tikv's side.
type RPCClient struct {
	Cluster   *Cluster
	MvccStore MVCCStore
	done      chan struct{}
}

// NewRPCClient creates an RPCClient.
// Note that close the RPCClient may close the underlying MvccStore.
func NewRPCClient(cluster *Cluster, mvccStore MVCCStore) *RPCClient {
	done := make(chan struct{})
	return &RPCClient{
		Cluster:   cluster,
		MvccStore: mvccStore,
		done:      done,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*metapb.Store, error) {
	store, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("connect fail")
	}
	if store.GetState() == metapb.StoreState_Offline ||
		store.GetState() == metapb.StoreState_Tombstone {
		return nil, errors.New("connection refused")
	}
	return store, nil
}

func (c *RPCClient) checkArgs(ctx context.Context, addr string) (*rpcHandler, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	store, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	handler := &rpcHandler{
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return handler, nil
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	reqCtx := &req.Context
	resp := &tikvrpc.Response{}

	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	switch req.Type {
	case tikvrpc.CmdGet:
		r := req.Get()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvGet(r)
	case tikvrpc.CmdScan:
		r := req.Scan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvScan(r)

	case tikvrpc.CmdPrewrite:
		failpoint.Inject("rpcPrewriteResult", func(val failpoint.Value) {
			switch val.(string) {
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		})

		r := req.Prewrite()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvPrewrite(r)
	case tikvrpc.CmdCommit:
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			switch val.(string) {
			case "timeout":
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}},
				}, nil)
			}
		})

		r := req.Commit()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvCommit(r)
		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case tikvrpc.CmdCheckTxnStatus:
		r := req.CheckTxnStatus()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CheckTxnStatusResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvCheckTxnStatus(r)
	case tikvrpc.CmdBatchRollback:
		r := req.BatchRollback()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvBatchRollback(r)
	case tikvrpc.CmdResolveLock:
		r := req.ResolveLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvResolveLock(r)
	case tikvrpc.CmdRawGet:
		r := req.RawGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawGet(r)
	case tikvrpc.CmdRawPut:
		r := req.RawPut()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawPut(r)
	case tikvrpc.CmdRawDelete:
		r := req.RawDelete()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawDelete(r)
	case tikvrpc.CmdRawScan:
		r := req.RawScan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawScan(r)
	case tikvrpc.CmdCop:
		r := req.Cop()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &coprocessor.Response{RegionError: err}
			return resp, nil
		}
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		var res *coprocessor.Response
		switch r.GetTp() {
		case kv.ReqTypeDAG:
			res = handler.handleCopDAGRequest(r)
		case kv.ReqTypeAnalyze:
			res = handler.handleCopAnalyzeRequest(r)
		default:
			panic(fmt.Sprintf("unknown coprocessor request type: %v", r.GetTp()))
		}
		resp.Resp = res
	default:
		return nil, errors.Errorf("unsupported this request type %v", req.Type)
	}
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	close(c.done)
	if raw, ok := c.MvccStore.(io.Closer); ok {
		return raw.Close()
	}
	return nil
}
