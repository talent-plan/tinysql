// Copyright 2017 PingCAP, Inc.
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

package tikvrpc

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tikvpb"
	"github.com/pingcap/errors"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdBatchRollback
	CmdResolveLock
	CmdCheckTxnStatus

	CmdRawGet CmdType = 256 + iota
	CmdRawPut
	CmdRawDelete
	CmdRawScan

	CmdCop CmdType = 512 + iota
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdScan:
		return "Scan"
	case CmdPrewrite:
		return "Prewrite"
	case CmdCommit:
		return "Commit"
	case CmdBatchRollback:
		return "BatchRollback"
	case CmdResolveLock:
		return "ResolveLock"
	case CmdRawGet:
		return "RawGet"
	case CmdRawPut:
		return "RawPut"
	case CmdRawDelete:
		return "RawDelete"
	case CmdRawScan:
		return "RawScan"
	case CmdCop:
		return "Cop"
	case CmdCheckTxnStatus:
		return "CheckTxnStatus"
	}
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	Type CmdType
	req  interface{}
	kvrpcpb.Context
	ReplicaReadSeed uint32
}

// NewRequest returns new kv rpc request.
func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request {
	if len(ctxs) > 0 {
		return &Request{
			Type:    typ,
			req:     pointer,
			Context: ctxs[0],
		}
	}
	return &Request{
		Type: typ,
		req:  pointer,
	}
}

// Get returns GetRequest in request.
func (req *Request) Get() *kvrpcpb.GetRequest {
	return req.req.(*kvrpcpb.GetRequest)
}

// Scan returns ScanRequest in request.
func (req *Request) Scan() *kvrpcpb.ScanRequest {
	return req.req.(*kvrpcpb.ScanRequest)
}

// Prewrite returns PrewriteRequest in request.
func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest {
	return req.req.(*kvrpcpb.PrewriteRequest)
}

// Commit returns CommitRequest in request.
func (req *Request) Commit() *kvrpcpb.CommitRequest {
	return req.req.(*kvrpcpb.CommitRequest)
}

// BatchRollback returns BatchRollbackRequest in request.
func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest {
	return req.req.(*kvrpcpb.BatchRollbackRequest)
}

// ResolveLock returns ResolveLockRequest in request.
func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest {
	return req.req.(*kvrpcpb.ResolveLockRequest)
}

// RawGet returns RawGetRequest in request.
func (req *Request) RawGet() *kvrpcpb.RawGetRequest {
	return req.req.(*kvrpcpb.RawGetRequest)
}

// RawPut returns RawPutRequest in request.
func (req *Request) RawPut() *kvrpcpb.RawPutRequest {
	return req.req.(*kvrpcpb.RawPutRequest)
}

// RawDelete returns PrewriteRequest in request.
func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest {
	return req.req.(*kvrpcpb.RawDeleteRequest)
}

// RawScan returns RawScanRequest in request.
func (req *Request) RawScan() *kvrpcpb.RawScanRequest {
	return req.req.(*kvrpcpb.RawScanRequest)
}

// Cop returns coprocessor request in request.
func (req *Request) Cop() *coprocessor.Request {
	return req.req.(*coprocessor.Request)
}

// CheckTxnStatus returns CheckTxnStatusRequest in request.
func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest {
	return req.req.(*kvrpcpb.CheckTxnStatusRequest)
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Resp interface{}
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	ctx := &req.Context
	if region != nil {
		ctx.RegionId = region.Id
		ctx.RegionEpoch = region.RegionEpoch
	}
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		req.Get().Context = ctx
	case CmdScan:
		req.Scan().Context = ctx
	case CmdPrewrite:
		req.Prewrite().Context = ctx
	case CmdCommit:
		req.Commit().Context = ctx
	case CmdBatchRollback:
		req.BatchRollback().Context = ctx
	case CmdResolveLock:
		req.ResolveLock().Context = ctx
	case CmdRawGet:
		req.RawGet().Context = ctx
	case CmdRawPut:
		req.RawPut().Context = ctx
	case CmdRawDelete:
		req.RawDelete().Context = ctx
	case CmdRawScan:
		req.RawScan().Context = ctx
	case CmdCop:
		req.Cop().Context = ctx
	case CmdCheckTxnStatus:
		req.CheckTxnStatus().Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p interface{}
	resp := &Response{}
	switch req.Type {
	case CmdGet:
		p = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		p = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		p = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdCommit:
		p = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		p = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		p = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdRawGet:
		p = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		p = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		p = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawScan:
		p = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdCop:
		p = &coprocessor.Response{
			RegionError: e,
		}
	case CmdCheckTxnStatus:
		p = &kvrpcpb.CheckTxnStatusResponse{
			RegionError: e,
		}
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

type getRegionError interface {
	GetRegionError() *errorpb.Error
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	if resp.Resp == nil {
		return nil, nil
	}
	err, ok := resp.Resp.(getRegionError)
	if !ok {
		return nil, fmt.Errorf("invalid response type %v", resp)
	}
	return err.GetRegionError(), nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaing, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdGet:
		resp.Resp, err = client.KvGet(ctx, req.Get())
	case CmdScan:
		resp.Resp, err = client.KvScan(ctx, req.Scan())
	case CmdPrewrite:
		resp.Resp, err = client.KvPrewrite(ctx, req.Prewrite())
	case CmdCommit:
		resp.Resp, err = client.KvCommit(ctx, req.Commit())
	case CmdBatchRollback:
		resp.Resp, err = client.KvBatchRollback(ctx, req.BatchRollback())
	case CmdResolveLock:
		resp.Resp, err = client.KvResolveLock(ctx, req.ResolveLock())
	case CmdRawGet:
		resp.Resp, err = client.RawGet(ctx, req.RawGet())
	case CmdRawPut:
		resp.Resp, err = client.RawPut(ctx, req.RawPut())
	case CmdRawDelete:
		resp.Resp, err = client.RawDelete(ctx, req.RawDelete())
	case CmdRawScan:
		resp.Resp, err = client.RawScan(ctx, req.RawScan())
	case CmdCop:
		resp.Resp, err = client.Coprocessor(ctx, req.Cop())
	case CmdCheckTxnStatus:
		resp.Resp, err = client.KvCheckTxnStatus(ctx, req.CheckTxnStatus())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel context.CancelFunc
}
