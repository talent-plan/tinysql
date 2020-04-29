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

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *RegionCache
	pdClient    pd.Client
	rpcClient   Client
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	if c.pdClient != nil {
		c.pdClient.Close()
	}
	if c.regionCache != nil {
		c.regionCache.Close()
	}
	if c.rpcClient == nil {
		return nil
	}
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{Key: key})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawGetResponse)
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   key,
		Value: value,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawDelete, &kvrpcpb.RawDeleteRequest{
		Key: key,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, append a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(append(startKey, '\0'), append(endKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawScan, &kvrpcpb.RawScanRequest{
			StartKey: startKey,
			Limit:    uint32(limit - len(keys)),
		})
		resp, loc, err := c.sendReq(startKey, req, false)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Resp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

func (c *RawKVClient) sendReq(key []byte, req *tikvrpc.Request, reverse bool) (*tikvrpc.Response, *KeyLocation, error) {
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		var loc *KeyLocation
		var err error
		if reverse {
			loc, err = c.regionCache.LocateEndKey(bo, key)
		} else {
			loc, err = c.regionCache.LocateKey(bo, key)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, loc, nil
	}
}
