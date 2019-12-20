// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
)

var (
	_ = Suite(&testChunkSizeControlSuite{})
)

type testSlowClient struct {
	sync.RWMutex
	tikv.Client
	regionDelay map[uint64]time.Duration
}

func (c *testSlowClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	regionID := req.RegionId
	delay := c.GetDelay(regionID)
	if req.Type == tikvrpc.CmdCop && delay > 0 {
		time.Sleep(delay)
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *testSlowClient) SetDelay(regionID uint64, dur time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.regionDelay[regionID] = dur
}

func (c *testSlowClient) GetDelay(regionID uint64) time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.regionDelay[regionID]
}

// manipulateCluster splits this cluster's region by splitKeys and returns regionIDs after split
func manipulateCluster(cluster *mocktikv.Cluster, splitKeys [][]byte) []uint64 {
	if len(splitKeys) == 0 {
		return nil
	}
	region, _ := cluster.GetRegionByKey(splitKeys[0])
	for _, key := range splitKeys {
		if r, _ := cluster.GetRegionByKey(key); r.Id != region.Id {
			panic("all split keys should belong to the same region")
		}
	}
	allRegionIDs := []uint64{region.Id}
	for i, key := range splitKeys {
		newRegionID, newPeerID := cluster.AllocID(), cluster.AllocID()
		cluster.Split(allRegionIDs[i], newRegionID, key, []uint64{newPeerID}, newPeerID)
		allRegionIDs = append(allRegionIDs, newRegionID)
	}
	return allRegionIDs
}

func generateTableSplitKeyForInt(tid int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		results = append(results, tablecodec.EncodeRowKey(tid, codec.EncodeInt(nil, int64(num))))
	}
	return results
}

type testChunkSizeControlKit struct {
	store   kv.Storage
	dom     *domain.Domain
	tk      *testkit.TestKit
	client  *testSlowClient
	cluster *mocktikv.Cluster
}

type testChunkSizeControlSuite struct {
	m map[string]*testChunkSizeControlKit
}

func (s *testChunkSizeControlSuite) SetUpSuite(c *C) {
	c.Skip("not stable because coprocessor may result in goroutine leak")
	tableSQLs := map[string]string{}
	tableSQLs["Limit&TableScan"] = "create table t (a int, primary key (a))"
	tableSQLs["Limit&IndexScan"] = "create table t (a int, index idx_a(a))"

	s.m = make(map[string]*testChunkSizeControlKit)
	for name, sql := range tableSQLs {
		// BootstrapSession is not thread-safe, so we have to prepare all resources in SetUp.
		kit := new(testChunkSizeControlKit)
		s.m[name] = kit
		kit.client = &testSlowClient{regionDelay: make(map[uint64]time.Duration)}
		kit.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(kit.cluster)

		var err error
		kit.store, err = mockstore.NewMockTikvStore(
			mockstore.WithCluster(kit.cluster),
			mockstore.WithHijackClient(func(c tikv.Client) tikv.Client {
				kit.client.Client = c
				return kit.client
			}),
		)
		c.Assert(err, IsNil)

		// init domain
		kit.dom, err = session.BootstrapSession(kit.store)
		c.Assert(err, IsNil)

		// create the test table
		kit.tk = testkit.NewTestKitWithInit(c, kit.store)
		kit.tk.MustExec(sql)
	}
}
