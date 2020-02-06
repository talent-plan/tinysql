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
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testStoreSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testStoreSuite{})

func (s *testStoreSuite) SetUpTest(c *C) {
	s.store = NewTestStore(c).(*tikvStore)
}

func (s *testStoreSuite) TearDownTest(c *C) {
	c.Assert(s.store.Close(), IsNil)
}

func (s *testStoreSuite) TestParsePath(c *C) {
	etcdAddrs, disableGC, err := parsePath("tikv://node1:2379,node2:2379")
	c.Assert(err, IsNil)
	c.Assert(etcdAddrs, DeepEquals, []string{"node1:2379", "node2:2379"})
	c.Assert(disableGC, IsFalse)

	_, _, err = parsePath("tikv://node1:2379")
	c.Assert(err, IsNil)
	_, disableGC, err = parsePath("tikv://node1:2379?disableGC=true")
	c.Assert(err, IsNil)
	c.Assert(disableGC, IsTrue)
}

type checkRequestClient struct {
	Client
	priority pb.CommandPri
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if c.priority != req.Priority {
		if resp.Resp != nil {
			(resp.Resp.(*pb.GetResponse)).Error = &pb.KeyError{
				Abort: "request check error",
			}
		}
	}
	return resp, err
}

func (s *testStoreSuite) TestRequestPriority(c *C) {
	client := &checkRequestClient{
		Client: s.store.client,
	}
	s.store.client = client

	// Cover 2PC commit.
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_High
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Cover the basic Get request.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_Low
	txn.SetOption(kv.Priority, kv.PriorityLow)
	_, err = txn.Get(context.TODO(), []byte("key"))
	c.Assert(err, IsNil)

	// A counter example.
	client.priority = pb.CommandPri_Low
	txn.SetOption(kv.Priority, kv.PriorityNormal)
	_, err = txn.Get(context.TODO(), []byte("key"))
	// err is translated to "try again later" by backoffer, so doesn't check error value here.
	c.Assert(err, NotNil)

	// Cover Seek request.
	client.priority = pb.CommandPri_High
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	iter, err := txn.Iter([]byte("key"), nil)
	c.Assert(err, IsNil)
	for iter.Valid() {
		c.Assert(iter.Next(), IsNil)
	}
	iter.Close()
}
