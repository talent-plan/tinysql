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

package tikv

import (
	"context"
	"flag"

	"github.com/google/uuid"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var mockStore = flag.Bool("mockStore", true, "use mock store when commit protocol is not implemented")

// NewTestTiKVStore creates a test store with Option
func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client) (kv.Storage, error) {
	if clientHijack != nil {
		client = clientHijack(client)
	}

	pdCli := pd.Client(&codecPDClient{pdClient})
	if pdClientHijack != nil {
		pdCli = pdClientHijack(pdCli)
	}

	// Make sure the uuid is unique.
	uid := uuid.New().String()
	spkv := NewMockSafePointKV()
	tikvStore, err := newTikvStore(uid, pdCli, spkv, client, false)

	tikvStore.mock = true
	if *mockStore {
		return &mockTikvStore{tikvStore}, errors.Trace(err)
	}
	return tikvStore, errors.Trace(err)
}

type mockTikvStore struct {
	*tikvStore
}

type mockTransaction struct {
	*tikvTxn
}

type mockTwoPhaseCommitter struct {
	*twoPhaseCommitter
}

func (m *mockTikvStore) Begin()  (kv.Transaction, error){
	txn, err := m.tikvStore.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mockTransaction{txn.(*tikvTxn)}, nil
}

func (m *mockTikvStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error){
	txn, err := m.tikvStore.BeginWithStartTS(startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mockTransaction{txn.(*tikvTxn)}, nil
}

func (txn *mockTransaction) Commit(ctx context.Context) error {
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
		committer, err = newTwoPhaseCommitter(txn.tikvTxn, connID)
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
	mockCommitter := &mockTwoPhaseCommitter{committer}
	err = mockCommitter.execute(ctx)
	return errors.Trace(err)
}

// execute executes the two-phase commit protocol.
func (c *mockTwoPhaseCommitter) execute(_ context.Context) (err error) {
	if len(c.mutations) == 0 {
		return nil
	}
	for k := range c.mutations {
		c.primaryKey = []byte(k)
		break
	}
	// prewrite
	for k, m := range c.mutations {
		req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &pb.PrewriteRequest{
			Mutations:    []*pb.Mutation{&m.Mutation},
			PrimaryLock:  c.primary(),
			StartVersion: c.startTS,
			LockTtl:      c.lockTTL,
		}, pb.Context{})
		bo :=  NewBackoffer(context.Background(), PrewriteMaxBackoff)
		sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
		loc, err := c.store.GetRegionCache().LocateKey(bo, []byte(k))
		if err != nil {
			return err
		}
		sender.SendReq(bo, req, loc.Region,readTimeoutShort  )
	}

	// commit
	for k := range c.mutations {
		req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &pb.CommitRequest{
			StartVersion:  c.startTS,
			Keys:          [][]byte{[]byte(k)},
			CommitVersion: c.commitTS,
		}, pb.Context{})
		bo :=  NewBackoffer(context.Background(), CommitMaxBackoff)
		sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
		loc, err := c.store.GetRegionCache().LocateKey(bo, []byte(k))
		if err != nil {
			return err
		}
		sender.SendReq(bo, req, loc.Region,readTimeoutShort  )
	}
	return nil
}
