package tikv

import (
	"context"
	"math/rand"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testProj6Suite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store *tikvStore
}

var _ = Suite(&testProj6Suite{})

func (s *testProj6Suite) SetUpSuite(c *C) {
	ManagedLockTTL = 3000 // 3s
	s.OneByOneSuite.SetUpSuite(c)
}

func (s *testProj6Suite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, []byte("a"), []byte("b"), []byte("c"))
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	client := mocktikv.NewRPCClient(s.cluster, mvccStore)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	spkv := NewMockSafePointKV()
	store, err := newTikvStore("mocktikv-store", pdCli, spkv, client, false)
	c.Assert(err, IsNil)
	s.store = store
	CommitMaxBackoff = 2000
}

func (s *testProj6Suite) TearDownSuite(c *C) {
	CommitMaxBackoff = 20000
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testProj6Suite) checkValues(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		val, err := txn.Get(context.TODO(), []byte(k))
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, v)
	}
}

func (s *testProj6Suite) mustNotExist(c *C, keys ...[]byte) {
	txn := s.begin(c)
	for _, k := range keys {
		_, err := txn.Get(context.TODO(), k)
		c.Assert(err, NotNil)
		c.Check(terror.ErrorEqual(err, kv.ErrNotExist), IsTrue)
	}
}

func (s *testProj6Suite) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, NotNil)
	lock, err := extractLockFromKeyErr(keyErr)
	c.Assert(err, IsNil)
	return lock
}

func (s *testProj6Suite) mustUnLock(c *C, key []byte) {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, IsNil)
	//lock, err := extractLockFromKeyErr(keyErr)
	//c.Assert(err, IsNil)
	//c.Assert(lock, IsNil)
}

func (s *testProj6Suite) begin(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testProj6Suite) beginWithStartTs(c *C, ts uint64) *tikvTxn {
	txn, err := s.store.BeginWithStartTS(ts)
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testProj6Suite) TestInitKeysAndMutations(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn.Delete([]byte("c"))
	c.Assert(err, IsNil)
	err = txn.LockKeys(context.Background(), new(kv.LockCtx), kv.Key("a"), kv.Key("d"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	err = committer.initKeysAndMutations()
	c.Assert(err, IsNil)

	key2mut := map[string]kvrpcpb.Mutation{
		"a": {
			Op:    kvrpcpb.Op_Put,
			Key:   []byte("a"),
			Value: []byte("a1"),
		},
		"b": {
			Op:    kvrpcpb.Op_Put,
			Key:   []byte("b"),
			Value: []byte("b1"),
		},
		"c": {
			Op:  kvrpcpb.Op_Del,
			Key: []byte("c"),
		},
		"d": {
			Op:  kvrpcpb.Op_Lock,
			Key: []byte("d"),
		},
	}
	c.Assert(len(committer.mutations), Equals, len(key2mut))
	c.Assert(len(committer.keys), Equals, len(key2mut))
	txnSize := 0
	for k, m := range committer.mutations {
		c.Assert(m.Op, Equals, key2mut[k].Op)
		c.Assert(m.Key, BytesEquals, key2mut[k].Key)
		c.Assert(m.Value, BytesEquals, key2mut[k].Value)
		delete(key2mut, k)
		txnSize += len(m.Key) + len(m.Value)
	}
	c.Assert(committer.txnSize, Equals, txnSize)
}

func (s *testProj6Suite) TestGroupKeysByRegion(c *C) {
	var (
		key1 = []byte("Z")
		key2 = []byte("a1")
		key3 = []byte("a")
		key4 = []byte("b")
		key5 = []byte("c")
	)
	keys := [][]byte{key2, key3, key4, key5}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	keys = append([][]byte{key1}, keys...)
	groups, first, err := s.store.GetRegionCache().GroupKeysByRegion(NewBackoffer(context.Background(), 1000), keys, nil)
	c.Assert(err, IsNil)
	c.Assert(first.GetID(), Equals, uint64(3))

	keys2id := map[string]uint64{
		string(key1): 3,
		string(key2): 4,
		string(key3): 4,
		string(key4): 5,
		string(key5): 6,
	}

	keyCnt := 0
	for region, keys := range groups {
		for _, key := range keys {
			keyCnt++
			c.Assert(region.GetID(), Equals, keys2id[string(key)])
			delete(keys2id, string(key))
		}
	}
	c.Assert(len(keys), Equals, keyCnt)
}

func (s *testProj6Suite) TestBuildPrewriteRequest(c *C) {
	txn := s.begin(c)

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")
	v3 := []byte("v3")
	batch := batchKeys{
		keys: [][]byte{k2, k3},
	}

	txn.Set(k1, v1)
	txn.Set(k2, v2)
	txn.Set(k3, v3)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	committer.primaryKey = k1
	c.Assert(committer.primary(), BytesEquals, k1)
	r := committer.buildPrewriteRequest(batch)
	c.Assert(r.Type, Equals, tikvrpc.CmdPrewrite)
	req := r.Prewrite()

	c.Assert(req.PrimaryLock, BytesEquals, k1)
	c.Assert(req.StartVersion, Equals, committer.startTS)
	c.Assert(req.LockTtl, Equals, committer.lockTTL)
	c.Assert(len(req.Mutations), Equals, len(batch.keys))
}

func (s *testProj6Suite) preparePrewritedTxn(c *C, pk string, kvs map[string]string) (*tikvTxn, *twoPhaseCommitter) {
	txn := s.begin(c)
	hasPk := false
	keys := make([][]byte, 0, len(kvs))
	for k, v := range kvs {
		keys = append(keys, []byte(k))
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
		if k == pk {
			hasPk = true
		}
	}
	c.Assert(hasPk, IsTrue)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	committer.primaryKey = []byte(pk)
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), PrewriteMaxBackoff)
	err = committer.prewriteKeys(bo, keys)
	c.Assert(err, IsNil)
	return txn, committer
}

func (s *testProj6Suite) TestCommitSingleBatch(c *C) {
	k1 := []byte("a1")
	k2 := []byte("b1")
	k3 := []byte("b2")

	_, committer := s.preparePrewritedTxn(c, "a1", map[string]string{
		"a1": "a1",
		"b1": "b1",
		"b2": "b2",
	})
	action := actionCommit{}
	bo := NewBackoffer(context.Background(), CommitMaxBackoff)
	loc, err := s.store.regionCache.LocateKey(NewBackoffer(context.Background(), getMaxBackoff), k1)
	c.Assert(err, IsNil)
	action.handleSingleBatch(committer, bo, batchKeys{
		region: loc.Region,
		keys:   [][]byte{k1},
	})
	s.mustUnLock(c, k1)
	s.mustGetLock(c, k2)
	s.mustGetLock(c, k3)

	loc, err = s.store.regionCache.LocateKey(NewBackoffer(context.Background(), getMaxBackoff), k2)
	c.Assert(err, IsNil)
	action.handleSingleBatch(committer, bo, batchKeys{
		region: loc.Region,
		keys:   [][]byte{k2, k3},
	})
	s.mustUnLock(c, k1)
	s.mustUnLock(c, k2)
	s.mustUnLock(c, k3)

	s.checkValues(c, map[string]string{
		"a1": "a1",
		"b1": "b1",
		"b2": "b2",
	})
}

func (s *testProj6Suite) TestUnCommit(c *C) {
	k1 := []byte("k1")
	k2 := []byte("k2")

	_, _ = s.preparePrewritedTxn(c, "k1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})

	s.mustNotExist(c, k1, k2)
}

func (s *testProj6Suite) TestCommit(c *C) {
	var err error
	k1 := []byte("k1")
	k2 := []byte("k2")

	_, committer := s.preparePrewritedTxn(c, "k1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	// check lock
	s.mustGetLock(c, k1)
	s.mustGetLock(c, k2)
	// commit keys
	committer.commitTS, err = s.store.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	err = committer.commitKeys(NewBackoffer(context.Background(), CommitMaxBackoff), [][]byte{k1, k2})
	c.Assert(err, IsNil)
	s.checkValues(c, map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
}

func (s *testProj6Suite) TestRollbackSingleBatch(c *C) {
	k1 := []byte("a1")
	k2 := []byte("b1")
	k3 := []byte("b2")

	_, committer := s.preparePrewritedTxn(c, "a1", map[string]string{
		"a1": "a1",
		"b1": "b1",
		"b2": "b2",
	})

	action := actionCleanup{}
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	loc, err := s.store.regionCache.LocateKey(NewBackoffer(context.Background(), getMaxBackoff), k1)
	c.Assert(err, IsNil)
	action.handleSingleBatch(committer, bo, batchKeys{
		region: loc.Region,
		keys:   [][]byte{k1},
	})
	s.mustUnLock(c, k1)
	s.mustGetLock(c, k2)
	s.mustGetLock(c, k3)

	loc, err = s.store.regionCache.LocateKey(NewBackoffer(context.Background(), getMaxBackoff), k2)
	c.Assert(err, IsNil)
	action.handleSingleBatch(committer, bo, batchKeys{
		region: loc.Region,
		keys:   [][]byte{k2, k3},
	})
	s.mustUnLock(c, k1)
	s.mustUnLock(c, k2)
	s.mustUnLock(c, k3)

	s.mustNotExist(c, k1, k2, k3)
}

func (s *testProj6Suite) TestRollback(c *C) {
	var err error
	k1 := []byte("k1")
	k2 := []byte("k2")

	_, committer := s.preparePrewritedTxn(c, "k1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	err = committer.cleanupKeys(bo, [][]byte{k1, k2})
	c.Assert(err, IsNil)

	s.mustNotExist(c, k1, k2)
}

func (s *testProj6Suite) TestResolveLock(c *C) {
	k1 := []byte("a")
	k2 := []byte("b")
	k3 := []byte("c")

	txn, committer := s.preparePrewritedTxn(c, "a", map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})
	var err error
	committer.commitTS, err = s.store.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	err = committer.commitKeys(NewBackoffer(context.Background(), CommitMaxBackoff), [][]byte{k1})
	c.Assert(err, IsNil)

	lr := newLockResolver(s.store)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	status, err := lr.GetTxnStatus(txn.StartTS(), committer.commitTS+1, k1)
	c.Assert(err, IsNil)
	// the transaction status from primary key is committed
	c.Assert(status.IsCommitted(), IsTrue)
	// resolve the lock for committed transactions
	for _, k := range [][]byte{k2, k3} {
		lock := s.mustGetLock(c, k)
		c.Assert(lock, NotNil)
		cleanRegions := make(map[RegionVerID]struct{})
		lr := newLockResolver(s.store)
		status, err := lr.getTxnStatusFromLock(NewBackoffer(context.Background(), 1000), lock, committer.commitTS+1)
		c.Assert(err, IsNil)
		err = lr.resolveLock(bo, lock, status, cleanRegions)
		c.Assert(err, IsNil)
		// after resolve, check the lock is cleared
		s.mustUnLock(c, k)
	}
}

func (s *testProj6Suite) TestFailAfterPrimary(c *C) {
	point := "github.com/pingcap/tidb/store/tikv/mockFailAfterPK"
	c.Assert(failpoint.Enable(point, `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable(point), IsNil)
	}()

	txn := s.begin(c)

	k1 := []byte("a")
	v1 := []byte("a")
	k2 := []byte("b")
	v2 := []byte("b")
	k3 := []byte("c")
	v3 := []byte("c")

	txn.Set(k1, v1)
	txn.Set(k2, v2)
	txn.Set(k3, v3)

	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	s.checkValues(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})
}

func (s *testProj6Suite) TestGetResolveLockCommit(c *C) {
	txn, committer := s.preparePrewritedTxn(c, "a", map[string]string{
		"a": "a",
		"b": "b",
	})
	k1 := []byte("a")
	k2 := []byte("b")
	var err error
	committer.commitTS, err = s.store.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	err = committer.commitKeys(NewBackoffer(context.Background(), CommitMaxBackoff), [][]byte{k1})
	c.Assert(err, IsNil)

	// there is a lock on k2 since it hasn't been committed
	s.mustGetLock(c, k2)

	// transaction with smaller startTS will not see the lock
	txn1 := s.beginWithStartTs(c, txn.startTS-1)
	_, err = txn1.Get(context.Background(), k2)
	c.Check(terror.ErrorEqual(err, kv.ErrNotExist), IsTrue)

	// check the lock is still exist
	s.mustGetLock(c, k2)

	// tinysql will always use the latest ts to try resolving the lock
	txn2 := s.beginWithStartTs(c, committer.commitTS+1)
	val, err := txn2.Get(context.Background(), k2)
	c.Assert(err, IsNil)
	c.Assert(val, BytesEquals, []byte("b"))

	s.mustUnLock(c, k2)
}


func (s *testProj6Suite) TestGetResolveLockRollback(c *C) {
	txn, committer := s.preparePrewritedTxn(c, "a", map[string]string{
		"a": "a",
		"b": "b",
	})
	k1 := []byte("a")
	k2 := []byte("b")

	// there is a lock on k1 and k2 since they haven't been committed
	s.mustGetLock(c, k1)
	s.mustGetLock(c, k2)

	// transaction with smaller startTS will not see the lock
	txn1 := s.beginWithStartTs(c, txn.startTS-1)
	_, err := txn1.Get(context.Background(), k1)
	c.Check(terror.ErrorEqual(err, kv.ErrNotExist), IsTrue)
	_, err = txn1.Get(context.Background(), k2)
	c.Check(terror.ErrorEqual(err, kv.ErrNotExist), IsTrue)

	// check the lock is still exist
	s.mustGetLock(c, k1)
	s.mustGetLock(c, k2)

	// tinysql will always use the latest ts to try resolving the lock
	txn2 := s.beginWithStartTs(c, committer.startTS+1)
	_, err = txn2.Get(context.Background(), k2)
	c.Assert(err, NotNil)
	c.Check(terror.ErrorEqual(err, kv.ErrNotExist), IsTrue)

	s.mustUnLock(c, k1)
	s.mustUnLock(c, k2)
}
