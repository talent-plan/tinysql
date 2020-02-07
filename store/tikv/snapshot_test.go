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
	"time"

	. "github.com/pingcap/check"
)

type testSnapshotSuite struct {
	OneByOneSuite
	store   *tikvStore
	prefix  string
	rowNums []int
}

var _ = Suite(&testSnapshotSuite{})

func (s *testSnapshotSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*tikvStore)
	s.prefix = fmt.Sprintf("snapshot_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, 100, 191)
}

func (s *testSnapshotSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSnapshotSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testSnapshotSuite) TestLockNotFoundPrint(c *C) {
	msg := "Txn(Mvcc(TxnLockNotFound { start_ts: 408090278408224772, commit_ts: 408090279311835140, " +
		"key: [116, 128, 0, 0, 0, 0, 0, 50, 137, 95, 105, 128, 0, 0, 0, 0,0 ,0, 1, 1, 67, 49, 57, 48, 57, 50, 57, 48, 255, 48, 48, 48, 48, 48, 52, 56, 54, 255, 50, 53, 53, 50, 51, 0, 0, 0, 252] }))"
	key := prettyLockNotFoundKey(msg)
	c.Assert(key, Equals, "{tableID=12937, indexID=1, indexValues={C19092900000048625523, }}")
}
