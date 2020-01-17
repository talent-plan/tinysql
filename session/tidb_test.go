// Copyright 2015 PingCAP, Inc.
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

package session

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, logutil.EmptyFileLogConfig, false))
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct {
	dbName string
	store  kv.Storage
	dom    *domain.Domain
}

func (s *testMainSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test_main_db"
	s.store = newStore(c, s.dbName)
	dom, err := BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testMainSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
	removeStore(c, s.dbName)
}

func (s *testMainSuite) TestParseErrorWarn(c *C) {
	ctx := core.MockContext()

	nodes, err := Parse(ctx, "select /*+ adf */ 1")
	c.Assert(err, IsNil)
	c.Assert(len(nodes), Equals, 1)
	c.Assert(len(ctx.GetSessionVars().StmtCtx.GetWarnings()), Equals, 1)

	_, err = Parse(ctx, "select")
	c.Assert(err, NotNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	return store
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func (s *testMainSuite) TestKeysNeedLock(c *C) {
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, 1)
	indexKey := tablecodec.EncodeIndexSeekKey(1, 1, []byte{1})
	uniqueValue := make([]byte, 8)
	uniqueUntouched := append(uniqueValue, '1')
	nonUniqueVal := []byte{'0'}
	nonUniqueUntouched := []byte{'1'}
	var deleteVal []byte
	rowVal := []byte{'a', 'b', 'c'}
	tests := []struct {
		key  []byte
		val  []byte
		need bool
	}{
		{rowKey, rowVal, true},
		{rowKey, deleteVal, true},
		{indexKey, nonUniqueVal, false},
		{indexKey, nonUniqueUntouched, false},
		{indexKey, uniqueValue, true},
		{indexKey, uniqueUntouched, false},
		{indexKey, deleteVal, false},
	}
	for _, tt := range tests {
		c.Assert(keyNeedToLock(tt.key, tt.val), Equals, tt.need)
	}
}
