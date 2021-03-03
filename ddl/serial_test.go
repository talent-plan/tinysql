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

package ddl_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testSerialSuite{})

type testSerialSuite struct {
	store     kv.Storage
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	dom       *domain.Domain
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	session.SetSchemaLease(200 * time.Millisecond)
	session.DisableStats4Test()

	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	config.StoreGlobalConfig(&newCfg)

	s.cluster = mocktikv.NewCluster()
	s.mvccStore = mocktikv.MustNewMVCCStore()

	ddl.WaitTimeWhenErrorOccurred = 1 * time.Microsecond
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSerialSuite) TearDownSuite(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.store != nil {
		s.store.Close()
	}
}

func (s *testSerialSuite) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	maxID, emptyTable := getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(999))

	tk.MustExec("insert into t values(10000, 1000)")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxID, Equals, int64(10000))
}

func (s *testSerialSuite) TestGetTableEndHandle(c *C) {
	// TestGetTableEndHandle test ddl.GetTableMaxRowID method, which will return the max row id of the table.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := s.dom.InfoSchema()
	d := s.dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	testCtx := newTestMaxTableRowIDContext(c, d, tbl)
	// test empty table
	checkGetMaxTableRowID(testCtx, s.store, true, int64(math.MaxInt64))

	tk.MustExec("insert into t values(-1, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, int64(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, false, int64(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, true, int64(math.MaxInt64))

	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", i))
	}

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable := getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxID, emptyTable = getMaxTableRowID(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxID)))
	c.Assert(emptyTable, IsFalse)
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testSerialSuite) TestCancelAddIndexPanic(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/errorMockPanic", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/errorMockPanic"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			txn, err = hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	origHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
}

func (s *testSerialSuite) TestCancelJobByErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit"), IsNil)
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 16")
	err := ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))

	_, err = tk.Exec("create table t (a int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
}

func (s *testSerialSuite) TestCanceledJobTakeTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cjtt(a int)")

	hook := &ddl.TestDDLCallback{}
	once := sync.Once{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		once.Do(func() {
			err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				t := meta.NewMeta(txn)
				return t.DropTableOrView(job.SchemaID, job.TableID, true)
			})
			c.Assert(err, IsNil)
		})
	}
	origHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(origHook)

	originalWT := ddl.WaitTimeWhenErrorOccurred
	ddl.WaitTimeWhenErrorOccurred = 1 * time.Second
	defer func() { ddl.WaitTimeWhenErrorOccurred = originalWT }()
	startTime := time.Now()
	tk.MustGetErrCode("alter table t_cjtt add column b int", mysql.ErrNoSuchTable)
	sub := time.Since(startTime)
	c.Assert(sub, Less, ddl.WaitTimeWhenErrorOccurred)
}
