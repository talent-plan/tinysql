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

package ddl_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testDBSuite1{&testDBSuite{}})
var _ = Suite(&testDBSuite2{&testDBSuite{}})
var _ = Suite(&testDBSuite3{&testDBSuite{}})
var _ = Suite(&testDBSuite4{&testDBSuite{}})
var _ = Suite(&testDBSuite5{&testDBSuite{}})

type testDBSuite struct {
	cluster    *mocktikv.Cluster
	mvccStore  mocktikv.MVCCStore
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	tk         *testkit.TestKit
	s          session.Session
	lease      time.Duration
	autoIDStep int64
}

func setUpSuite(s *testDBSuite, c *C) {
	var err error

	s.lease = 100 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	ddl.WaitTimeWhenErrorOccured = 0

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.s, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	_, err = s.s.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)

	s.tk = testkit.NewTestKit(c, s.store)
}

func tearDownSuite(s *testDBSuite, c *C) {
	s.s.Execute(context.Background(), "drop database if exists test_db")
	s.s.Close()
	s.dom.Close()
	s.store.Close()
}

func (s *testDBSuite) SetUpSuite(c *C) {
	setUpSuite(s, c)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	tearDownSuite(s, c)
}

type testDBSuite1 struct{ *testDBSuite }
type testDBSuite2 struct{ *testDBSuite }
type testDBSuite3 struct{ *testDBSuite }
type testDBSuite4 struct{ *testDBSuite }
type testDBSuite5 struct{ *testDBSuite }

func (s *testDBSuite4) TestAddIndexWithPK(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("create table test_add_index_with_pk(a int not null, b int not null default '0', primary key(a))")
	s.tk.MustExec("insert into test_add_index_with_pk values(1, 2)")
	s.tk.MustExec("alter table test_add_index_with_pk add index idx (a)")
	s.tk.MustQuery("select a from test_add_index_with_pk").Check(testkit.Rows("1"))
	s.tk.MustExec("insert into test_add_index_with_pk values(2, 2)")
	s.tk.MustExec("alter table test_add_index_with_pk add index idx1 (a, b)")
	s.tk.MustQuery("select * from test_add_index_with_pk").Check(testkit.Rows("1 2", "2 2"))
	s.tk.MustExec("create table test_add_index_with_pk1(a int not null, b int not null default '0', c int, d int, primary key(c))")
	s.tk.MustExec("insert into test_add_index_with_pk1 values(1, 1, 1, 1)")
	s.tk.MustExec("alter table test_add_index_with_pk1 add index idx (c)")
	s.tk.MustExec("insert into test_add_index_with_pk1 values(2, 2, 2, 2)")
	s.tk.MustQuery("select * from test_add_index_with_pk1").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	s.tk.MustExec("create table test_add_index_with_pk2(a int not null, b int not null default '0', c int unsigned, d int, primary key(c))")
	s.tk.MustExec("insert into test_add_index_with_pk2 values(1, 1, 1, 1)")
	s.tk.MustExec("alter table test_add_index_with_pk2 add index idx (c)")
	s.tk.MustExec("insert into test_add_index_with_pk2 values(2, 2, 2, 2)")
	s.tk.MustQuery("select * from test_add_index_with_pk2").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
}

func testGetTableByName(c *C, ctx sessionctx.Context, db, table string) table.Table {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testDBSuite) testGetTable(c *C, name string) table.Table {
	ctx := s.s.(sessionctx.Context)
	return testGetTableByName(c, ctx, s.schemaName, name)
}

func (s *testDBSuite) testGetDB(c *C, dbName string) *model.DBInfo {
	ctx := s.s.(sessionctx.Context)
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
	c.Assert(ok, IsTrue)
	return db
}

func backgroundExec(s kv.Storage, sql string, done chan error) {
	se, err := session.CreateSession4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.Execute(context.Background(), "use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), sql)
	done <- errors.Trace(err)
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func (s *testDBSuite4) TestCancelAddIndex1(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t")
	s.mustExec(c, "create table t(c1 int, c2 int)")
	defer s.mustExec(c, "drop table t;")

	for i := 0; i < 50; i++ {
		s.mustExec(c, fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	var checkErr error
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0 {
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

			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	rs, err := s.tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")

	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	t := s.testGetTable(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	s.mustExec(c, "alter table t add index idx_c2(c2)")
	s.mustExec(c, "alter table t drop index idx_c2")
}

// TestCancelDropIndex tests cancel ddl job which type is drop index.
func (s *testDBSuite5) TestCancelDropIndex(c *C) {
	idxName := "idx_c2"
	addIdxSQL := "alter table t add index idx_c2 (c2);"
	dropIdxSQL := "alter table t drop index idx_c2;"
	testCancelDropIndex(c, s.store, s.dom.DDL(), idxName, addIdxSQL, dropIdxSQL)
}

// testCancelDropIndex tests cancel ddl job which type is drop index.
func testCancelDropIndex(c *C, store kv.Storage, d ddl.DDL, idxName, addIdxSQL, dropIdxSQL string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	defer tk.MustExec("drop table t;")
	for i := 0; i < 5; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	testCases := []struct {
		needAddIndex   bool
		jobState       model.JobState
		JobSchemaState model.SchemaState
		cancelSucc     bool
	}{
		// model.JobStateNone means the jobs is canceled before the first run.
		{true, model.JobStateNone, model.StateNone, true},
		{false, model.JobStateRunning, model.StateWriteOnly, true},
		{false, model.JobStateRunning, model.StateDeleteOnly, false},
		{true, model.JobStateRunning, model.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if (job.Type == model.ActionDropIndex || job.Type == model.ActionDropPrimaryKey) &&
			job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.Store = store
			err := hookCtx.NewTxn(context.TODO())
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
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := d.GetHook()
	d.(ddl.DDLForTest).SetHook(hook)
	ctx := tk.Se.(sessionctx.Context)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			tk.MustExec(addIdxSQL)
		}
		rs, err := tk.Exec(dropIdxSQL)
		if rs != nil {
			rs.Close()
		}
		t := testGetTableByName(c, ctx, "test_db", "t")
		indexInfo := t.Meta().FindIndexByName(idxName)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			c.Assert(indexInfo, NotNil)
			c.Assert(indexInfo.State, Equals, model.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfo, IsNil)
		}
	}
	d.(ddl.DDLForTest).SetHook(originalHook)
	tk.MustExec(addIdxSQL)
	tk.MustExec(dropIdxSQL)
}

// TestCancelDropTable tests cancel ddl job which type is drop table.
func (s *testDBSuite2) TestCancelDropTableAndSchema(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	testCases := []struct {
		needAddTableOrDB bool
		action           model.ActionType
		jobState         model.JobState
		JobSchemaState   model.SchemaState
		cancelSucc       bool
	}{
		// Check drop table.
		// model.JobStateNone means the jobs is canceled before the first run.
		{true, model.ActionDropTable, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropTable, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropTable, model.JobStateRunning, model.StateDeleteOnly, false},

		// Check drop database.
		{true, model.ActionDropSchema, model.JobStateNone, model.StateNone, true},
		{false, model.ActionDropSchema, model.JobStateRunning, model.StateWriteOnly, false},
		{true, model.ActionDropSchema, model.JobStateRunning, model.StateDeleteOnly, false},
	}
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	var jobID int64
	testCase := &testCases[0]
	s.mustExec(c, "create database if not exists test_drop_db")
	dbInfo := s.testGetDB(c, "test_drop_db")

	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState && job.SchemaID == dbInfo.ID {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.Store = s.store
			err := hookCtx.NewTxn(context.TODO())
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
			checkErr = txn.Commit(context.Background())
		}
	}
	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	var err error
	sql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddTableOrDB {
			s.mustExec(c, "create database if not exists test_drop_db")
			s.mustExec(c, "use test_drop_db")
			s.mustExec(c, "create table if not exists t(c1 int, c2 int)")
		}

		dbInfo = s.testGetDB(c, "test_drop_db")

		if testCase.action == model.ActionDropTable {
			sql = "drop table t;"
		} else if testCase.action == model.ActionDropSchema {
			sql = "drop database test_drop_db;"
		}

		_, err = s.tk.Exec(sql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			s.mustExec(c, fmt.Sprintf("insert into t values (%d, %d)", i, i))
		} else {
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDDLJob.GenWithStackByArgs(jobID).Error())
			_, err = s.tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
			c.Assert(err, NotNil)
		}
	}
}

func (s *testDBSuite3) TestAddAnonymousIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	s.mustExec(c, "create table t_anonymous_index (c1 int, c2 int, C3 int)")
	s.mustExec(c, "alter table t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	_, err := s.tk.Exec("alter table t_anonymous_index drop index")
	c.Assert(err, NotNil)
	// The index name is c1 when adding index (c1, c2).
	s.mustExec(c, "alter table t_anonymous_index drop index c1")
	t := s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 0)
	// for adding some indices that the first column name is c1
	s.mustExec(c, "alter table t_anonymous_index add index (c1)")
	_, err = s.tk.Exec("alter table t_anonymous_index add index c1 (c2)")
	c.Assert(err, NotNil)
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 1)
	idx := t.Indices()[0].Meta().Name.L
	c.Assert(idx, Equals, "c1")
	// The MySQL will be a warning.
	s.mustExec(c, "alter table t_anonymous_index add index c1_3 (c1)")
	s.mustExec(c, "alter table t_anonymous_index add index (c1, c2, C3)")
	// The MySQL will be a warning.
	s.mustExec(c, "alter table t_anonymous_index add index (c1)")
	t = s.testGetTable(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 4)
	s.mustExec(c, "alter table t_anonymous_index drop index c1")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_2")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_3")
	s.mustExec(c, "alter table t_anonymous_index drop index c1_4")
	// for case insensitive
	s.mustExec(c, "alter table t_anonymous_index add index (C3)")
	s.mustExec(c, "alter table t_anonymous_index drop index c3")
	s.mustExec(c, "alter table t_anonymous_index add index c3 (C3)")
	s.mustExec(c, "alter table t_anonymous_index drop index C3")
	// for anonymous index with column name `primary`
	s.mustExec(c, "create table t_primary (`primary` int, key (`primary`))")
	t = s.testGetTable(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	s.mustExec(c, "create table t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	t = s.testGetTable(c, "t_primary_2")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustExec(c, "create table t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	t = s.testGetTable(c, "t_primary_3")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
}

func (s *testDBSuite5) TestAddMultiColumnsIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.tk.MustExec("drop database if exists tidb;")
	s.tk.MustExec("create database tidb;")
	s.tk.MustExec("use tidb;")
	s.tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	s.tk.MustExec("insert tidb.test values (1, 2);")
	s.tk.MustExec("insert into tidb.test values (2, 2);")
	// Test that the b value is nil.
	s.tk.MustExec("insert into tidb.test (a) values (3);")
	s.tk.MustExec("insert into tidb.test values (4, 4);")
	// Test that the b value is nil again.
	s.tk.MustExec("insert into tidb.test (a) values (5);")
	s.tk.MustExec("insert tidb.test values (6, 6);")
	s.tk.MustExec("alter table tidb.test add index idx1 (a, b);")

}

func (s *testDBSuite2) TestDropIndex(c *C) {
	idxName := "c3_index"
	createSQL := "create table test_drop_index (c1 int, c2 int, c3 int, unique key(c1), key c3_index(c3))"
	dropIdxSQL := "alter table test_drop_index drop index c3_index;"
	testDropIndex(c, s.store, s.lease, createSQL, dropIdxSQL, idxName)
}

func testDropIndex(c *C, store kv.Storage, lease time.Duration, createSQL, dropIdxSQL, idxName string) {
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test_db")
	tk.MustExec("drop table if exists test_drop_index")
	tk.MustExec(createSQL)
	done := make(chan error, 1)
	tk.MustExec("delete from test_drop_index")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		tk.MustExec(fmt.Sprintf("insert into test_drop_index values (%d, %d, %d)", i, i, i))
	}
	ctx := tk.Se.(sessionctx.Context)
	t := testGetTableByName(c, ctx, "test_db", "test_drop_index")
	var c3idx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			c3idx = tidx
			break
		}
	}
	c.Assert(c3idx, NotNil)

	testddlutil.SessionExecInGoroutine(c, store, dropIdxSQL, done)

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 5
			// add some data
			for i := num; i < num+step; i++ {
				tk.MustExec(fmt.Sprintf("insert into test_drop_index values (%d, %d, %d)", i, i, i))
			}
			num += step
		}
	}

	rows := tk.MustQuery("explain select c1 from test_drop_index where c3 >= 0")
	c.Assert(strings.Contains(fmt.Sprintf("%v", rows), idxName), IsFalse)

	// Check in index, it must be no index in KV.
	// Make sure there is no index with name c3_index.
	t = testGetTableByName(c, ctx, "test_db", "test_drop_index")
	var nidx table.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			nidx = tidx
			break
		}
	}
	c.Assert(nidx, IsNil)
	tk.MustExec("drop table test_drop_index")
}

func (s *testDBSuite4) TestAddIndexWithDupCols(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)
	err1 := infoschema.ErrColumnExists.GenWithStackByArgs("b")
	err2 := infoschema.ErrColumnExists.GenWithStackByArgs("B")

	s.tk.MustExec("create table test_add_index_with_dup (a int, b int)")
	_, err := s.tk.Exec("create index c on test_add_index_with_dup(b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("create index c on test_add_index_with_dup(b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table test_add_index_with_dup add index c (b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = s.tk.Exec("alter table test_add_index_with_dup add index c (b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	s.tk.MustExec("drop table test_add_index_with_dup")
}

func (s *testDBSuite1) TestAddColumnTooMany(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	count := int(atomic.LoadUint32(&ddl.TableColumnCountLimit) - 1)
	var cols []string
	for i := 0; i < count; i++ {
		cols = append(cols, fmt.Sprintf("a%d int", i))
	}
	createSQL := fmt.Sprintf("create table t_column_too_many (%s)", strings.Join(cols, ","))
	s.tk.MustExec(createSQL)
	s.tk.MustExec("alter table t_column_too_many add column a_512 int")
	alterSQL := "alter table t_column_too_many add column a_513 int"
	s.tk.MustGetErrCode(alterSQL, mysql.ErrTooManyFields)
}

// TestDropColumn is for inserting value with a to-be-dropped column when do drop column.
// Column info from schema in build-insert-plan should be public only,
// otherwise they will not be consist with Table.Col(), then the server will panic.
func (s *testDBSuite2) TestDropColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("create database drop_col_db")
	s.tk.MustExec("use drop_col_db")
	num := 25
	multiDDL := make([]string, 0, num)
	sql := "create table t2 (c1 int, c2 int, c3 int, "
	for i := 4; i < 4+num; i++ {
		multiDDL = append(multiDDL, fmt.Sprintf("alter table t2 drop column c%d", i))

		if i != 3+num {
			sql += fmt.Sprintf("c%d int, ", i)
		} else {
			sql += fmt.Sprintf("c%d int)", i)
		}
	}
	s.tk.MustExec(sql)
	dmlDone := make(chan error, num)
	ddlDone := make(chan error, num)

	testddlutil.ExecMultiSQLInGoroutine(c, s.store, "drop_col_db", multiDDL, ddlDone)
	for i := 0; i < num; i++ {
		testddlutil.ExecMultiSQLInGoroutine(c, s.store, "drop_col_db", []string{"insert into t2 set c1 = 1, c2 = 1, c3 = 1, c4 = 1"}, dmlDone)
	}
	for i := 0; i < num; i++ {
		select {
		case err := <-ddlDone:
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		}
	}

	s.tk.MustExec("drop database drop_col_db")
}

func (s *testDBSuite4) TestChangeColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use " + s.schemaName)

	s.mustExec(c, "create table t3 (a int default '0', b varchar(10), d int not null default '0')")
	s.mustExec(c, "insert into t3 set b = 'a'")
	s.tk.MustQuery("select a from t3").Check(testkit.Rows("0"))
	s.mustExec(c, "alter table t3 change a aa bigint")
	s.mustExec(c, "insert into t3 set b = 'b'")
	s.tk.MustQuery("select aa from t3").Check(testkit.Rows("0", "<nil>"))
	// for no default flag
	s.mustExec(c, "alter table t3 change d dd bigint not null")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colD := tblInfo.Columns[2]
	hasNoDefault := mysql.HasNoDefaultValueFlag(colD.Flag)
	c.Assert(hasNoDefault, IsTrue)
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	s.mustExec(c, "alter table t3 change b b varchar(20) null default 'c'")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colB := tblInfo.Columns[1]
	hasNotNull := mysql.HasNotNullFlag(colB.Flag)
	c.Assert(hasNotNull, IsFalse)
	s.mustExec(c, "insert into t3 set aa = 3, dd = 5")
	s.tk.MustQuery("select b from t3").Check(testkit.Rows("a", "b", "c"))

	// for failing tests
	sql := "alter table t3 change aa a bigint default ''"
	s.tk.MustGetErrCode(sql, mysql.ErrInvalidDefault)
	sql = "alter table t3 change a testx.t3.aa bigint"
	s.tk.MustGetErrCode(sql, mysql.ErrWrongDBName)
	sql = "alter table t3 change t.a aa bigint"
	s.tk.MustGetErrCode(sql, mysql.ErrWrongTableName)
	s.mustExec(c, "create table t4 (c1 int, c2 int, c3 int default 1, index (c1));")
	s.tk.MustExec("insert into t4(c2) values (null);")
	sql = "alter table t4 change c1 a1 int not null;"
	s.tk.MustGetErrCode(sql, mysql.ErrInvalidUseOfNull)
	sql = "alter table t4 change c2 a bigint not null;"
	s.tk.MustGetErrCode(sql, mysql.WarnDataTruncated)
	// Rename to an existing column.
	s.mustExec(c, "alter table t3 add column a bigint")
	sql = "alter table t3 change aa a bigint"
	s.tk.MustGetErrCode(sql, mysql.ErrDupFieldName)

	s.tk.MustExec("drop table t3")
}

func (s *testDBSuite) mustExec(c *C, query string) {
	s.tk.MustExec(query)
}

func (s *testDBSuite1) TestCreateTable(c *C) {
	s.tk.MustExec("use test")
	s.tk.MustExec("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	s.tk.MustExec("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	cols := tbl.Cols()

	c.Assert(len(cols), Equals, 1)
	col := cols[0]
	c.Assert(col.Name.L, Equals, "a")
	d, ok := col.DefaultValue.(string)
	c.Assert(ok, IsTrue)
	c.Assert(d, Equals, "2.0")

	s.tk.MustExec("drop table t")

	// test for enum column
	failSQL := "create table t_enum (a enum('e','e'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('e','E'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a enum('abc','Abc'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	// test for set column
	failSQL = "create table t_enum (a set('e','e'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('e','E'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	failSQL = "create table t_enum (a set('abc','Abc'));"
	s.tk.MustGetErrCode(failSQL, mysql.ErrDuplicatedValueInType)
	_, err = s.tk.Exec("create table t_enum (a enum('B','b'));")
	c.Assert(err.Error(), Equals, "[types:1291]Column 'a' has duplicated value 'B' in ENUM")
}

func (s *testDBSuite5) TestCheckColumnDefaultValue(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test;")
	s.tk.MustExec("drop table if exists text_default_text;")
	s.tk.MustGetErrCode("create table text_default_text(c1 text not null default '');", mysql.ErrBlobCantHaveDefault)
	s.tk.MustGetErrCode("create table text_default_text(c1 text not null default 'scds');", mysql.ErrBlobCantHaveDefault)

	s.tk.MustExec("drop table if exists text_default_blob;")
	s.tk.MustGetErrCode("create table text_default_blob(c1 blob not null default '');", mysql.ErrBlobCantHaveDefault)
	s.tk.MustGetErrCode("create table text_default_blob(c1 blob not null default 'scds54');", mysql.ErrBlobCantHaveDefault)

	s.tk.MustExec("set sql_mode='';")
	s.tk.MustExec("create table text_default_text(c1 text not null default '');")
	s.tk.MustQuery(`show create table text_default_text`).Check(testutil.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_text"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")

	s.tk.MustExec("create table text_default_blob(c1 blob not null default '');")
	s.tk.MustQuery(`show create table text_default_blob`).Check(testutil.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_blob"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().Columns[0].DefaultValue, Equals, "")
}

func (s *testDBSuite3) TestColumnModifyingDefinition(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists test2;")
	s.tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	s.tk.MustExec("alter table test2 change c2 a int not null;")
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test2"))
	c.Assert(err, IsNil)
	var c2 *table.Column
	for _, col := range t.Cols() {
		if col.Name.L == "a" {
			c2 = col
		}
	}
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsTrue)

	s.tk.MustExec("drop table if exists test2;")
	s.tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	s.tk.MustExec("insert into test2(c2) values (null);")
	s.tk.MustGetErrCode("alter table test2 change c2 a int not null", mysql.ErrInvalidUseOfNull)
	s.tk.MustGetErrCode("alter table test2 change c1 a1 bigint not null;", mysql.WarnDataTruncated)
}

func (s *testDBSuite5) TestModifyColumnRollBack(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (c1 int, c2 int, c3 int default 1, index (c1));")

	var c2 *table.Column
	var checkErr error
	hook := &ddl.TestDDLCallback{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}

		t := s.testGetTable(c, "t1")
		for _, col := range t.Cols() {
			if col.Name.L == "c2" {
				c2 = col
			}
		}
		if mysql.HasPreventNullInsertFlag(c2.Flag) {
			s.tk.MustGetErrCode("insert into t1(c2) values (null);", mysql.ErrBadNull)
		}

		hookCtx := mock.NewContext()
		hookCtx.Store = s.store
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}

		jobIDs := []int64{job.ID}
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
		// It only tests cancel one DDL job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}

		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}

	originalHook := s.dom.DDL().GetHook()
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(s.store, "alter table t1 change c2 c2 bigint not null;", done)
	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[ddl:8214]Cancelled DDL job")
			break LOOP
		case <-ticker.C:
			s.mustExec(c, "insert into t1(c2) values (null);")
		}
	}

	t := s.testGetTable(c, "t1")
	for _, col := range t.Cols() {
		if col.Name.L == "c2" {
			c2 = col
		}
	}
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsFalse)
	s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)
	s.mustExec(c, "drop table t1")
}

func (s *testDBSuite1) TestModifyColumnNullToNotNull(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test_db")
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (c1 int, c2 int);")

	tbl := s.testGetTable(c, "t1")
	getModifyColumn := func() *table.Column {
		t := s.testGetTable(c, "t1")
		for _, col := range t.Cols() {
			if col.Name.L == "c2" {
				return col
			}
		}
		return nil
	}

	originalHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalHook)

	// Check insert null before job first update.
	times := 0
	hook := &ddl.TestDDLCallback{}
	s.tk.MustExec("delete from t1")
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if times == 0 {
			_, checkErr = tk2.Exec("insert into t1 values ();")
		}
		times++
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	_, err := s.tk.Exec("alter table t1 change c2 c2 int not null;")
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1138]Invalid use of NULL value")
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil>"))

	// Check insert error when column has PreventNullInsertFlag.
	s.tk.MustExec("delete from t1")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.State != model.JobStateRunning {
			return
		}
		// now c2 has PreventNullInsertFlag, an error is expected.
		_, checkErr = tk2.Exec("insert into t1 values ();")
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	s.tk.MustExec("alter table t1 change c2 c2 bigint not null;")
	c.Assert(checkErr.Error(), Equals, "[table:1048]Column 'c2' cannot be null")

	c2 := getModifyColumn()
	c.Assert(mysql.HasNotNullFlag(c2.Flag), IsTrue)
	c.Assert(mysql.HasPreventNullInsertFlag(c2.Flag), IsFalse)
	_, err = s.tk.Exec("insert into t1 values ();")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[table:1364]Field 'c2' doesn't have a default value")
}

func (s *testDBSuite3) TestTransactionWithWriteOnlyColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key);")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=2",
			"commit",
		},
	}

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		switch job.SchemaState {
		case model.StateWriteOnly:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				s.mustExec(c, sql)
			}
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	s.mustExec(c, "delete from t1")

	// test transaction on drop column.
	go backgroundExec(s.store, "alter table t1 drop column c", done)
	err = <-done
	c.Assert(err, IsNil)
	s.tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

func (s *testDBSuite4) TestAddColumn2(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.mustExec(c, "use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key, b int);")
	defer s.mustExec(c, "drop table if exists t1, t2")

	originHook := s.dom.DDL().GetHook()
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originHook)
	hook := &ddl.TestDDLCallback{}
	var writeOnlyTable table.Table
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			writeOnlyTable, _ = s.dom.InfoSchema().TableByID(job.TableID)
		}
	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(s.store, "alter table t1 add column c int not null", done)
	err := <-done
	c.Assert(err, IsNil)

	s.mustExec(c, "insert into t1 values (1,1,1)")
	s.tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 1 1"))

	// mock for outdated tidb update record.
	c.Assert(writeOnlyTable, NotNil)
	ctx := context.Background()
	err = s.tk.Se.NewTxn(ctx)
	c.Assert(err, IsNil)
	oldRow, err := writeOnlyTable.RowWithCols(s.tk.Se, 1, writeOnlyTable.WritableCols())
	c.Assert(err, IsNil)
	c.Assert(len(oldRow), Equals, 3)
	err = writeOnlyTable.RemoveRecord(s.tk.Se, 1, oldRow)
	c.Assert(err, IsNil)
	_, err = writeOnlyTable.AddRecord(s.tk.Se, types.MakeDatums(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()), table.IsUpdate)
	c.Assert(err, IsNil)
	err = s.tk.Se.StmtCommit()
	c.Assert(err, IsNil)
	err = s.tk.Se.CommitTxn(ctx)
	c.Assert(err, IsNil)

	s.tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 2 1"))

	// Test for _tidb_rowid
	var re *testkit.Result
	s.mustExec(c, "create table t2 (a int);")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		// allow write _tidb_rowid first
		s.mustExec(c, "set @@tidb_opt_write_row_id=1")
		s.mustExec(c, "begin")
		s.mustExec(c, "insert into t2 (a,_tidb_rowid) values (1,2);")
		re = s.tk.MustQuery(" select a,_tidb_rowid from t2;")
		s.mustExec(c, "commit")

	}
	s.dom.DDL().(ddl.DDLForTest).SetHook(hook)

	go backgroundExec(s.store, "alter table t2 add column b int not null default 3", done)
	err = <-done
	c.Assert(err, IsNil)
	re.Check(testkit.Rows("1 2"))
	s.tk.MustQuery("select a,b,_tidb_rowid from t2").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite4) TestIfNotExists(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key);")

	// ADD COLUMN
	sql := "alter table t1 add column b int"
	s.mustExec(c, sql)
	s.tk.MustGetErrCode(sql, mysql.ErrDupFieldName)
	s.mustExec(c, "alter table t1 add column if not exists b int")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1060|Duplicate column name 'b'"))

	// ADD INDEX
	sql = "alter table t1 add index idx_b (b)"
	s.mustExec(c, sql)
	s.tk.MustGetErrCode(sql, mysql.ErrDupKeyName)
	s.mustExec(c, "alter table t1 add index if not exists idx_b (b)")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// CREATE INDEX
	sql = "create index idx_b on t1 (b)"
	s.tk.MustGetErrCode(sql, mysql.ErrDupKeyName)
	s.mustExec(c, "create index if not exists idx_b on t1 (b)")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))
}

func (s *testDBSuite4) TestIfExists(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.mustExec(c, "drop table if exists t1")
	s.mustExec(c, "create table t1 (a int key, b int);")

	// DROP COLUMN
	sql := "alter table t1 drop column b"
	s.mustExec(c, sql)
	s.tk.MustGetErrCode(sql, mysql.ErrCantDropFieldOrKey)
	s.mustExec(c, "alter table t1 drop column if exists b") // only `a` exists now
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|column b doesn't exist"))

	// CHANGE COLUMN
	sql = "alter table t1 change column b c int"
	s.tk.MustGetErrCode(sql, mysql.ErrBadField)
	s.mustExec(c, "alter table t1 change column if exists b c int")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'b' in 't1'"))
	s.mustExec(c, "alter table t1 change column if exists a c int") // only `c` exists now

	// MODIFY COLUMN
	sql = "alter table t1 modify column a bigint"
	s.tk.MustGetErrCode(sql, mysql.ErrBadField)
	s.mustExec(c, "alter table t1 modify column if exists a bigint")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1054|Unknown column 'a' in 't1'"))
	s.mustExec(c, "alter table t1 modify column if exists c bigint") // only `c` exists now

	// DROP INDEX
	s.mustExec(c, "alter table t1 add index idx_c (c)")
	sql = "alter table t1 drop index idx_c"
	s.mustExec(c, sql)
	s.tk.MustGetErrCode(sql, mysql.ErrCantDropFieldOrKey)
	s.mustExec(c, "alter table t1 drop index if exists idx_c")
	c.Assert(s.tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	s.tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1091|index idx_c doesn't exist"))
}

func (s *testDBSuite1) TestModifyColumnCharset(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")
	s.tk.MustExec("create table t_mcc(a varchar(8) charset utf8, b varchar(8) charset utf8)")
	defer s.mustExec(c, "drop table t_mcc;")

	result := s.tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	s.tk.MustExec("alter table t_mcc modify column a varchar(8);")
	t := s.testGetTable(c, "t_mcc")
	t.Meta().Version = model.TableInfoVersion0
	// When the table version is TableInfoVersion0, the following statement don't change "b" charset.
	// So the behavior is not compatible with MySQL.
	s.tk.MustExec("alter table t_mcc modify column b varchar(8);")
	result = s.tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

}

func (s *testDBSuite2) TestSkipSchemaChecker(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	tk := s.tk
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	// Test can't skip schema checker.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1;")
	tk2.MustExec("alter table t1 add column b int;")
	_, err := tk.Exec("commit")
	c.Assert(terror.ErrorEqual(domain.ErrInfoSchemaChanged, err), IsTrue)
}

func init() {
	// Make sure it will only be executed once.
	domain.SchemaOutOfDateRetryInterval = int64(50 * time.Millisecond)
}
