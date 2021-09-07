// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testIntegrationSuite1{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite2{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite3{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite4{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite5{&testIntegrationSuite{}})

type testIntegrationSuite struct {
	lease     time.Duration
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
	ctx       sessionctx.Context
	tk        *testkit.TestKit
}

func setupIntegrationSuite(s *testIntegrationSuite, c *C) {
	var err error
	s.lease = 50 * time.Millisecond
	ddl.WaitTimeWhenErrorOccurred = 0

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.store, err = mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
	s.tk = testkit.NewTestKit(c, s.store)
}

func tearDownIntegrationSuiteTest(s *testIntegrationSuite, c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func tearDownIntegrationSuite(s *testIntegrationSuite, c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	setupIntegrationSuite(s, c)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	tearDownIntegrationSuite(s, c)
}

type testIntegrationSuite1 struct{ *testIntegrationSuite }
type testIntegrationSuite2 struct{ *testIntegrationSuite }

func (s *testIntegrationSuite2) TearDownTest(c *C) {
	tearDownIntegrationSuiteTest(s.testIntegrationSuite, c)
}

type testIntegrationSuite3 struct{ *testIntegrationSuite }
type testIntegrationSuite4 struct{ *testIntegrationSuite }
type testIntegrationSuite5 struct{ *testIntegrationSuite }

// TestInvalidNameWhenCreateTable for issue #3848
func (s *testIntegrationSuite3) TestInvalidNameWhenCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t(xxx.t.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongDBName), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t(test.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongTableName), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t(t.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongDBName), IsTrue, Commentf("err %v", err))
}

// TestCreateTableIfNotExists for issue #6879
func (s *testIntegrationSuite3) TestCreateTableIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	tk.MustExec("create table ct1(a bigint)")
	tk.MustExec("create table ct(a bigint)")

	// Test duplicate create-table without `LIKE` clause
	tk.MustExec("create table if not exists ct(b bigint, c varchar(60));")
	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), IsTrue)
}

// for issue #9910
func (s *testIntegrationSuite2) TestCreateTableWithKeyWord(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), node_state varchar(20));")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite1) TestUniqueKeyNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a int primary key, b varchar(255))")

	tk.MustExec("insert into t values(1, NULL)")
	tk.MustExec("insert into t values(2, NULL)")
	tk.MustExec("alter table t add unique index b(b);")
	res := tk.MustQuery("select count(*) from t use index(b);")
	res.Check(testkit.Rows("2"))
}

func (s *testIntegrationSuite3) TestEndIncluded(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < ddl.DefaultTaskHandleCnt+1; i++ {
		tk.MustExec("insert into t values(1, 1)")
	}
	tk.MustExec("alter table t add index b(b);")
}

// TestModifyColumnAfterAddIndex Issue 5134
func (s *testIntegrationSuite3) TestModifyColumnAfterAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table city (city VARCHAR(2) KEY);")
	tk.MustExec("alter table city change column city city varchar(50);")
	tk.MustExec(`insert into city values ("abc"), ("abd");`)
}

func (s *testIntegrationSuite3) TestIssue2293(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_issue_2293 (a int)")
	tk.MustGetErrCode("alter table t_issue_2293 add b int not null default 'a'", mysql.ErrInvalidDefault)
	tk.MustExec("insert into t_issue_2293 value(1)")
	tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite1) TestIndexLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))")
	tk.MustExec("create index idx on idx_len(a)")
	tk.MustExec("alter table idx_len add index idxa(a)")
	tk.MustExec("create index idx1 on idx_len(b)")
	tk.MustExec("alter table idx_len add index idxb(b)")
	tk.MustExec("create index idx2 on idx_len(c)")
	tk.MustExec("alter table idx_len add index idxc(c)")
	tk.MustExec("create index idx3 on idx_len(d)")
	tk.MustExec("alter table idx_len add index idxd(d)")
	tk.MustExec("create index idx4 on idx_len(f)")
	tk.MustExec("alter table idx_len add index idxf(f)")
	tk.MustExec("create index idx5 on idx_len(g)")
	tk.MustExec("alter table idx_len add index idxg(g)")
	tk.MustExec("create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))")
}

func (s *testIntegrationSuite3) TestIssue3833(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table issue3833 (b char(0), c binary(0), d  varchar(0))")
	tk.MustGetErrCode("create index idx on issue3833 (b)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (b)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(b))", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (c)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (c)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(c))", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (d)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (d)", mysql.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(d))", mysql.ErrWrongKeyColumn)
}

func (s *testIntegrationSuite3) TestTableDDLWithFloatType(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustGetErrCode("create table t (a decimal(1, 2))", mysql.ErrMBiggerThanD)
	s.tk.MustGetErrCode("create table t (a float(1, 2))", mysql.ErrMBiggerThanD)
	s.tk.MustGetErrCode("create table t (a double(1, 2))", mysql.ErrMBiggerThanD)
	s.tk.MustExec("create table t (a double(1, 1))")
	s.tk.MustGetErrCode("alter table t add column b decimal(1, 2)", mysql.ErrMBiggerThanD)
	// add multi columns now not support, so no case.
	s.tk.MustGetErrCode("alter table t modify column a float(1, 4)", mysql.ErrMBiggerThanD)
	s.tk.MustGetErrCode("alter table t change column a aa float(1, 4)", mysql.ErrMBiggerThanD)
	s.tk.MustExec("drop table t")
}

func (s *testIntegrationSuite1) TestTableDDLWithTimeType(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustGetErrCode("create table t (a time(7))", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("create table t (a datetime(7))", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("create table t (a timestamp(7))", mysql.ErrTooBigPrecision)
	_, err := s.tk.Exec("create table t (a time(-1))")
	c.Assert(err, NotNil)
	s.tk.MustExec("create table t (a datetime)")
	s.tk.MustGetErrCode("alter table t add column b time(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t add column b datetime(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t add column b timestamp(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t modify column a time(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t modify column a datetime(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t modify column a timestamp(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t change column a aa time(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t change column a aa datetime(7)", mysql.ErrTooBigPrecision)
	s.tk.MustGetErrCode("alter table t change column a aa timestamp(7)", mysql.ErrTooBigPrecision)
	s.tk.MustExec("alter table t change column a aa datetime(0)")
	s.tk.MustExec("drop table t")
}

func (s *testIntegrationSuite5) TestBackwardCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_backward_compatibility")
	defer tk.MustExec("drop database test_backward_compatibility")
	tk.MustExec("use test_backward_compatibility")
	tk.MustExec("create table t(a int primary key, b int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// alter table t add index idx_b(b);
	is := s.dom.InfoSchema()
	schemaName := model.NewCIStr("test_backward_compatibility")
	tableName := model.NewCIStr("t")
	schema, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.TableByName(schemaName, tableName)
	c.Assert(err, IsNil)

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tbl.Meta().ID, 100)

	unique := false
	indexName := model.NewCIStr("idx_b")
	idxColName := &ast.IndexPartSpecification{
		Column: &ast.ColumnName{
			Schema: schemaName,
			Table:  tableName,
			Name:   model.NewCIStr("b"),
		},
		Length: types.UnspecifiedLength,
	}
	idxColNames := []*ast.IndexPartSpecification{idxColName}
	var indexOption *ast.IndexOption
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames, indexOption},
	}
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

	// Simulate old TiDB init the add index job, old TiDB will not init the model.Job.ReorgMeta field,
	// if we set job.SnapshotVer here, can simulate the behavior.
	job.SnapshotVer = txn.StartTS()
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	ticker := time.NewTicker(s.lease)
	defer ticker.Stop()
	for range ticker.C {
		historyJob, err := s.getHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		if historyJob == nil {

			continue
		}
		c.Assert(historyJob.Error, IsNil)

		if historyJob.IsSynced() {
			break
		}
	}

	// finished add index

}

func (s *testIntegrationSuite3) TestMultiRegionGetTableEndHandle(c *C) {
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
}

type testMaxTableRowIDContext struct {
	c   *C
	d   ddl.DDL
	tbl table.Table
}

func newTestMaxTableRowIDContext(c *C, d ddl.DDL, tbl table.Table) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		c:   c,
		d:   d,
		tbl: tbl,
	}
}

func getMaxTableRowID(ctx *testMaxTableRowIDContext, store kv.Storage) (int64, bool) {
	c := ctx.c
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	maxID, emptyTable, err := d.GetTableMaxRowID(curVer.Ver, tbl.(table.PhysicalTable))
	c.Assert(err, IsNil)
	return maxID, emptyTable
}

func checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, store kv.Storage, expectEmpty bool, expectMaxID int64) {
	c := ctx.c
	maxID, emptyTable := getMaxTableRowID(ctx, store)
	c.Assert(emptyTable, Equals, expectEmpty)
	c.Assert(maxID, Equals, expectMaxID)
}

func (s *testIntegrationSuite) getHistoryDDLJob(id int64) (*model.Job, error) {
	var job *model.Job

	err := kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func (s *testIntegrationSuite2) TestAddIndexAfterAddColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")

	s.tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0')")
	s.tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2)")
	s.tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0'")
	sql := "alter table test_add_index_after_add_col add unique index cc(c) "
	s.tk.MustGetErrCode(sql, mysql.ErrDupEntry)
	sql = "alter table test_add_index_after_add_col add index idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);"
	s.tk.MustGetErrCode(sql, mysql.ErrTooManyKeyParts)
}

func (s *testIntegrationSuite2) TestAddAnonymousIndex(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("create table t_anonymous_index (c1 int, c2 int, C3 int)")
	s.tk.MustExec("alter table t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	_, err := s.tk.Exec("alter table t_anonymous_index drop index")
	c.Assert(err, NotNil)
	// The index name is c1 when adding index (c1, c2).
	s.tk.MustExec("alter table t_anonymous_index drop index c1")
	t := testGetTableByName(c, s.ctx, "test", "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 0)
	// for adding some indices that the first column name is c1
	s.tk.MustExec("alter table t_anonymous_index add index (c1)")
	_, err = s.tk.Exec("alter table t_anonymous_index add index c1 (c2)")
	c.Assert(err, NotNil)
	t = testGetTableByName(c, s.ctx, "test", "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 1)
	idx := t.Indices()[0].Meta().Name.L
	c.Assert(idx, Equals, "c1")
	// The MySQL will be a warning.
	s.tk.MustExec("alter table t_anonymous_index add index c1_3 (c1)")
	s.tk.MustExec("alter table t_anonymous_index add index (c1, c2, C3)")
	// The MySQL will be a warning.
	s.tk.MustExec("alter table t_anonymous_index add index (c1)")
	t = testGetTableByName(c, s.ctx, "test", "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 4)
	s.tk.MustExec("alter table t_anonymous_index drop index c1")
	s.tk.MustExec("alter table t_anonymous_index drop index c1_2")
	s.tk.MustExec("alter table t_anonymous_index drop index c1_3")
	s.tk.MustExec("alter table t_anonymous_index drop index c1_4")
	// for case insensitive
	s.tk.MustExec("alter table t_anonymous_index add index (C3)")
	s.tk.MustExec("alter table t_anonymous_index drop index c3")
	s.tk.MustExec("alter table t_anonymous_index add index c3 (C3)")
	s.tk.MustExec("alter table t_anonymous_index drop index C3")
	// for anonymous index with column name `primary`
	s.tk.MustExec("create table t_primary (`primary` int, key (`primary`))")
	t = testGetTableByName(c, s.ctx, "test", "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	s.tk.MustExec("create table t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	t = testGetTableByName(c, s.ctx, "test", "t_primary_2")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.tk.MustExec("create table t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	t = testGetTableByName(c, s.ctx, "test", "t_primary_3")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
}

func (s *testIntegrationSuite1) TestAddColumnTooMany(c *C) {
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

func (s *testIntegrationSuite3) TestAlterColumn(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test_db")

	s.tk.MustExec("create table test_alter_column (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on update current_timestamp)")
	s.tk.MustExec("insert into test_alter_column set b = 'a', c = 'aa'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111"))
	ctx := s.tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colA := tblInfo.Columns[0]
	hasNoDefault := mysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.tk.MustExec("alter table test_alter_column alter column a set default 222")
	s.tk.MustExec("insert into test_alter_column set b = 'b', c = 'bb'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colA = tblInfo.Columns[0]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	s.tk.MustExec("alter table test_alter_column alter column b set default null")
	s.tk.MustExec("insert into test_alter_column set c = 'cc'")
	s.tk.MustQuery("select b from test_alter_column").Check(testkit.Rows("a", "b", "<nil>"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsTrue)
	s.tk.MustExec("alter table test_alter_column alter column c set default 'xx'")
	s.tk.MustExec("insert into test_alter_column set a = 123")
	s.tk.MustQuery("select c from test_alter_column").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsFalse)
	// TODO: After fix issue 2606.
	// s.tk.MustExec( "alter table test_alter_column alter column d set default null")
	s.tk.MustExec("alter table test_alter_column alter column a drop default")
	s.tk.MustExec("insert into test_alter_column set b = 'd', c = 'dd'")
	s.tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222", "222", "123", "<nil>"))

	// for failing tests
	sql := "alter table db_not_exist.test_alter_column alter column b set default 'c'"
	s.tk.MustGetErrCode(sql, mysql.ErrNoSuchTable)
	sql = "alter table test_not_exist alter column b set default 'c'"
	s.tk.MustGetErrCode(sql, mysql.ErrNoSuchTable)
	sql = "alter table test_alter_column alter column col_not_exist set default 'c'"
	s.tk.MustGetErrCode(sql, mysql.ErrBadField)
	sql = "alter table test_alter_column alter column c set default null"
	s.tk.MustGetErrCode(sql, mysql.ErrInvalidDefault)

	// The followings tests whether adding constraints via change / modify column
	// is forbidden as expected.
	s.tk.MustExec("drop table if exists mc")
	s.tk.MustExec("create table mc(a int key, b int, c int)")
	_, err = s.tk.Exec("alter table mc modify column a int key") // Adds a new primary key
	c.Assert(err, NotNil)
	_, err = s.tk.Exec("alter table mc modify column c int unique") // Adds a new unique key
	c.Assert(err, NotNil)
	result := s.tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Change / modify column should preserve index options.
	s.tk.MustExec("drop table if exists mc")
	s.tk.MustExec("create table mc(a int key, b int, c int unique)")
	s.tk.MustExec("alter table mc modify column a bigint") // NOT NULL & PRIMARY KEY should be preserved
	s.tk.MustExec("alter table mc modify column b bigint")
	s.tk.MustExec("alter table mc modify column c bigint") // Unique should be preserved
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	s.tk.MustExec("drop table if exists mc")
	s.tk.MustExec("create table mc(a int key auto_increment, b int)")
	s.tk.MustExec("alter table mc modify column a bigint auto_increment") // Keeps auto_increment
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)
	_, err = s.tk.Exec("alter table mc modify column a bigint") // Droppping auto_increment is not allow when @@tidb_allow_remove_auto_inc == 'off'
	c.Assert(err, NotNil)
	s.tk.MustExec("set @@tidb_allow_remove_auto_inc = on")
	s.tk.MustExec("alter table mc modify column a bigint") // Dropping auto_increment is ok when @@tidb_allow_remove_auto_inc == 'on'
	result = s.tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	_, err = s.tk.Exec("alter table mc modify column a bigint auto_increment") // Adds auto_increment should throw error
	c.Assert(err, NotNil)

	s.tk.MustExec("drop table if exists t")

	s.tk.MustExec("drop table if exists multi_unique")
	s.tk.MustExec("create table multi_unique (a int unique unique)")
	s.tk.MustExec("drop table multi_unique")
	s.tk.MustExec("create table multi_unique (a int key primary key unique unique)")
	s.tk.MustExec("drop table multi_unique")
	s.tk.MustExec("create table multi_unique (a int key unique unique key unique)")
	s.tk.MustExec("drop table multi_unique")
	s.tk.MustExec("create table multi_unique (a serial serial default value)")
	s.tk.MustExec("drop table multi_unique")
	s.tk.MustExec("create table multi_unique (a serial serial default value serial default value)")
	s.tk.MustExec("drop table multi_unique")
}

func (s *testIntegrationSuite4) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int auto_increment, unique key (a))")
	dropIndexSQL := "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, mysql.ErrWrongAutoKey)

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment, b int(11), c bigint, unique key (a, b, c))")
	dropIndexSQL = "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, mysql.ErrWrongAutoKey)
}
