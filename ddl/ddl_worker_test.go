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

package ddl

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDDLSuite) SetUpSuite(c *C) {
	WaitTimeWhenErrorOccurred = 1 * time.Microsecond
}

func (s *testDDLSuite) TearDownSuite(c *C) {
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	c.Assert(d1.GetLease(), Equals, testLease)
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestTableError(c *C) {
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong, so dropping table is failed.
	doDDLJobErr(c, -1, 1, model.ActionDropTable, nil, ctx, d)
	// Table ID is wrong, so dropping table is failed.
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	job := doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropTable, nil, ctx, d)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		t := meta.NewMeta(txn)
		_, err1 := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Args is wrong, so creating table is failed.
	doDDLJobErr(c, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, d)
	// Schema ID is wrong, so creating table is failed.
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)
	// Table exists, so creating table is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)

}

func (s *testDDLSuite) TestInvalidDDLJob(c *C) {
	store := testCreateStore(c, "test_invalid_ddl_job_type_error")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err.Error(), Equals, "[ddl:8204]invalid ddl job type: none")
}

func (s *testDDLSuite) TestIndexError(c *C) {
	store := testCreateStore(c, "test_index_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong.
	doDDLJobErr(c, -1, 1, model.ActionAddIndex, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropIndex, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	// for adding index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, []interface{}{1}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("t"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func testCheckOwner(c *C, d *ddl, expectedVal bool) {
	c.Assert(d.isOwner(), Equals, expectedVal)
}

func testCheckJobDone(c *C, d *ddl, job *model.Job, isAdd bool) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		checkHistoryJob(c, historyJob)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *ddl, job *model.Job, state *model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("history job %s", historyJob))
		if state != nil {
			c.Assert(historyJob.SchemaState, Equals, *state)
		}
		return nil
	})
}

func doDDLJobErrWithSchemaState(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}, state *model.SchemaState) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	// TODO: Add the detail error check.
	c.Assert(err, NotNil, Commentf("err:%v", err))
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDDLJobSuccess(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}) {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, c, schemaID, tableID, tp, args, nil)
}

func checkCancelState(txn kv.Transaction, job *model.Job, test *testCancelJob) error {
	var checkErr error
	addIndexFirstReorg := (test.act == model.ActionAddIndex || test.act == model.ActionAddPrimaryKey) &&
		job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0
	// If the action is adding index and the state is writing reorganization, it wants to test the case of cancelling the job when backfilling indexes.
	// When the job satisfies this case of addIndexFirstReorg, the worker hasn't started to backfill indexes.
	if test.cancelState == job.SchemaState && !addIndexFirstReorg && !job.IsRollingback() {
		errs, err := admin.CancelJobs(txn, test.jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return checkErr
		}
		// It only tests cancel one DDL job.
		if !terror.ErrorEqual(errs[0], test.cancelRetErrs[0]) {
			checkErr = errors.Trace(errs[0])
			return checkErr
		}
	}
	return checkErr
}

type testCancelJob struct {
	jobIDs        []int64
	cancelRetErrs []error          // cancelRetErrs is the first return value of CancelJobs.
	act           model.ActionType // act is the job action.
	cancelState   model.SchemaState
}

func buildCancelJobTests(firstID int64) []testCancelJob {
	noErrs := []error{nil}
	tests := []testCancelJob{
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 1}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 2}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 3}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 4}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 4)}, cancelState: model.StatePublic},

		// Test cancel drop index job , see TestCancelDropIndex.
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 5}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 6}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 7}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 8}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 8)}, cancelState: model.StatePublic},

		// Test create table, watch out, table id will alloc a globalID.
		{act: model.ActionCreateTable, jobIDs: []int64{firstID + 10}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		// Test create database, watch out, database id will alloc a globalID.
		{act: model.ActionCreateSchema, jobIDs: []int64{firstID + 12}, cancelRetErrs: noErrs, cancelState: model.StateNone},

		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 13}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 13)}, cancelState: model.StateDeleteOnly},
		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 14}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 14)}, cancelState: model.StateWriteOnly},
		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 15}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 15)}, cancelState: model.StateWriteReorganization},
		{act: model.ActionRebaseAutoID, jobIDs: []int64{firstID + 16}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionShardRowID, jobIDs: []int64{firstID + 17}, cancelRetErrs: noErrs, cancelState: model.StateNone},

		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 18}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 19}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 20}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 20)}, cancelState: model.StatePublic},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 21}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 22}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 22)}, cancelState: model.StatePublic},

		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 23}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 24}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 24)}, cancelState: model.StatePublic},

		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 19}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 20}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 26)}, cancelState: model.StatePublic},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 27}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 28}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 28)}, cancelState: model.StatePublic},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 30}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 31}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 31)}, cancelState: model.StatePublic},

		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 32}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 33}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 34}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 35}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 35)}, cancelState: model.StatePublic},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 36}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 37}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 37)}, cancelState: model.StateDeleteOnly},
	}

	return tests
}

func (s *testDDLSuite) checkAddIdx(c *C, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	checkIdxExist(c, d, schemaID, tableID, idxName, success)
}

func checkIdxExist(c *C, d *ddl, schemaID int64, tableID int64, idxName string, expectedExist bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, idxInfo := range changedTable.Meta().Indices {
		if idxInfo.Name.O == idxName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, expectedExist)
}

func (s *testDDLSuite) checkAddColumn(c *C, d *ddl, schemaID int64, tableID int64, colName string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, colInfo := range changedTable.Meta().Columns {
		if colInfo.Name.O == colName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, success)
}

func (s *testDDLSuite) checkCancelDropColumn(c *C, d *ddl, schemaID int64, tableID int64, colName string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	notFound := true
	for _, colInfo := range changedTable.Meta().Columns {
		if colInfo.Name.O == colName {
			notFound = false
			break
		}
	}
	c.Assert(notFound, Equals, success)
}

func (s *testDDLSuite) TestCancelJob(c *C) {
	store := testCreateStore(c, "test_cancel_job")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_cancel_job")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	// create a partition table.
	partitionTblInfo := testTableInfo(c, d, "t_partition", 5)
	// Skip using sessPool. Make sure adding primary key can be successful.
	partitionTblInfo.Columns[0].Flag |= mysql.NotNullFlag
	// create table t (c1 int, c2 int, c3 int, c4 int, c5 int);
	tblInfo := testTableInfo(c, d, "t", 5)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateTable(c, ctx, d, dbInfo, partitionTblInfo)
	tableAutoID := int64(100)
	shardRowIDBits := uint64(5)
	tblInfo.AutoIncID = tableAutoID
	tblInfo.ShardRowIDBits = shardRowIDBits
	job := testCreateTable(c, ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2, 3, 4, 5);
	originTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2, 3, 4, 5)
	_, err = originTable.AddRecord(ctx, row)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDDLCallback{}
	// set up hook
	firstJobID := job.ID
	tests := buildCancelJobTests(firstJobID)
	var checkErr error
	var mu sync.Mutex
	var test *testCancelJob
	updateTest := func(t *testCancelJob) {
		mu.Lock()
		test = t
		mu.Unlock()
	}
	hookCancelFunc := func(job *model.Job) {
		if job.State == model.JobStateSynced || job.State == model.JobStateCancelled || job.State == model.JobStateCancelling {
			return
		}
		// This hook only valid for the related test job.
		// This is use to avoid parallel test fail.
		mu.Lock()
		if len(test.jobIDs) > 0 && test.jobIDs[0] != job.ID {
			mu.Unlock()
			return
		}
		mu.Unlock()
		if checkErr != nil {
			return
		}

		hookCtx := mock.NewContext()
		hookCtx.Store = store
		err1 := hookCtx.NewTxn(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		txn, err1 = hookCtx.Txn(true)
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		mu.Lock()
		checkErr = checkCancelState(txn, job, test)
		mu.Unlock()
		if checkErr != nil {
			return
		}
		err1 = txn.Commit(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
	}
	tc.onJobUpdated = hookCancelFunc
	tc.onJobRunBefore = hookCancelFunc
	d.SetHook(tc)

	// for adding index
	updateTest(&tests[0])
	idxOrigName := "idx"
	validArgs := []interface{}{false, model.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}

	// When the job satisfies this test case, the option will be rollback, so the job's schema state is none.
	cancelState := model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[1])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[2])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[3])
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add column
	updateTest(&tests[4])
	addingColName := "colA"
	newColumnDef := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
		Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
		Options: []*ast.ColumnOption{},
	}
	col, _, err := buildColumnAndConstraint(ctx, 2, newColumnDef, nil)
	c.Assert(err, IsNil)

	addColumnArgs := []interface{}{col, 0}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)

	updateTest(&tests[5])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)

	updateTest(&tests[6])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)

	updateTest(&tests[7])
	testAddColumn(c, ctx, d, dbInfo, tblInfo, addColumnArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, true)

	// for create table
	tblInfo1 := testTableInfo(c, d, "t1", 2)
	updateTest(&tests[8])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo1.ID, model.ActionCreateTable, []interface{}{tblInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckTableState(c, d, dbInfo, tblInfo1, model.StateNone)

	// for create database
	dbInfo1 := testSchemaInfo(c, d, "test_cancel_job1")
	updateTest(&tests[9])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo1.ID, 0, model.ActionCreateSchema, []interface{}{dbInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckSchemaState(c, d, dbInfo1, model.StateNone)

	// for drop column.
	updateTest(&tests[10])
	dropColName := "c3"
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, true)

	updateTest(&tests[11])
	dropColName = "c4"
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, true)

	updateTest(&tests[12])
	dropColName = "c5"
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumn(c, d, dbInfo.ID, tblInfo.ID, dropColName, true)

	// cancel rebase auto id
	updateTest(&tests[13])
	rebaseIDArgs := []interface{}{int64(200)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionRebaseAutoID, rebaseIDArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	changedTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().AutoIncID, Equals, tableAutoID)

	// cancel shard bits
	updateTest(&tests[14])
	shardRowIDArgs := []interface{}{uint64(7)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionShardRowID, shardRowIDArgs, &cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().ShardRowIDBits, Equals, shardRowIDBits)

	// modify column
	col.DefaultValue = "1"
	updateTest(&tests[15])
	modifyColumnArgs := []interface{}{col, col.Name, byte(0)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyColumnArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	changedCol := model.FindColumnInfo(changedTable.Meta().Columns, col.Name.L)
	c.Assert(changedCol.DefaultValue, IsNil)

	// test modify table charset failed caused by canceled.
	test = &tests[22]
	modifyTableCharsetArgs := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Charset, Equals, "utf8")
	c.Assert(changedTable.Meta().Collate, Equals, "utf8_bin")

	// test modify table charset successfully.
	test = &tests[23]
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Charset, Equals, "utf8mb4")
	c.Assert(changedTable.Meta().Collate, Equals, "utf8mb4_bin")
}

func (s *testDDLSuite) TestIgnorableSpec(c *C) {
	specs := []ast.AlterTableType{
		ast.AlterTableOption,
		ast.AlterTableAddColumns,
		ast.AlterTableAddConstraint,
		ast.AlterTableDropColumn,
		ast.AlterTableDropPrimaryKey,
		ast.AlterTableDropIndex,
		ast.AlterTableModifyColumn,
		ast.AlterTableChangeColumn,
		ast.AlterTableAlterColumn,
	}
	for _, spec := range specs {
		c.Assert(isIgnorableSpec(spec), IsFalse)
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		c.Assert(isIgnorableSpec(spec), IsTrue)
	}
}

func (s *testDDLSuite) TestBuildJobDependence(c *C) {
	store := testCreateStore(c, "test_set_job_relation")
	defer store.Close()

	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.EnQueueDDLJob(job1)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job2)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job3)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job6)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job7)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job9)
		c.Assert(err, IsNil)
		return nil
	})
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job4)
		c.Assert(err, IsNil)
		c.Assert(job4.DependencyID, Equals, int64(2))
		return nil
	})
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job5)
		c.Assert(err, IsNil)
		c.Assert(job5.DependencyID, Equals, int64(3))
		return nil
	})
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job8)
		c.Assert(err, IsNil)
		c.Assert(job8.DependencyID, Equals, int64(0))
		return nil
	})
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job10)
		c.Assert(err, IsNil)
		c.Assert(job10.DependencyID, Equals, int64(9))
		return nil
	})
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job12)
		c.Assert(err, IsNil)
		c.Assert(job12.DependencyID, Equals, int64(7))
		return nil
	})
}

func (s *testDDLSuite) TestDDLPackageExecuteSQL(c *C) {
	store := testCreateStore(c, "test_run_sql")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	testCheckOwner(c, d, true)
	defer d.Stop()
	worker := d.generalWorker()
	c.Assert(worker, NotNil)

	// In test environment, worker.ctxPool will be nil, and get will return mock.Context.
	// We just test that can use it to call sqlexec.SQLExecutor.Execute.
	sess, err := worker.sessPool.get()
	c.Assert(err, IsNil)
	defer worker.sessPool.put(sess)
	se := sess.(sqlexec.SQLExecutor)
	_, _ = se.Execute(context.Background(), "create table t(a int);")
}
