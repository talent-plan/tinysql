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

package ddl_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

var _ = Suite(&testStateChangeSuite{})
var _ = SerialSuites(&serialTestStateChangeSuite{})

type serialTestStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuiteBase struct {
	lease  time.Duration
	store  kv.Storage
	dom    *domain.Domain
	se     session.Session
	p      *parser.Parser
	preSQL string
}

func (s *testStateChangeSuiteBase) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	ddl.WaitTimeWhenErrorOccured = 1 * time.Microsecond
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create database test_db_state default charset utf8 default collate utf8_bin")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testStateChangeSuiteBase) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

type sqlWithErr struct {
	sql       string
	expectErr error
}

type expectQuery struct {
	sql  string
	rows []string
}

// TestDeletaOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnly(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	dropColumnSQL := "alter table t drop column c1"
	s.runTestInSchemaState(c, model.StateDeleteOnly, "", dropColumnSQL, sqls, nil)
}

func (s *testStateChangeSuiteBase) runTestInSchemaState(c *C, state model.SchemaState, tableName, alterTableSQL string,
	sqlWithErrs []sqlWithErr, expectQuery *expectQuery) {
	_, err := s.se.Execute(context.Background(), `create table t (
		c1 varchar(64),
		c2 varchar(64),
		c3 varchar(64),
		c4 int primary key,
		unique key idx2 (c2, c3))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")
	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'N', '2017-07-01', 8)")
	c.Assert(err, IsNil)

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	var checkErr error
	times := 0
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		if job.SchemaState != state {
			return
		}
		for _, sqlWithErr := range sqlWithErrs {
			_, err = se.Execute(context.Background(), sqlWithErr.sql)
			if !terror.ErrorEqual(err, sqlWithErr.expectErr) {
				checkErr = err
				break
			}
		}
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.(ddl.DDLForTest).SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	d.(ddl.DDLForTest).SetHook(originalCallback)

	if expectQuery != nil {
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test_db_state")
		result, err := s.execQuery(tk, expectQuery.sql)
		c.Assert(err, IsNil)
		err = checkResult(result, testkit.Rows(expectQuery.rows...))
		c.Assert(err, IsNil)
	}
}

func (s *testStateChangeSuiteBase) execQuery(tk *testkit.TestKit, sql string) (*testkit.Result, error) {
	comment := Commentf("sql:%s", sql)
	rs, err := tk.Exec(sql)
	if err != nil {
		return nil, err
	}
	result := tk.ResultSetToResult(rs, comment)
	return result, nil
}

func checkResult(result *testkit.Result, expected [][]interface{}) error {
	got := fmt.Sprintf("%s", result.Rows())
	need := fmt.Sprintf("%s", expected)
	if got != need {
		return fmt.Errorf("need %v, but got %v", need, got)
	}
	return nil
}

func (s *testStateChangeSuite) TestParallelAlterModifyColumn(c *C) {
	sql := "ALTER TABLE t MODIFY COLUMN b int;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "select * from t")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

func (s *testStateChangeSuite) TestParallelAddColumAndSetDefaultValue(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create table tx (
		c1 varchar(64),
		c2 varchar(64),
		primary key idx2 (c2, c1))`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tx values('a', 'N')")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table tx")

	sql1 := "alter table tx add column cx int"
	sql2 := "alter table tx alter c2 set default 'N'"

	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "delete from tx where c1='a'")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelChangeColumnName(c *C) {
	sql1 := "ALTER TABLE t CHANGE a aa int;"
	sql2 := "ALTER TABLE t CHANGE b aa int;"
	f := func(c *C, err1, err2 error) {
		// Make sure only a DDL encounters the error of 'duplicate column name'.
		var oneErr error
		if (err1 != nil && err2 == nil) || (err1 == nil && err2 != nil) {
			if err1 != nil {
				oneErr = err1
			} else {
				oneErr = err2
			}
		}
		c.Assert(oneErr.Error(), Equals, "[schema:1060]Duplicate column name 'aa'")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAddIndex(c *C) {
	sql1 := "ALTER TABLE t add index index_b(b);"
	sql2 := "CREATE INDEX index_b ON t (c);"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1061]index already exist index_b")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelDropColumn(c *C) {
	sql := "ALTER TABLE t drop COLUMN c ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1091]column c doesn't exist")
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

func (s *testStateChangeSuite) TestParallelDropIndex(c *C) {
	sql1 := "alter table t drop index idx1 ;"
	sql2 := "alter table t drop index idx2 ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

type checkRet func(c *C, err1, err2 error)

func (s *testStateChangeSuiteBase) testControlParallelExecSQL(c *C, sql1, sql2 string, f checkRet) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int, d int auto_increment,e int, index idx1(d), index idx2(d,e))")
	c.Assert(err, IsNil)
	if len(s.preSQL) != 0 {
		_, err := s.se.Execute(context.Background(), s.preSQL)
		c.Assert(err, IsNil)
	}
	defer s.se.Execute(context.Background(), "drop table t")

	_, err = s.se.Execute(context.Background(), "drop database if exists t_part")
	c.Assert(err, IsNil)
	s.se.Execute(context.Background(), `create table t_part (a int key);`)

	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				jobs, err1 := admin.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)

	wg := sync.WaitGroup{}
	var err1 error
	var err2 error
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	wg.Add(2)
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				jobs, err3 := admin.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		_, err1 = se.Execute(context.Background(), sql1)
	}()
	go func() {
		defer wg.Done()
		<-ch
		_, err2 = se1.Execute(context.Background(), sql2)
	}()

	wg.Wait()
	f(c, err1, err2)
}

func (s *testStateChangeSuite) testParallelExecSQL(c *C, sql string) {
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	se1, err1 := session.CreateSession(s.store)
	c.Assert(err1, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	var err2, err3 error
	wg := sync.WaitGroup{}

	callback := &ddl.TestDDLCallback{}
	once := sync.Once{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		// sleep a while, let other job enqueue.
		once.Do(func() {
			time.Sleep(time.Millisecond * 10)
		})
	}

	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err2 = se.Execute(context.Background(), sql)
	}()

	go func() {
		defer wg.Done()
		_, err3 = se1.Execute(context.Background(), sql)
	}()
	wg.Wait()
	c.Assert(err2, IsNil)
	c.Assert(err3, IsNil)
}

// TestCreateTableIfNotExists parallel exec create table if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateTableIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_not_exists")
	s.testParallelExecSQL(c, "create table if not exists test_not_exists(a int);")
}

// TestCreateDBIfNotExists parallel exec create database if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateDBIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop database test_not_exists")
	s.testParallelExecSQL(c, "create database if not exists test_not_exists;")
}

// TestDDLIfNotExists parallel exec some DDLs with `if not exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDDLIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_not_exists")
	_, err := s.se.Execute(context.Background(), "create table if not exists test_not_exists(a int)")
	c.Assert(err, IsNil)

	// ADD COLUMN
	s.testParallelExecSQL(c, "alter table test_not_exists add column if not exists b int")

	// ADD INDEX
	s.testParallelExecSQL(c, "alter table test_not_exists add index if not exists idx_b (b)")

	// CREATE INDEX
	s.testParallelExecSQL(c, "create index if not exists idx_b on test_not_exists (b)")
}

// TestDDLIfExists parallel exec some DDLs with `if exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDDLIfExists(c *C) {
	defer func() {
		s.se.Execute(context.Background(), "drop table test_exists")
		s.se.Execute(context.Background(), "drop table test_exists_2")
	}()
	_, err := s.se.Execute(context.Background(), "create table if not exists test_exists (a int key, b int)")
	c.Assert(err, IsNil)

	// DROP COLUMN
	s.testParallelExecSQL(c, "alter table test_exists drop column if exists b") // only `a` exists now

	// CHANGE COLUMN
	s.testParallelExecSQL(c, "alter table test_exists change column if exists a c int") // only, `c` exists now

	// MODIFY COLUMN
	s.testParallelExecSQL(c, "alter table test_exists modify column if exists a bigint")

	// DROP INDEX
	_, err = s.se.Execute(context.Background(), "alter table test_exists add index idx_c (c)")
	c.Assert(err, IsNil)
	s.testParallelExecSQL(c, "alter table test_exists drop index if exists idx_c")
}

// TestParallelDDLBeforeRunDDLJob tests a session to execute DDL with an outdated information schema.
// This test is used to simulate the following conditions:
// In a cluster, TiDB "a" executes the DDL.
// TiDB "b" fails to load schema, then TiDB "b" executes the DDL statement associated with the DDL statement executed by "a".
func (s *testStateChangeSuite) TestParallelDDLBeforeRunDDLJob(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_table")
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table test_table (c1 int, c2 int default 1, index (c1))")
	c.Assert(err, IsNil)

	// Create two sessions.
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	intercept := &ddl.TestInterceptor{}
	firstConnID := uint64(1)
	finishedCnt := int32(0)
	interval := 5 * time.Millisecond
	var sessionCnt int32 // sessionCnt is the number of sessions that goes into the function of OnGetInfoSchema.
	intercept.OnGetInfoSchemaExported = func(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
		// The following code is for testing.
		// Make sure the two sessions get the same information schema before executing DDL.
		// After the first session executes its DDL, then the second session executes its DDL.
		var info infoschema.InfoSchema
		atomic.AddInt32(&sessionCnt, 1)
		for {
			// Make sure there are two sessions running here.
			if atomic.LoadInt32(&sessionCnt) == 2 {
				info = is
				break
			}
			// Print log to notify if TestParallelDDLBeforeRunDDLJob hang up
			log.Info("sleep in TestParallelDDLBeforeRunDDLJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		currID := ctx.GetSessionVars().ConnectionID
		for {
			seCnt := atomic.LoadInt32(&sessionCnt)
			// Make sure the two session have got the same information schema. And the first session can continue to go on,
			// or the frist session finished this SQL(seCnt = finishedCnt), then other sessions can continue to go on.
			if currID == firstConnID || seCnt == finishedCnt {
				break
			}
			// Print log to notify if TestParallelDDLBeforeRunDDLJob hang up
			log.Info("sleep in TestParallelDDLBeforeRunDDLJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		return info
	}
	d := s.dom.DDL()
	d.(ddl.DDLForTest).SetInterceptor(intercept)

	// Make sure the connection 1 executes a SQL before the connection 2.
	// And the connection 2 executes a SQL with an outdated information schema.
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		se.SetConnectionID(firstConnID)
		_, err1 := se.Execute(context.Background(), "alter table test_table drop column c2")
		c.Assert(err1, IsNil)
		atomic.StoreInt32(&sessionCnt, finishedCnt)
	}()
	go func() {
		defer wg.Done()

		se1.SetConnectionID(2)
		_, err2 := se1.Execute(context.Background(), "alter table test_table add column c2 int")
		c.Assert(err2, NotNil)
		c.Assert(strings.Contains(err2.Error(), "Information schema is changed"), IsTrue)
	}()

	wg.Wait()

	intercept = &ddl.TestInterceptor{}
	d.(ddl.DDLForTest).SetInterceptor(intercept)
}
