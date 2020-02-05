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

package executor_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

type testBypassSuite struct{}

func (s *testBypassSuite) SetUpSuite(c *C) {
}

func (s *testSuite4) TestInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errInsertSelectSQL := `insert insert_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSetSQL := `insert insert_test set c1 = 3;`
	tk.MustExec(insertSetSQL)

	errInsertSelectSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSelectSQL := `create table insert_test_1 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test;`
	tk.MustExec(insertSelectSQL)

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_1 values(default, default, default, default, default)`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec("create table insert_err (id int, c1 varchar(8))")
	_, err = tk.Exec("insert insert_err values (1, 'abcdabcdabcd')")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	_, err = tk.Exec("insert insert_err values (1, '你好，世界')")
	c.Assert(err, IsNil)

	tk.MustExec("create table t (id int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t t1 set id = (select count(*) + 1 from t t2 where t1.id = t2.id)")
	r := tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c binary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("255"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c varbinary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("1"))

	// issue 3832
	tk.MustExec("create table t1 (b char(0));")
	_, err = tk.Exec(`insert into t1 values ("");`)
	c.Assert(err, IsNil)

	// test auto_increment with unsigned.
	tk.MustExec("drop table if exists test")
	tk.MustExec("CREATE TABLE test(id int(10) UNSIGNED NOT NULL AUTO_INCREMENT, p int(10) UNSIGNED NOT NULL, PRIMARY KEY(p), KEY(id))")
	tk.MustExec("insert into test(p) value(1)")
	tk.MustQuery("select * from test").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from test use index (id) where id = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into test values(NULL, 2)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2"))
	tk.MustExec("insert into test values(2, 3)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2", "2 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create table t(a float unsigned, b double unsigned)")
	tk.MustExec("insert into t value(-1.1, -1.1), (-2.1, -2.1), (0, 0), (1.1, 1.1)")
	tk.MustQuery("show warnings").
		Check(testkit.Rows("Warning 1690 constant -1.1 overflows float", "Warning 1690 constant -1.1 overflows double",
			"Warning 1690 constant -2.1 overflows float", "Warning 1690 constant -2.1 overflows double"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0 0", "0 0", "0 0", "1.1 1.1"))

	// issue 7061
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 1, b int default 2)")
	tk.MustExec("insert into t values(default, default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(default(b), default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (b) values(default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (b) values(default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func (s *testSuite4) TestInsertAutoInc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)

	insertSQL := `insert into insert_autoinc_test(c1) values (1), (2)`
	tk.MustExec(insertSQL)
	tk.MustExec("begin")
	r := tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 := fmt.Sprintf("%v %v", "1", "1")
	rowStr2 := fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (5,5)`
	tk.MustExec(insertSQL)
	insertSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 := fmt.Sprintf("%v %v", "5", "5")
	rowStr4 := fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (3,3)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 := fmt.Sprintf("%v %v", "3", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(c1) values (7)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 := fmt.Sprintf("%v %v", "7", "7")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4, rowStr6))
	tk.MustExec("commit")

	// issue-962
	createSQL = `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0.3, 1)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "1", "1")
	r.Check(testkit.Rows(rowStr1))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-0.3, 2)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-3.3, 3)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "-3", "3")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (4.3, 4)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "4", "4")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4))
	insertSQL = `insert into insert_autoinc_test(c1) values (5)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "5", "5")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 6)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5, rowStr6))

	// SQL_MODE=NO_AUTO_VALUE_ON_ZERO
	createSQL = `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)
	insertSQL = `insert into insert_autoinc_test(id, c1) values (5, 1)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "5", "1")
	r.Check(testkit.Rows(rowStr1))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 2)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "6", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 3)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "7", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("set SQL_MODE=NO_AUTO_VALUE_ON_ZERO")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 4)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "0", "4")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 5)`
	_, err := tk.Exec(insertSQL)
	// ERROR 1062 (23000): Duplicate entry '0' for key 'PRIMARY'
	c.Assert(err, NotNil)
	insertSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "8", "6")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 7)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "9", "7")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6))
	tk.MustExec("set SQL_MODE='';")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 8)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr7 := fmt.Sprintf("%v %v", "10", "8")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 9)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr8 := fmt.Sprintf("%v %v", "11", "9")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7, rowStr8))
}

func (s *testSuite4) TestInsertSetWithDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Assign `DEFAULT` in `INSERT ... SET ...` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int default 10, b int default 20);")
	tk.MustExec("insert into t1 set a=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default, a=1;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("20 10"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b)+default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("30 20"))
}

func (s *testSuite4) TestReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errReplaceSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSetSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetSQL)

	errReplaceSetSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test;`
	tk.MustExec(replaceSelectSQL)

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	// For Issue989
	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))

	// For Issue1012
	issue1012SQL := `CREATE TABLE tIssue1012 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (1, 2);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (2, 1);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `replace into tIssue1012(a, b) values (1, 1);`
	tk.MustExec(issue1012SQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(3))

	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))

	// Test Replace with info message
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 values(1,1);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	tk.MustExec(`replace into t1 values(1,1),(2,2);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.MustExec(`replace into t1 values(4,14),(5,15),(6,16),(7,17),(8,18)`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(7))
	tk.MustExec(`replace into t1 select * from (select 1, 2) as tmp;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	// Assign `DEFAULT` in `REPLACE` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int primary key, b int default 20, c int default 30);")
	tk.MustExec("insert into t1 value (1, 2, 3);")
	tk.MustExec("replace t1 set a=1, b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("replace t1 set a=2, b=default, c=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 20 30"))
	tk.MustExec("replace t1 set a=2, b=default(c), c=default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20"))
	tk.MustExec("replace t1 set a=default(b)+default(c)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20", "50 20 30"))
}

func (s *testSuite8) TestUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	tk.MustExec("insert into update_test(name) values ('bb')")
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("8 aa", "9 bb"))
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), index(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	_, err := tk.Exec("update update_test set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "[table:1048]Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
	tk.MustExec("create table update_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into update_test(id) values (1)")
	tk.MustExec("update update_test set id = 2 where id = 1 limit 1")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")

	// Test that in a transaction, when a constraint failed in an update statement, the record is not inserted.
	tk.MustExec("create table update_unique (id int primary key, name int unique)")
	tk.MustExec("insert update_unique values (1, 1), (2, 2);")
	tk.MustExec("begin")
	_, err = tk.Exec("update update_unique set name = 1 where id = 2")
	c.Assert(err, NotNil)
	tk.MustExec("commit")
	tk.MustQuery("select * from update_unique").Check(testkit.Rows("1 1", "2 2"))

	// for issue #5132
	tk.MustExec("CREATE TABLE `tt1` (" +
		"`a` int(11) NOT NULL," +
		"`b` varchar(32) DEFAULT NULL," +
		"`c` varchar(32) DEFAULT NULL," +
		"PRIMARY KEY (`a`)," +
		"UNIQUE KEY `b_idx` (`b`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("insert into tt1 values(1, 'a', 'a');")
	tk.MustExec("insert into tt1 values(2, 'd', 'b');")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "2 d b"))
	tk.MustExec("update tt1 set a=5 where c='b';")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "5 d b"))

	// issue 7237, update subquery table should be forbidden
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (k int, v int)")
	_, err = tk.Exec("update t, (select * from t) as b set b.k = t.k")
	c.Assert(err.Error(), Equals, "[planner:1288]The target table b of the UPDATE is not updatable")
	tk.MustExec("update t, (select * from t) as b set t.k = b.k")

	// issue 8119
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 float(1,1));")
	tk.MustExec("insert into t values (0.0);")
	_, err = tk.Exec("update t set c1 = 2.0;")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, e int, index idx(a))")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("update t1 join t2 on t1.a=t2.a set t1.a=1 where t2.b=1 and t2.c=2")

	// Assign `DEFAULT` in `UPDATE` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int default 1, b int default 2);")
	tk.MustExec("insert into t1 values (10, 10), (20, 20);")
	tk.MustExec("update t1 set a=default where b=10;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "20 20"))
	tk.MustExec("update t1 set a=30, b=default where a=20;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "30 2"))
	tk.MustExec("update t1 set a=default, b=default where a=30;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "1 2"))
	tk.MustExec("insert into t1 values (40, 40)")
	tk.MustExec("update t1 set a=default, b=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2", "1 2", "1 2"))
	tk.MustExec("update t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 1", "2 1", "2 1"))
}

func (s *testSuite4) fillMultiTableForUpdate(tk *testkit.TestKit) {
	// Create and fill table items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill table month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func (s *testSuite4) TestMultipleTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillMultiTableForUpdate(tk)

	tk.MustExec(`UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// Single-table syntax but with multiple tables
	tk.MustExec(`UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 11", "12 items_price_12", "13 13"))
	tk.MustExec("commit")

	// JoinTable with alias table name.
	tk.MustExec(`UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// fix https://github.com/pingcap/tidb/issues/369
	testSQL := `
		DROP TABLE IF EXISTS t1, t2;
		create table t1 (c int);
		create table t2 (c varchar(256));
		insert into t1 values (1), (2);
		insert into t2 values ("a"), ("b");
		update t1, t2 set t1.c = 10, t2.c = "abc";`
	tk.MustExec(testSQL)

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testSQL)

	r = tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))

	// test https://github.com/pingcap/tidb/issues/3604
	tk.MustExec("drop table if exists t, t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.MustExec("update t m, t n set m.a = m.a + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2", "4 3"))
	tk.MustExec("update t m, t n set n.a = n.a - 1, n.b = n.b + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
}

func (s *testSuite) TestDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillData(tk, "delete_test")

	tk.MustExec(`update delete_test set name = "abc" where id = 2;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec(`delete from delete_test where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from delete_test where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into delete_test values (2, 'abc')")
	tk.MustExec(`delete from delete_test where delete_test.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Select data
	tk.MustExec("begin")
	rows := tk.MustQuery(`SELECT * from delete_test limit 2;`)
	rows.Check(testkit.Rows("1 hello"))
	tk.MustExec("commit")
}

func (s *testSuite4) TestNotNullDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists t1,t2;")
	defer tk.MustExec("drop table t1,t2")
	tk.MustExec("create table t1 (a int not null default null default 1);")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table  t2 change column a a int not null default null default 1;")
}

// TestIssue4067 Test issue https://github.com/pingcap/tidb/issues/4067
func (s *testSuite7) TestIssue4067(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	tk.MustExec("insert into t1 values(123)")
	tk.MustExec("insert into t2 values(123)")
	tk.MustExec("delete from t1 where id not in (select id from t2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("123"))
	tk.MustExec("delete from t1 where id in (select id from t2)")
	tk.MustQuery("select * from t1").Check(nil)
}

func (s *testSuite7) TestDataTooLongErrMsg(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(2));")
	_, err := tk.Exec("insert into t values('123');")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for column 'a' at row 1")
	tk.MustExec("insert into t values('12')")
	_, err = tk.Exec("update t set a = '123' where a = '12';")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for column 'a' at row 1")
}

func (s *testSuite7) TestUpdateSelect(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table msg (id varchar(8), b int, status int, primary key (id, b))")
	tk.MustExec("insert msg values ('abc', 1, 1)")
	tk.MustExec("create table detail (id varchar(8), start varchar(8), status int, index idx_start(start))")
	tk.MustExec("insert detail values ('abc', '123', 2)")
	tk.MustExec("UPDATE msg SET msg.status = (SELECT detail.status FROM detail WHERE msg.id = detail.id)")
}

func (s *testSuite7) TestUpdateDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ttt (id bigint(20) NOT NULL, host varchar(30) NOT NULL, PRIMARY KEY (id), UNIQUE KEY i_host (host));")
	tk.MustExec("insert into ttt values (8,8),(9,9);")

	tk.MustExec("begin")
	tk.MustExec("update ttt set id = 0, host='9' where id = 9 limit 1;")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows("8 8"))
	tk.MustExec("update ttt set id = 0, host='8' where id = 8 limit 1;")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows())
	tk.MustExec("commit")

	tk.MustExec("drop table ttt")
}

func (s *testSuite7) TestUpdateAffectRowCnt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table a(id int auto_increment, a int default null, primary key(id))")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set id = id*10 where a = 1001")
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))

	tk.MustExec("drop table a")
	tk.MustExec("create table a ( a bigint, b bigint)")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set a = a*10 where b = 1001")
	ctx = tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))
}

func (s *testSuite7) TestReplaceLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table testLog (a int not null primary key, b int unique key);`)

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("testLog")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("b")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(1), 1)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = tk.Exec(`replace into testLog values (0, 0), (1, 1);`)
	c.Assert(err, NotNil)
	expErr := errors.New(`can not be duplicated row, due to old row not found. handle 1 not found`)
	c.Assert(expErr.Error() == err.Error(), IsTrue, Commentf("obtained error: (%s)\nexpected error: (%s)", err.Error(), expErr.Error()))
}

func (s *testSuite7) TestIssue11059(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (pk int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (2, 11, 215)")
	tk.MustExec("insert into t values (3, 7, 2111)")
	_, err := tk.Exec("update t set pk = 2 where uk = 7")
	c.Assert(err, NotNil)
}
