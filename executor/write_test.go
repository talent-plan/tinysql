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

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c binary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r := tk.MustQuery("select length(c) from t;")
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

func (s *testSuite) TestDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillData(tk, "delete_test")

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
