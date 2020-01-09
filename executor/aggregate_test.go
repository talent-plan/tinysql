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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

type testSuiteAgg struct {
	*baseTestSuite
	testData testutil.TestData
}

func (s *testSuiteAgg) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "agg_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteAgg) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testSuiteAgg) TestAggPrune(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b varchar(50), c int)")
	tk.MustExec("insert into t values(1, '1ff', NULL), (2, '234.02', 1)")
	tk.MustQuery("select id, sum(b) from t group by id").Check(testkit.Rows("1 1", "2 234.02"))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("235.02"))
	tk.MustQuery("select id, count(c) from t group by id").Check(testkit.Rows("1 0", "2 1"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b float, c float)")
	tk.MustExec("insert into t values(1, 1, 3), (2, 1, 6)")
	tk.MustQuery("select sum(b/c) from t group by id").Check(testkit.Rows("0.3333333333333333", "0.16666666666666666"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b float, c float, d float)")
	tk.MustExec("insert into t values(1, 1, 3, NULL), (2, 1, NULL, 6), (3, NULL, 1, 2), (4, NULL, NULL, 1), (5, NULL, 2, NULL), (6, 3, NULL, NULL), (7, NULL, NULL, NULL), (8, 1, 2 ,3)")
	tk.MustQuery("select count(distinct b, c, d) from t group by id").Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(10))")
	tk.MustExec("insert into t value(1, 11),(3, NULL)")
	tk.MustQuery("SELECT a, MIN(b), MAX(b) FROM t GROUP BY a").Check(testkit.Rows("1 11 11", "3 <nil> <nil>"))
}

func (s *testSuiteAgg) TestGroupConcatAggr(c *C) {
	// issue #5411
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table test(id int, name int)")
	tk.MustExec("insert into test values(1, 10);")
	tk.MustExec("insert into test values(1, 20);")
	tk.MustExec("insert into test values(1, 30);")
	tk.MustExec("insert into test values(2, 20);")
	tk.MustExec("insert into test values(3, 200);")
	tk.MustExec("insert into test values(3, 500);")
	result := tk.MustQuery("select id, group_concat(name) from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ';') from test group by id order by id")
	result.Check(testkit.Rows("1 10;20;30", "2 20", "3 200;500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ',') from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery(`select id, group_concat(name SEPARATOR '%') from test group by id order by id`)
	result.Check(testkit.Rows("1 10%20%30", "2 20", `3 200%500`))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '') from test group by id order by id")
	result.Check(testkit.Rows("1 102030", "2 20", "3 200500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '123') from test group by id order by id")
	result.Check(testkit.Rows("1 101232012330", "2 20", "3 200123500"))

	// issue #9920
	tk.MustQuery("select group_concat(123, null)").Check(testkit.Rows("<nil>"))
}

func (s *testSuiteAgg) TestSelectDistinct(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_distinct_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select distinct name from select_distinct_test;")
	r.Check(testkit.Rows("hello"))
	tk.MustExec("commit")

}

func (s *testSuiteAgg) TestAggPushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("alter table t add index idx(a, b, c)")
	// test for empty table
	tk.MustQuery("select count(a) from t group by a;").Check(testkit.Rows())
	tk.MustQuery("select count(a) from t;").Check(testkit.Rows("0"))
	// test for one row
	tk.MustExec("insert t values(0,0,0)")
	tk.MustQuery("select distinct b from t").Check(testkit.Rows("0"))
	tk.MustQuery("select count(b) from t group by a;").Check(testkit.Rows("1"))
	// test for rows
	tk.MustExec("insert t values(1,1,1),(3,3,6),(3,2,5),(2,1,4),(1,1,3),(1,1,2);")
	tk.MustQuery("select count(a) from t where b>0 group by a, b;").Sort().Check(testkit.Rows("1", "1", "1", "3"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a;").Check(testkit.Rows("3", "1", "1", "1"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a limit 1;").Check(testkit.Rows("3"))

	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("create table tt(a int primary key, b int, c int)")
	tk.MustExec("insert into t values(1, 1, 1), (2, 1, 1)")
	tk.MustExec("insert into tt values(1, 2, 1)")
	tk.MustQuery("select max(a.b), max(b.b) from t a join tt b on a.a = b.a group by a.c").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, count(b) from (select * from t union all select * from tt) k group by a order by a").Check(testkit.Rows("1 2", "2 1"))
}

func (s *testSuiteAgg) TestIssue13652(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a real)")
	tk.MustQuery("select a from t group by (a)")
	tk.MustQuery("select a from t group by ((a))")
	tk.MustQuery("select a from t group by +a")
	tk.MustQuery("select a from t group by ((+a))")
	_, err := tk.Exec("select a from t group by (-a)")
	c.Assert(err.Error(), Equals, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
}

func (s *testSuiteAgg) TestHaving(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert into t values (1,2,3), (2, 3, 1), (3, 1, 2)")

	tk.MustQuery("select c1 as c2, c3 from t having c2 = 2").Check(testkit.Rows("2 1"))
	tk.MustQuery("select c1 as c2, c3 from t group by c2 having c2 = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c2 having sum(c2) = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c3 having sum(c2) = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c3 having sum(0) + c2 = 2;").Check(testkit.Rows("2 1"))
	tk.MustQuery("select c1 as a from t having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select t.c1 from t having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select a.c1 from t as a having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select c1 as a from t group by c3 having sum(a) = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select c1 as a from t group by c3 having sum(a) + a = 2;").Check(testkit.Rows("1"))
	tk.MustQuery("select a.c1 as c, a.c1 as d from t as a, t as b having c1 = 1 limit 1;").Check(testkit.Rows("1 1"))

	tk.MustQuery("select sum(c1) as s from t group by c1 having sum(c1) order by s").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select sum(c1) - 1 as s from t group by c1 having sum(c1) - 1 order by s").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select 1 from t group by c1 having sum(abs(c2 + c3)) = c1").Check(testkit.Rows("1"))
}

func (s *testSuiteAgg) TestAggEliminator(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustQuery("select min(a), min(a) from t").Check(testkit.Rows("<nil> <nil>"))
	tk.MustExec("insert into t values(1, -1), (2, -2), (3, 1), (4, NULL)")
	tk.MustQuery("select max(a) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b) from t").Check(testkit.Rows("-2"))
	tk.MustQuery("select max(b*b) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b*b) from t").Check(testkit.Rows("1"))
	tk.MustQuery("select group_concat(b, b) from t group by a").Sort().Check(testkit.Rows("-1-1", "-2-2", "11", "<nil>"))
}

func (s *testSuiteAgg) TestMaxMinFloatScalaFunc(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec(`DROP TABLE IF EXISTS T;`)
	tk.MustExec(`CREATE TABLE T(A VARCHAR(10), B VARCHAR(10), C FLOAT);`)
	tk.MustExec(`INSERT INTO T VALUES('0', "val_b", 12.191);`)
	tk.MustQuery(`SELECT MAX(CASE B WHEN 'val_b'  THEN C ELSE 0 END) val_b FROM T WHERE cast(A as signed) = 0 GROUP BY a;`).Check(testkit.Rows("12.190999984741211"))
	tk.MustQuery(`SELECT MIN(CASE B WHEN 'val_b'  THEN C ELSE 0 END) val_b FROM T WHERE cast(A as signed) = 0 GROUP BY a;`).Check(testkit.Rows("12.190999984741211"))
}

func (s *testSuiteAgg) TestBuildProjBelowAgg(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	rs := tk.MustQuery("select i+1 as a, count(i+2), sum(i+3), group_concat(i+4), bit_or(i+5) from t group by i, hex(i+6) order by a")
	rs.Check(testkit.Rows(
		"2 3 12 5,5,5 6",
		"3 3 15 6,6,6 7",
		"4 3 18 7,7,7 8"))
}

func (s *testSuiteAgg) TestInjectProjBelowTopN(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testSuiteAgg) TestFirstRowEnum(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a enum('a', 'b'));`)
	tk.MustExec(`insert into t values('a');`)
	tk.MustQuery(`select a from t group by a;`).Check(testkit.Rows(
		`a`,
	))
}

func (s *testSuiteAgg) TestAggJSON(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a datetime, b json, index idx(a));`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:00', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:01', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:02', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:03', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:04', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:05', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:06', '"hello"');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:07', '"hello"');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:08', '"hello"');`)
	tk.MustExec(`set @@sql_mode='';`)
	tk.MustQuery(`select b from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
	tk.MustQuery(`select min(b) from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
	tk.MustQuery(`select max(b) from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
}

func (s *testSuiteAgg) TestIssue10099(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b char(10))")
	tk.MustExec("insert into t values('1', '222'), ('12', '22')")
	tk.MustQuery("select count(distinct a, b) from t").Check(testkit.Rows("2"))
}

func (s *testSuiteAgg) TestIssue10098(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("create table t(a char(10), b char(10))")
	tk.MustExec("insert into t values('1', '222'), ('12', '22')")
	tk.MustQuery("select group_concat(distinct a, b) from t").Check(testkit.Rows("1222,1222"))
}

func (s *testSuiteAgg) TestIssue10608(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t, s;`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into s values(100292, 508931), (120002, 508932)")
	tk.MustExec("insert into t values(508931), (508932)")
	tk.MustQuery("select (select  /*+ stream_agg() */ group_concat(concat(123,'-')) from t where t.a = s.b group by t.a) as t from s;").Check(testkit.Rows("123-", "123-"))
	tk.MustQuery("select (select  /*+ hash_agg() */ group_concat(concat(123,'-')) from t where t.a = s.b group by t.a) as t from s;").Check(testkit.Rows("123-", "123-"))

}
