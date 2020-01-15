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

func (s *testSuiteAgg) TestAggEliminator(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustQuery("select min(a), min(a) from t").Check(testkit.Rows("<nil> <nil>"))
	tk.MustExec("insert into t values(1, -1), (2, -2), (3, 1), (4, NULL)")
	tk.MustQuery("select max(a) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b) from t").Check(testkit.Rows("-2"))
	tk.MustQuery("select max(b*b) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b*b) from t").Check(testkit.Rows("1"))
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
