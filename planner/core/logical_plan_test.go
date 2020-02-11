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

package core

import (
	"context"
	"fmt"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPlanSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	testData testutil.TestData
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockUnsignedTable()})
	s.ctx = MockContext()
	s.Parser = parser.New()

	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "plan_suite_unexported")
	c.Assert(err, IsNil)
}

func (s *testPlanSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for ith, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[ith], Commentf("for %s %d", ca, ith))
	}
}

func (s *testPlanSuite) TestJoinPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		join, ok := proj.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		c.Assert(leftCond, Equals, output[i].Left, comment)
		c.Assert(rightCond, Equals, output[i].Right, comment)
	}
}

func (s *testPlanSuite) TestOuterWherePredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Sel   string
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		selection, ok := proj.children[0].(*LogicalSelection)
		c.Assert(ok, IsTrue, comment)
		selCond := fmt.Sprintf("%s", selection.Conditions)
		s.testData.OnRecord(func() {
			output[i].Sel = selCond
		})
		c.Assert(selCond, Equals, output[i].Sel, comment)
		join, ok := selection.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		c.Assert(leftCond, Equals, output[i].Left, comment)
		c.Assert(rightCond, Equals, output[i].Right, comment)
	}
}

func (s *testPlanSuite) TestSimplifyOuterJoin(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Best     string
			JoinType string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i].Best = planString
		})
		c.Assert(planString, Equals, output[i].Best, comment)
		join, ok := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		if !ok {
			join, ok = p.(LogicalPlan).Children()[0].Children()[0].(*LogicalJoin)
			c.Assert(ok, IsTrue, comment)
		}
		s.testData.OnRecord(func() {
			output[i].JoinType = join.JoinType.String()
		})
		c.Assert(join.JoinType.String(), Equals, output[i].JoinType, comment)
	}
}

func (s *testPlanSuite) TestDeriveNotNullConds(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Plan  string
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].Plan = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i].Plan, comment)
		join := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		left := join.Children()[0].(*DataSource)
		right := join.Children()[1].(*DataSource)
		leftConds := fmt.Sprintf("%s", left.pushedDownConds)
		rightConds := fmt.Sprintf("%s", right.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftConds, rightConds
		})
		c.Assert(leftConds, Equals, output[i].Left, comment)
		c.Assert(rightConds, Equals, output[i].Right, comment)
	}
}

func buildLogicPlan4GroupBy(s *testPlanSuite, c *C, sql string) (Plan, error) {
	sqlMode := s.ctx.GetSessionVars().SQLMode
	mockedTableInfo := MockSignedTable()
	// mock the table info here for later use
	// enable only full group by
	s.ctx.GetSessionVars().SQLMode = sqlMode | mysql.ModeOnlyFullGroupBy
	defer func() { s.ctx.GetSessionVars().SQLMode = sqlMode }() // restore it
	comment := Commentf("for %s", sql)
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil, comment)

	stmt.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).TableInfo = mockedTableInfo

	p, _, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	return p, err
}

func (s *testPlanSuite) TestGroupByWhenNotExistCols(c *C) {
	sqlTests := []struct {
		sql              string
		expectedErrMatch string
	}{
		{
			sql:              "select a from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has an as column alias
			sql:              "select a as tempField from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has as table alias
			sql:              "select tempTable.a from t as tempTable group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.tempTable\\.a'.*",
		},
		{
			// has a func call
			sql:              "select length(a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(b + a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) as tempField from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
	}
	for _, test := range sqlTests {
		sql := test.sql
		p, err := buildLogicPlan4GroupBy(s, c, sql)
		c.Assert(err, NotNil)
		c.Assert(p, IsNil)
		c.Assert(err, ErrorMatches, test.expectedErrMatch)
	}
}

func (s *testPlanSuite) TestPlanBuilder(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)

		s.ctx.GetSessionVars().HashJoinConcurrency = 1
		Preprocess(s.ctx, stmt, s.is)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagPrunColumns, lp)
			c.Assert(err, IsNil)
		}
		s.testData.OnRecord(func() {
			output[i] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i], Commentf("for %s", ca))
	}
}

func (s *testPlanSuite) TestJoinReOrder(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestEagerAggregation(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	s.ctx.GetSessionVars().AllowAggPushDown = true
	for ith, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPredicatePushDown|flagPrunColumns|flagPushDownAgg, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[ith], Commentf("for %s %d", tt, ith))
	}
	s.ctx.GetSessionVars().AllowAggPushDown = false
}

func (s *testPlanSuite) TestColumnPruning(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []map[int][]string
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(ctx, flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i] = make(map[int][]string)
		})
		s.checkDataSourceCols(lp, c, output[i], comment)
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	ctx := MockContext()
	pA := DataSource{}.Init(ctx)
	pB := DataSource{}.Init(ctx)
	c.Assert(pA.id+1, Equals, pB.id)
}

func (s *testPlanSuite) checkDataSourceCols(p LogicalPlan, c *C, ans map[int][]string, comment CommentInterface) {
	switch p.(type) {
	case *DataSource:
		s.testData.OnRecord(func() {
			ans[p.ID()] = make([]string, p.Schema().Len())
		})
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, Commentf("For %v DataSource ID %d Not found", comment, p.ID()))
		c.Assert(len(p.Schema().Columns), Equals, len(colList), comment)
		for i, col := range p.Schema().Columns {
			s.testData.OnRecord(func() {
				colList[i] = col.String()
			})
			c.Assert(col.String(), Equals, colList[i], comment)
		}
	}
	for _, child := range p.Children() {
		s.checkDataSourceCols(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestValidate(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		err *terror.Error
	}{
		{
			sql: "select date_format((1,2), '%H');",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) between (3,4) and (5,6)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) in ((3,4),(5,6))",
			err: nil,
		},
		{
			sql: "select (1,2) in ((3,4),5)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) is null",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (+(1,2))=(1,2)",
			err: nil,
		},
		{
			sql: "select (-(1,2))=(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2)||(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) < (3,4)",
			err: nil,
		},
		{
			sql: "select (1,2) < 3",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select 1, * from t",
			err: ErrInvalidWildCard,
		},
		{
			sql: "select *, 1 from t",
			err: nil,
		},
		{
			sql: "select 1, t.* from t",
			err: nil,
		},
		{
			sql: "insert into t set a = 1, b = a + 1",
			err: nil,
		},
		{
			sql: "insert into t set a = 1, b = values(a) + 1",
			err: nil,
		},
		{
			sql: "select a as c1, b as c1 from t order by c1",
			err: ErrAmbiguous,
		},
		{
			sql: "select * from t t1 use index(e)",
			err: ErrKeyDoesNotExist,
		},
		{
			sql: "select a from t having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t group by c2 + 1 having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a as b, b from t having b",
			err: ErrAmbiguous,
		},
		{
			sql: "select a + 1 from t having a",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t having sum(avg(a))",
			err: ErrInvalidGroupFuncUse,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		sql := tt.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		_, _, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		if tt.err == nil {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(tt.err.Equal(err), IsTrue, comment)
		}
	}
}

func (s *testPlanSuite) checkUniqueKeys(p LogicalPlan, c *C, ans map[int][][]string, sql string) {
	s.testData.OnRecord(func() {
		ans[p.ID()] = make([][]string, len(p.Schema().Keys))
	})
	keyList, ok := ans[p.ID()]
	c.Assert(ok, IsTrue, Commentf("for %s, %v not found", sql, p.ID()))
	c.Assert(len(p.Schema().Keys), Equals, len(keyList), Commentf("for %s, %v, the number of key doesn't match, the schema is %s", sql, p.ID(), p.Schema()))
	for i := range keyList {
		s.testData.OnRecord(func() {
			keyList[i] = make([]string, len(p.Schema().Keys[i]))
		})
		c.Assert(len(p.Schema().Keys[i]), Equals, len(keyList[i]), Commentf("for %s, %v %v, the number of column doesn't match", sql, p.ID(), keyList[i]))
		for j := range keyList[i] {
			s.testData.OnRecord(func() {
				keyList[i][j] = p.Schema().Keys[i][j].String()
			})
			c.Assert(p.Schema().Keys[i][j].String(), Equals, keyList[i][j], Commentf("for %s, %v %v, column dosen't match", sql, p.ID(), keyList[i]))
		}
	}
	s.testData.OnRecord(func() {
		ans[p.ID()] = keyList
	})
	for _, child := range p.Children() {
		s.checkUniqueKeys(child, c, ans, sql)
	}
}

func (s *testPlanSuite) TestUniqueKeyInfo(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []map[int][][]string
	s.testData.GetTestCases(c, &input, &output)
	s.testData.OnRecord(func() {
		output = make([]map[int][][]string, len(input))
	})

	ctx := context.Background()
	for ith, tt := range input {
		comment := Commentf("for %s %d", tt, ith)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = make(map[int][][]string)
		})
		s.checkUniqueKeys(lp, c, output[ith], tt)
	}
}

func (s *testPlanSuite) TestAggPrune(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo|flagEliminateAgg|flagEliminateProjection, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], comment)
	}
}

func (s *testPlanSuite) TestTopNPushDown(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is)
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i], comment)
	}
}

func (s *testPlanSuite) TestNameResolver(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		err string
	}{
		{"select a from t", ""},
		{"select c3 from t", "[planner:1054]Unknown column 'c3' in 'field list'"},
		{"select c1 from t4", "[schema:1146]Table 'test.t4' doesn't exist"},
		{"select * from t", ""},
		{"select t.* from t", ""},
		{"select t2.* from t", "[planner:1051]Unknown table 't2'"},
		{"select b as a, c as a from t group by a", "[planner:1052]Column 'c' in field list is ambiguous"},
		{"select 1 as a, b as a, c as a from t group by a", ""},
		{"select a, b as a from t group by a+1", ""},
		{"select c, a as c from t order by c+1", ""},
		{"select * from t as t1, t as t2 join t as t3 on t2.a = t3.a", ""},
		{"select * from t as t1, t as t2 join t as t3 on t1.c1 = t2.a", "[planner:1054]Unknown column 't1.c1' in 'on clause'"},
		{"select a from t group by a having a = 3", ""},
		{"select a from t group by a having c2 = 3", "[planner:1054]Unknown column 'c2' in 'having clause'"},
		{"select a from t where t11.a < t.a", "[planner:1054]Unknown column 't11.a' in 'where clause'"},
		{"select a from t having t11.c1 < t.a", "[planner:1054]Unknown column 't11.c1' in 'having clause'"},
		{"select a from t where t.a < t.a order by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'order clause'"},
		{"select a from t group by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'group statement'"},
		{"select '' as fakeCol from t group by values(fakeCol)", "[planner:1054]Unknown column '' in 'VALUES() function'"},
	}

	ctx := context.Background()
	for _, t := range tests {
		comment := Commentf("for %s", t.sql)
		stmt, err := s.ParseOneStmt(t.sql, "", "")
		c.Assert(err, IsNil, comment)
		s.ctx.GetSessionVars().HashJoinConcurrency = 1

		_, _, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		if t.err == "" {
			c.Check(err, IsNil)
		} else {
			c.Assert(err.Error(), Equals, t.err)
		}
	}
}

func (s *testPlanSuite) TestOuterJoinEliminator(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v sql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is)
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], comment)
	}
}

func byItemsToProperty(byItems []*ByItems) *property.PhysicalProperty {
	pp := &property.PhysicalProperty{}
	for _, item := range byItems {
		pp.Items = append(pp.Items, property.Item{Col: item.Expr.(*expression.Column), Desc: item.Desc})
	}
	return pp
}

func pathsName(paths []*candidatePath) string {
	var names []string
	for _, path := range paths {
		if path.path.IsTablePath {
			names = append(names, "PRIMARY_KEY")
		} else {
			names = append(names, path.path.Index.Name.O)
		}
	}
	return strings.Join(names, ",")
}

func (s *testPlanSuite) TestSkylinePruning(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql:    "select * from t",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where a > 1",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t where a > 1 order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select f from t where f > 1",
			result: "f,f_g",
		},
		{
			sql:    "select f from t where f > 1 order by a",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1 and g > 1",
			result: "PRIMARY_KEY,f,g,f_g",
		},
		{
			sql:    "select count(1) from t",
			result: "PRIMARY_KEY,c_d_e,f,g,f_g,c_d_e_str,e_d_c_str_prefix",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is)
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		lp := p.(LogicalPlan)
		_, err = lp.recursiveDeriveStats()
		c.Assert(err, IsNil, comment)
		var ds *DataSource
		var byItems []*ByItems
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			case *LogicalSort:
				byItems = v.ByItems
				lp = lp.Children()[0]
			case *LogicalProjection:
				newItems := make([]*ByItems, 0, len(byItems))
				for _, col := range byItems {
					idx := v.schema.ColumnIndex(col.Expr.(*expression.Column))
					switch expr := v.Exprs[idx].(type) {
					case *expression.Column:
						newItems = append(newItems, &ByItems{Expr: expr, Desc: col.Desc})
					}
				}
				byItems = newItems
				lp = lp.Children()[0]
			default:
				lp = lp.Children()[0]
			}
		}
		paths := ds.skylinePruning(byItemsToProperty(byItems))
		c.Assert(pathsName(paths), Equals, tt.result, comment)
	}
}

func (s *testPlanSuite) TestUpdateEQCond(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select t1.a from t t1, t t2 where t1.a = t2.a+1",
			best: "Join{DataScan(t1)->DataScan(t2)->Projection}(test.t.a,Column#25)->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is)
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}
