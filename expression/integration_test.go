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

package expression_test

import (
	"bytes"
	"context"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"sort"
	"strconv"
)

var _ = Suite(&testIntegrationSuite{})
var _ = Suite(&testIntegrationSuite2{})

type testIntegrationSuite struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

type testIntegrationSuite2 struct {
	testIntegrationSuite
}

func (s *testIntegrationSuite) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testIntegrationSuite) TestInPredicate4UnsignedInt(c *C) {
	// for issue #6661
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a bigint unsigned,key (a));")
	tk.MustExec("INSERT INTO t VALUES (0), (4), (5), (6), (7), (8), (9223372036854775810), (18446744073709551614), (18446744073709551615);")
	r := tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 18446744073709551615);`)
	r.Check(testkit.Rows("0", "4", "5", "6", "7", "8", "9223372036854775810", "18446744073709551614"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 4, 9223372036854775810);`)
	r.Check(testkit.Rows("0", "5", "6", "7", "8", "18446744073709551614", "18446744073709551615"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 0, 4, 18446744073709551614);`)
	r.Check(testkit.Rows("5", "6", "7", "8", "9223372036854775810", "18446744073709551615"))

	// for issue #4473
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1 (some_id smallint(5) unsigned,key (some_id) )")
	tk.MustExec("insert into t1 values (1),(2)")
	r = tk.MustQuery(`select some_id from t1 where some_id not in(2,-1);`)
	r.Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestFilterExtractFromDNF(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")

	tests := []struct {
		exprStr string
		result  string
	}{
		{
			exprStr: "a = 1 or a = 1 or a = 1",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "a = 1 or a = 1 or (a = 1 and b = 1)",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "(a = 1 and a = 1) or a = 1 or b = 1",
			result:  "[or(or(and(eq(test.t.a, 1), eq(test.t.a, 1)), eq(test.t.a, 1)), eq(test.t.b, 1))]",
		},
		{
			exprStr: "(a = 1 and b = 2) or (a = 1 and b = 3) or (a = 1 and b = 4)",
			result:  "[eq(test.t.a, 1) or(eq(test.t.b, 2), or(eq(test.t.b, 3), eq(test.t.b, 4)))]",
		},
		{
			exprStr: "(a = 1 and b = 1 and c = 1) or (a = 1 and b = 1) or (a = 1 and b = 1 and c > 2 and c < 3)",
			result:  "[eq(test.t.a, 1) eq(test.t.b, 1)]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		sctx := tk.Se.(sessionctx.Context)
		sc := sctx.GetSessionVars().StmtCtx
		stmts, err := session.Parse(sctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(sctx).InfoSchema()
		err = plannercore.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		conds := make([]expression.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = expression.PushDownNot(sctx, cond)
		}
		afterFunc := expression.ExtractFiltersFromDNFs(sctx, conds)
		sort.Slice(afterFunc, func(i, j int) bool {
			return bytes.Compare(afterFunc[i].HashCode(sc), afterFunc[j].HashCode(sc)) < 0
		})
		c.Assert(fmt.Sprintf("%s", afterFunc), Equals, tt.result, Commentf("wrong result for expr: %s", tt.exprStr))
	}
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, err
	}
	session.SetSchemaLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, err
}

func (s *testIntegrationSuite) TestPrefixIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  			name varchar(12) DEFAULT NULL,
  			KEY pname (name(12))
		)`)

	tk.MustExec("insert into t1 values('借款策略集_网页');")
	res := tk.MustQuery("select * from t1 where name = '借款策略集_网页';")
	res.Check(testkit.Rows("借款策略集_网页"))

	tk.MustExec(`CREATE TABLE prefix (
		a int(11) NOT NULL,
		b varchar(55) DEFAULT NULL,
		c int(11) DEFAULT NULL,
		PRIMARY KEY (a),
		KEY prefix_index (b(2)),
		KEY prefix_complex (a,b(2))
	);`)

	tk.MustExec("INSERT INTO prefix VALUES(0, 'b', 2), (1, 'bbb', 3), (2, 'bbc', 4), (3, 'bbb', 5), (4, 'abc', 6), (5, 'abc', 7), (6, 'abc', 7), (7, 'ÿÿ', 8), (8, 'ÿÿ0', 9), (9, 'ÿÿÿ', 10);")
	res = tk.MustQuery("select c, b from prefix where b > 'ÿ' and b < 'ÿÿc'")
	res.Check(testkit.Rows("8 ÿÿ", "9 ÿÿ0"))
}

func (s *testIntegrationSuite) TestUnknowHintIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("select /*+ unknown_hint(c1)*/ 1").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 29 near \"unknown_hint(c1)*/ 1\" "))
	_, err := tk.Exec("select 1 from /*+ test1() */ t")
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestForeignKeyVar(c *C) {

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("SET FOREIGN_KEY_CHECKS=1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 8047 variable 'foreign_key_checks' does not yet support value: 1"))
}

func (s *testIntegrationSuite) TestIssue10804(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET SESSION information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`0`))
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET GLOBAL information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`0`))
}

func (s *testIntegrationSuite) TestInvalidEndingStatement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	parseErrMsg := "[parser:1064]"
	errMsgLen := len(parseErrMsg)

	assertParseErr := func(sql string) {
		_, err := tk.Exec(sql)
		c.Assert(err, NotNil)
		c.Assert(err.Error()[:errMsgLen], Equals, parseErrMsg)
	}

	assertParseErr("drop table if exists t'xyz")
	assertParseErr("drop table if exists t'")
	assertParseErr("drop table if exists t`")
	assertParseErr(`drop table if exists t'`)
	assertParseErr(`drop table if exists t"`)
}

func (s *testIntegrationSuite) TestIssue10675(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int);`)
	tk.MustExec(`insert into t values(1);`)
	tk.MustQuery(`select * from t where a < -184467440737095516167.1;`).Check(testkit.Rows())
	tk.MustQuery(`select * from t where a > -184467440737095516167.1;`).Check(
		testkit.Rows("1"))
	tk.MustQuery(`select * from t where a < 184467440737095516167.1;`).Check(
		testkit.Rows("1"))
	tk.MustQuery(`select * from t where a > 184467440737095516167.1;`).Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestDefEnableVectorizedEvaluation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use mysql")
	tk.MustQuery(`select @@tidb_enable_vectorized_expression`).Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestDecodetoChunkReuse(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table chk (a int,b varchar(20))")
	for i := 0; i < 200; i++ {
		if i%5 == 0 {
			tk.MustExec(fmt.Sprintf("insert chk values (NULL,NULL)"))
			continue
		}
		tk.MustExec(fmt.Sprintf("insert chk values (%d,'%s')", i, strconv.Itoa(i)))
	}

	tk.Se.GetSessionVars().DistSQLScanConcurrency = 1
	tk.MustExec("set tidb_init_chunk_size = 2")
	tk.MustExec("set tidb_max_chunk_size = 32")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", variable.DefInitChunkSize))
		tk.MustExec(fmt.Sprintf("set tidb_max_chunk_size = %d", variable.DefMaxChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			if count%5 == 0 {
				c.Assert(req.GetRow(i).IsNull(0), Equals, true)
				c.Assert(req.GetRow(i).IsNull(1), Equals, true)
			} else {
				c.Assert(req.GetRow(i).IsNull(0), Equals, false)
				c.Assert(req.GetRow(i).IsNull(1), Equals, false)
				c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
				c.Assert(req.GetRow(i).GetString(1), Equals, strconv.Itoa(count))
			}
			count++
		}
	}
	c.Assert(count, Equals, 200)
	rs.Close()
}
