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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestAnalyzePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("analyze table t")

	is := infoschema.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi := table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	do, err := session.GetDomain(s.store)
	c.Assert(err, IsNil)
	handle := do.StatsHandle()
	for _, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Assert(len(statsTbl.Columns), Equals, 3)
		c.Assert(len(statsTbl.Indices), Equals, 1)
		for _, col := range statsTbl.Columns {
			c.Assert(col.Len(), Greater, 0)
		}
		for _, idx := range statsTbl.Indices {
			c.Assert(idx.Len(), Greater, 0)
		}
	}

	tk.MustExec("drop table t")
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("alter table t analyze partition p0")
	is = infoschema.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi = table.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	for i, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
		if i == 0 {
			c.Assert(statsTbl.Pseudo, IsFalse)
			c.Assert(len(statsTbl.Columns), Equals, 3)
			c.Assert(len(statsTbl.Indices), Equals, 1)
		} else {
			c.Assert(statsTbl.Pseudo, IsTrue)
		}
	}
}

func (s *testSuite1) TestAnalyzeReplicaReadFollower(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().SetReplicaRead(kv.ReplicaReadFollower)
	tk.MustExec("analyze table t")
}

func (s *testSuite1) TestAnalyzeRestrict(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Se.(sessionctx.Context)
	ctx.GetSessionVars().InRestrictedSQL = true
	tk.MustExec("analyze table t")
}

func (s *testSuite1) TestAnalyzeParameters(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustExec("insert into t values (19), (19), (19)")

	tk.MustExec("set @@tidb_enable_fast_analyze = 1")
	tk.MustExec("analyze table t with 30 samples")
	is := infoschema.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	col := tbl.Columns[1]
	c.Assert(col.Len(), Equals, 20)
	c.Assert(len(col.CMSketch.TopN()), Equals, 1)
	width, depth := col.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(5))
	c.Assert(width, Equals, int32(2048))

	tk.MustExec("analyze table t with 4 buckets, 0 topn, 4 cmsketch width, 4 cmsketch depth")
	tbl = s.dom.StatsHandle().GetTableStats(tableInfo)
	col = tbl.Columns[1]
	c.Assert(col.Len(), Equals, 4)
	c.Assert(len(col.CMSketch.TopN()), Equals, 0)
	width, depth = col.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(4))
	c.Assert(width, Equals, int32(4))
}

func (s *testSuite1) TestAnalyzeTooLongColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze table t")
	is := infoschema.GetInfoSchema(tk.Se.(sessionctx.Context))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := table.Meta()
	tbl := s.dom.StatsHandle().GetTableStats(tableInfo)
	c.Assert(tbl.Columns[1].Len(), Equals, 0)
	c.Assert(tbl.Columns[1].TotColSize, Equals, int64(65559))
}

func (s *testSuite1) TestExtractTopN(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, 0)", i+10))
	}
	tk.MustExec("analyze table t")
	is := s.dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := table.Meta()
	tblStats := s.dom.StatsHandle().GetTableStats(tblInfo)
	colStats := tblStats.Columns[tblInfo.Columns[1].ID]
	c.Assert(len(colStats.CMSketch.TopN()), Equals, 1)
	item := colStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
	idxStats := tblStats.Indices[tblInfo.Indices[0].ID]
	c.Assert(len(idxStats.CMSketch.TopN()), Equals, 1)
	item = idxStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
}
