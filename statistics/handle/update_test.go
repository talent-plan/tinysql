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

package handle_test

import (
	"fmt"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	store kv.Storage
	do    *domain.Domain
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.store, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testStatsSuite) TestSingleSessionInsert(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("analyze table t1")
	testKit.MustExec("analyze table t2")

	rowCount1 := 10
	rowCount2 := 20
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("insert into t2 values(1, 2)")
	}

	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 := h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("analyze table t1")
	// Test update in a txn.
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*2))

	// Test IncreaseFactor.
	count, err := stats1.ColumnEqualRowCount(testKit.Se.GetSessionVars().StmtCtx, types.NewIntDatum(1), tableInfo1.Columns[0].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(rowCount1*2))

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	testKit.MustExec("commit")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("delete from t1 limit 1")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("update t2 set c2 = c1")
	}
	testKit.MustExec("commit")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*3))
	stats2 = h.GetTableStats(tableInfo2)
	c.Assert(stats2.Count, Equals, int64(rowCount2))

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(0))

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))

	rs = testKit.MustQuery("select tot_col_size from mysql.stats_histograms").Sort()
	rs.Check(testkit.Rows("0", "0", "20", "20"))

	// test dump delta only when `modify count / count` is greater than the ratio.
	originValue := handle.DumpStatsDeltaRatio
	handle.DumpStatsDeltaRatio = 0.5
	defer func() {
		handle.DumpStatsDeltaRatio = originValue
	}()
	handle.DumpStatsDeltaRatio = 0.5
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values (1,2)")
	}
	h.DumpStatsDeltaToKV(handle.DumpDelta)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	// not dumped
	testKit.MustExec("insert into t1 values (1,2)")
	h.DumpStatsDeltaToKV(handle.DumpDelta)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	h.FlushStats()
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsSuite) TestRollback(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("begin")
	testKit.MustExec("insert into t values (1,2)")
	testKit.MustExec("rollback")

	is := s.do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)

	stats := h.GetTableStats(tableInfo)
	c.Assert(stats.Count, Equals, int64(0))
	c.Assert(stats.ModifyCount, Equals, int64(0))
}

func (s *testStatsSuite) TestMultiSession(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("analyze table t1")

	rowCount1 := 10
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	testKit1 := testkit.NewTestKit(c, s.store)
	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}
	testKit2 := testkit.NewTestKit(c, s.store)
	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}
	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 := h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}

	testKit.Se.Close()
	testKit2.Se.Close()

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1*2))
	// The session in testKit is already Closed, set it to nil will create a new session.
	testKit.Se = nil
	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("60"))
}

func (s *testStatsSuite) TestTxnWithFailure(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int primary key, c2 int)")
	testKit.MustExec("analyze table t1")

	is := s.do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tableInfo1 := tbl1.Meta()
	h := s.do.StatsHandle()

	rowCount1 := 10
	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(?, 2)", i)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 := h.GetTableStats(tableInfo1)
	// have not commit
	c.Assert(stats1.Count, Equals, int64(0))
	testKit.MustExec("commit")

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	c.Assert(err, NotNil)

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1))

	testKit.MustExec("insert into t1 values(-1, 2)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	stats1 = h.GetTableStats(tableInfo1)
	c.Assert(stats1.Count, Equals, int64(rowCount1+1))
}

func (s *testStatsSuite) TestUpdatePartition(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	createTable := `CREATE TABLE t (a int, b char(5)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11))`
	testKit.MustExec(createTable)
	testKit.MustExec("analyze table t")
	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	pi := tableInfo.GetPartitionInfo()
	c.Assert(len(pi.Definitions), Equals, 2)
	bColID := tableInfo.Columns[1].ID

	testKit.MustExec(`insert into t values (1, "a"), (7, "a")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(1))
		c.Assert(statsTbl.Count, Equals, int64(1))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(2))
	}

	testKit.MustExec(`update t set a = a + 1, b = "aa"`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(2))
		c.Assert(statsTbl.Count, Equals, int64(1))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(3))
	}

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(tableInfo, def.ID)
		c.Assert(statsTbl.ModifyCount, Equals, int64(3))
		c.Assert(statsTbl.Count, Equals, int64(0))
		c.Assert(statsTbl.Columns[bColID].TotColSize, Equals, int64(0))
	}
}

func appendBucket(h *statistics.Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
}

func (s *testStatsSuite) TestSplitRange(c *C) {
	h := statistics.NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 1, 1)
	appendBucket(h, 2, 5)
	appendBucket(h, 7, 7)
	appendBucket(h, 8, 8)
	appendBucket(h, 10, 13)

	tests := []struct {
		points  []int64
		exclude []bool
		result  string
	}{
		{
			points:  []int64{1, 1},
			exclude: []bool{false, false},
			result:  "[1,1]",
		},
		{
			points:  []int64{0, 1, 3, 8, 8, 20},
			exclude: []bool{true, false, true, false, true, false},
			result:  "(0,1],(3,7),[7,8),[8,8],(8,10),[10,20]",
		},
		{
			points:  []int64{8, 10, 20, 30},
			exclude: []bool{false, false, true, true},
			result:  "[8,10),[10,10],(20,30)",
		},
		{
			// test remove invalid range
			points:  []int64{8, 9},
			exclude: []bool{false, true},
			result:  "[8,9)",
		},
	}
	for _, t := range tests {
		ranges := make([]*ranger.Range, 0, len(t.points)/2)
		for i := 0; i < len(t.points); i += 2 {
			ranges = append(ranges, &ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(t.points[i])},
				LowExclude:  t.exclude[i],
				HighVal:     []types.Datum{types.NewIntDatum(t.points[i+1])},
				HighExclude: t.exclude[i+1],
			})
		}
		ranges, _ = h.SplitRange(nil, ranges, false)
		var ranStrs []string
		for _, ran := range ranges {
			ranStrs = append(ranStrs, ran.String())
		}
		c.Assert(strings.Join(ranStrs, ","), Equals, t.result)
	}
}

func (s *testStatsSuite) TestOutOfOrderUpdate(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("insert into t values (1,2)")

	do := s.do
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	testKit.MustExec("analyze table t")

	// Simulate the case that another tidb has inserted some value, but delta info has not been dumped to kv yet.
	testKit.MustExec("insert into t values (2,2),(4,5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("1"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("0"))
}

func (s *testStatsSuite) TestLoadHistCorrelation(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	h := s.do.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c int)")
	testKit.MustExec("insert into t values(1),(2),(3),(4),(5)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	h.Clear()
	c.Assert(h.Update(s.do.InfoSchema()), IsNil)
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustExec("explain select * from t where c = 1")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'")
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "1")
}
