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

package core_test

import (
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var _ = Suite(&testPrepareSuite{})
var _ = SerialSuites(&testPrepareSerialSuite{})

type testPrepareSuite struct {
}

type testPrepareSerialSuite struct {
}

func (s *testPrepareSuite) TestPrepareOverMaxPreparedStmtCount(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")

	// test prepare and deallocate.
	prePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	tk.MustExec(`prepare stmt1 from "select 1"`)
	onePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared+1, Equals, onePrepared)
	tk.MustExec(`deallocate prepare stmt1`)
	deallocPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared, Equals, deallocPrepared)

	// test change global limit and make it affected in test session.
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("-1"))
	tk.MustExec("set @@global.max_prepared_stmt_count = 2")
	tk.MustQuery("select @@global.max_prepared_stmt_count").Check(testkit.Rows("2"))

	// Disable global variable cache, so load global session variable take effect immediate.
	dom.GetGlobalVarsCache().Disable()

	// test close session to give up all prepared stmt
	tk.MustExec(`prepare stmt2 from "select 1"`)
	prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
	tk.Se.Close()
	drawPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared-1, Equals, drawPrepared)

	// test meet max limit.
	tk.Se = nil
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("2"))
	for i := 1; ; i++ {
		prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
		if prePrepared >= 2 {
			_, err = tk.Exec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
			c.Assert(terror.ErrorEqual(err, variable.ErrMaxPreparedStmtCountReached), IsTrue)
			break
		}
		tk.Exec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
	}
}

func readGaugeInt(g prometheus.Gauge) int {
	ch := make(chan prometheus.Metric, 1)
	g.Collect(ch)
	m := <-ch
	mm := &dto.Metric{}
	m.Write(mm)
	return int(mm.GetGauge().GetValue())
}

// unit test for issue https://github.com/pingcap/tidb/issues/9478
func (s *testPrepareSuite) TestPrepareWithWindowFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("use test")
	tk.MustExec("create table window_prepare(a int, b double)")
	tk.MustExec("insert into window_prepare values(1, 1.1), (2, 1.9)")
	tk.MustExec("prepare stmt1 from 'select row_number() over() from window_prepare';")
	// Test the unnamed window can be executed successfully.
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1", "2"))
	// Test the stmt can be prepared successfully.
	tk.MustExec("prepare stmt2 from 'select count(a) over (order by a rows between ? preceding and ? preceding) from window_prepare'")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt2 using @a, @b").Check(testkit.Rows("0", "0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByItems(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int)")
	tk.MustExec("insert into t(id, v) values(1, 2),(1, 2),(2, 3);")
	tk.MustExec("prepare s1 from 'select max(v) from t group by floor(id/?)';")
	tk.MustExec("set @a=2;")
	tk.MustQuery("execute s1 using @a;").Sort().Check(testkit.Rows("2", "3"))

	tk.MustExec("prepare s1 from 'select max(v) from t group by ?';")
	tk.MustExec("set @a=2;")
	err = tk.ExecToErr("execute s1 using @a;")
	c.Assert(err.Error(), Equals, "Unknown column '2' in 'group statement'")
	tk.MustExec("set @a=2.0;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("3"))
}
