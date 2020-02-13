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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite5) TestShowWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))

	// Test Warning level 'Error'
	testSQL = `create table show_warnings (a int)`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_warnings' already exists"))
	tk.MustQuery("select @@error_count").Check(testutil.RowsWithSep("|", "1"))
}

func (s *testSuite5) TestShowErrors(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_errors (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table show_errors (a int)`
	tk.Exec(testSQL)

	tk.MustQuery("show errors").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_errors' already exists"))
}

func (s *testSuite5) TestShowEscape(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists `t``abl\"e`")
	tk.MustExec("create table `t``abl\"e`(`c``olum\"n` int(11) primary key)")
	tk.MustQuery("show create table `t``abl\"e`").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE `t``abl\"e` (\n"+
			"  `c``olum\"n` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`c``olum\"n`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// ANSI_QUOTES will change the SHOW output
	tk.MustExec("set @old_sql_mode=@@sql_mode")
	tk.MustExec("set sql_mode=ansi_quotes")
	tk.MustQuery("show create table \"t`abl\"\"e\"").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE \"t`abl\"\"e\" (\n"+
			"  \"c`olum\"\"n\" int(11) NOT NULL,\n"+
			"  PRIMARY KEY (\"c`olum\"\"n\")\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table \"t`abl\"\"e\"")
	tk.MustExec("set sql_mode=@old_sql_mode")
}
