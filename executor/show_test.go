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
	"github.com/pingcap/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
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

	// Test Warning level 'Note'
	testSQL = `create table show_warnings_2 (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table if not exists show_warnings_2 like show_warnings`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1050|Table 'test.show_warnings_2' already exists"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "1"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "0"))
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

func (s *testSuite5) TestIssue3641(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	_, err := tk.Exec("show tables;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	_, err = tk.Exec("show table status;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
}

func (s *testSuite5) TestCollation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	rs, err := tk.Exec("show collation;")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[1].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[2].Column.Tp, Equals, mysql.TypeLonglong)
	c.Assert(fields[3].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[4].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[5].Column.Tp, Equals, mysql.TypeLonglong)
}

func (s *testSuite5) TestShowOpenTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("show open tables")
	tk.MustQuery("show open tables in test")
}

func (s *testSuite5) TestShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustQuery("show create table v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("drop table t1")

	tk.MustExec("drop view if exists v")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v as select JSON_MERGE('{}', '{}') as col;")
	tk.MustQuery("show create view v").Check(testutil.RowsWithSep("|", "v|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v` (`col`) AS SELECT JSON_MERGE('{}', '{}') AS `col`  "))
	tk.MustExec("drop view if exists v")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select avg(a),t1.* from t1 group by a")
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`avg(a)`, `a`, `b`) AS SELECT AVG(`a`),`test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1` GROUP BY `a`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select a+b, t1.* , a as c from t1")
	tk.MustQuery("show create view v1").Check(testutil.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a+b`, `a`, `b`, `c`) AS SELECT `a`+`b`,`test`.`t1`.`a`,`test`.`t1`.`b`,`a` AS `c` FROM `test`.`t1`  "))
	tk.MustExec("drop table t1")
	tk.MustExec("drop view v1")

	// For issue #9211
	tk.MustExec("create table t(c int, b int as (c + 1))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(c int, b int as (c + 1) not null)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t ( a char(10) charset utf8 collate utf8_bin, b char(10) as (rtrim(a)));")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` char(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `b` char(10) GENERATED ALWAYS AS (rtrim(`a`)) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec(`drop table if exists different_charset`)
	tk.MustExec(`create table different_charset(ch1 varchar(10) charset utf8, ch2 varchar(10) charset binary);`)
	tk.MustQuery(`show create table different_charset`).Check(testutil.RowsWithSep("|",
		""+
			"different_charset CREATE TABLE `different_charset` (\n"+
			"  `ch1` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `ch2` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table `t` (\n" +
		"`a` timestamp not null default current_timestamp,\n" +
		"`b` timestamp(3) default current_timestamp(3),\n" +
		"`c` datetime default current_timestamp,\n" +
		"`d` datetime(4) default current_timestamp(4),\n" +
		"`e` varchar(20) default 'cUrrent_tImestamp',\n" +
		"`f` datetime(2) default current_timestamp(2) on update current_timestamp(2),\n" +
		"`g` timestamp(2) default current_timestamp(2) on update current_timestamp(2))")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"+
			"  `c` datetime DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `d` datetime(4) DEFAULT CURRENT_TIMESTAMP(4),\n"+
			"  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n"+
			"  `f` datetime(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2),\n"+
			"  `g` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec("create table t (a int, b int) shard_row_id_bits = 4 pre_split_regions=3;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000 SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop table t")

	//for issue #11831
	tk.MustExec("create table ttt4(a varchar(123) default null collate utf8mb4_unicode_ci)engine=innodb default charset=utf8mb4 collate=utf8mb4_unicode_ci;")
	tk.MustQuery("show create table `ttt4`").Check(testutil.RowsWithSep("|",
		""+
			"ttt4 CREATE TABLE `ttt4` (\n"+
			"  `a` varchar(123) COLLATE utf8mb4_unicode_ci DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	))
	tk.MustExec("create table ttt5(a varchar(123) default null)engine=innodb default charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table `ttt5`").Check(testutil.RowsWithSep("|",
		""+
			"ttt5 CREATE TABLE `ttt5` (\n"+
			"  `a` varchar(123) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
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

	tk.MustExec("rename table \"t`abl\"\"e\" to t")
	tk.MustExec("set sql_mode=@old_sql_mode")
}
