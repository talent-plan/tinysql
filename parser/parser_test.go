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

package parser_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

func (s *testParserSuite) TestSimple(c *C) {
	parser := parser.New()

	reservedKws := []string{
		"add", "all", "alter", "analyze", "and", "as", "asc", "between", "bigint",
		"binary", "blob", "both", "by", "cascade", "case", "change", "character", "check", "collate",
		"column", "constraint", "convert", "create", "cross", "current_date", "current_time",
		"current_timestamp", "current_user", "database", "databases", "day_hour", "day_microsecond",
		"day_minute", "day_second", "decimal", "default", "delete", "desc", "describe",
		"distinct", "distinctRow", "div", "double", "drop", "dual", "else", "enclosed", "escaped",
		"exists", "explain", "false", "float", "for", "force", "foreign", "from",
		"fulltext", "grant", "group", "having", "hour_microsecond", "hour_minute",
		"hour_second", "if", "ignore", "in", "index", "infile", "inner", "insert", "int", "into", "integer",
		"interval", "is", "join", "key", "keys", "kill", "leading", "left", "like", "limit", "lines", "load",
		"localtime", "localtimestamp", "lock", "longblob", "longtext", "mediumblob", "maxvalue", "mediumint", "mediumtext",
		"minute_microsecond", "minute_second", "mod", "not", "no_write_to_binlog", "null", "numeric",
		"on", "option", "optionally", "or", "order", "outer", "partition", "precision", "primary", "procedure", "range", "read", "real",
		"references", "regexp", "rename", "repeat", "replace", "revoke", "restrict", "right", "rlike",
		"schema", "schemas", "second_microsecond", "select", "set", "show", "smallint",
		"starting", "table", "terminated", "then", "tinyblob", "tinyint", "tinytext", "to",
		"trailing", "true", "union", "unique", "unlock", "unsigned",
		"update", "use", "using", "utc_date", "values", "varbinary", "varchar",
		"when", "where", "write", "xor", "year_month", "zerofill",
		"generated", "virtual", "stored", "usage",
		"delayed", "high_priority", "low_priority",
		"cumeDist", "denseRank", "firstValue", "lag", "lastValue", "lead", "nthValue", "ntile",
		"over", "percentRank", "rank", "row", "rows", "rowNumber", "window", "linear",
		"match", "language", "until",
		// TODO: support the following keywords
		// "with",
	}
	for _, kw := range reservedKws {
		src := fmt.Sprintf("SELECT * FROM db.%s;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT * FROM %s.desc", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT t.%s FROM t", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datediff", "datetime", "deallocate", "do", "from_days", "end", "engine", "engines", "execute", "extended", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "tablespace", "text", "time", "timestamp", "tidb", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substr", "subpartition", "subpartitions", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "quarter", "escape", "grants", "status", "fields", "triggers",
		"delay_key_write", "isolation", "partitions", "repeatable", "committed", "uncommitted", "only", "serializable", "level",
		"curtime", "variables", "dayname", "version", "btree", "hash", "row_format", "dynamic", "fixed", "compressed",
		"compact", "redundant", "sql_no_cache sql_no_cache", "sql_cache sql_cache", "action", "round",
		"enable", "disable", "reverse", "space", "privileges", "get_lock", "release_lock", "sleep", "no", "greatest", "least",
		"binlog", "hex", "unhex", "function", "indexes", "from_unixtime", "processlist", "events", "less", "than", "timediff",
		"ln", "log", "log2", "log10", "timestampdiff", "pi", "quote", "none", "super", "shared", "exclusive",
		"always", "stats", "stats_meta", "stats_histogram", "stats_buckets", "stats_healthy", "tidb_version", "replication", "slave", "client",
		"max_connections_per_hour", "max_queries_per_hour", "max_updates_per_hour", "max_user_connections", "event", "reload", "routine", "temporary",
		"following", "preceding", "unbounded", "respect", "nulls", "current", "last", "against", "expansion",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for -- Comment and unary -- operator
	src := "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	stmts, _, err := parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 2)

	// Testcase for /*! xx */
	// See http://dev.mysql.com/doc/refman/5.7/en/comments.html
	// Fix: https://github.com/pingcap/tidb/issues/971
	src = "/*!40101 SET character_set_client = utf8 */;"
	stmts, _, err = parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 1)
	stmt := stmts[0]
	_, ok := stmt.(*ast.SetStmt)
	c.Assert(ok, IsTrue)

	// for issue #2017
	src = "insert into blobtable (a) values ('/*! truncated */');"
	stmt, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	is, ok := stmt.(*ast.InsertStmt)
	c.Assert(ok, IsTrue)
	c.Assert(is.Lists, HasLen, 1)
	c.Assert(is.Lists[0], HasLen, 1)
	c.Assert(is.Lists[0][0].(ast.ValueExpr).GetDatumString(), Equals, "/*! truncated */")

	// for issue #961
	src = "create table t (c int key);"
	st, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	cs, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	c.Assert(cs.Cols, HasLen, 1)
	c.Assert(cs.Cols[0].Options, HasLen, 1)
	c.Assert(cs.Cols[0].Options[0].Tp, Equals, ast.ColumnOptionPrimaryKey)

	// for issue #4497
	src = "create table t1(a NVARCHAR(100));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for issue 2803
	src = "use quote;"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// issue #4354
	src = "select b'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = "select B'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// src = "select 0b'';"
	// _, err = parser.ParseOneStmt(src, "", "")
	// c.Assert(err, NotNil)

	// for #4909, support numericType `signed` filedOpt.
	src = "CREATE TABLE t(_sms smallint signed, _smu smallint unsigned);"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for #7371, support NATIONAL CHARACTER
	// reference link: https://dev.mysql.com/doc/refman/5.7/en/charset-national.html
	src = "CREATE TABLE t(c1 NATIONAL CHARACTER(10));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = `CREATE TABLE t(a tinyint signed,
		b smallint signed,
		c mediumint signed,
		d int signed,
		e int1 signed,
		f int2 signed,
		g int3 signed,
		h int4 signed,
		i int8 signed,
		j integer signed,
		k bigint signed,
		l bool signed,
		m boolean signed
		);`

	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ct, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	for _, col := range ct.Cols {
		c.Assert(col.Tp.Flag&mysql.UnsignedFlag, Equals, uint(0))
	}

	// for issue #4006
	src = `insert into tb(v) (select v from tb);`
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for issue #9823
	src = "SELECT 9223372036854775807;"
	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	sel, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	expr := sel.Fields.Fields[0]
	vExpr := expr.Expr.(*driver.ValueExpr)
	c.Assert(vExpr.Kind(), Equals, types.KindInt64)
	src = "SELECT 9223372036854775808;"
	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	sel, ok = st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	expr = sel.Fields.Fields[0]
	vExpr = expr.Expr.(*driver.ValueExpr)
	c.Assert(vExpr.Kind(), Equals, types.KindUint64)
}

func (s *testParserSuite) TestSpecialComments(c *C) {
	parser := parser.New()

	// 1. Make sure /*! ... */ respects the same SQL mode.
	_, err := parser.ParseOneStmt(`SELECT /*! '\' */;`, "", "")
	c.Assert(err, NotNil)

	parser.SetSQLMode(mysql.ModeNoBackslashEscapes)
	st, err := parser.ParseOneStmt(`SELECT /*! '\' */;`, "", "")
	c.Assert(err, IsNil)
	c.Assert(st, FitsTypeOf, &ast.SelectStmt{})

	// 2. Make sure multiple statements inside /*! ... */ will not crash
	// (this is issue #330)
	stmts, _, err := parser.Parse("/*! SET x = 1; SELECT 2 */", "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 2)
	c.Assert(stmts[0], FitsTypeOf, &ast.SetStmt{})
	c.Assert(stmts[0].Text(), Equals, "SET x = 1;")
	c.Assert(stmts[1], FitsTypeOf, &ast.SelectStmt{})
	c.Assert(stmts[1].Text(), Equals, "/*! SET x = 1; SELECT 2 */")
	// ^ not sure if correct approach; having multiple statements in MySQL is a syntax error.

	// 3. Make sure invalid text won't cause infinite loop
	// (this is issue #336)
	st, err = parser.ParseOneStmt("SELECT /*+ 😅 */ SLEEP(1);", "", "")
	c.Assert(err, IsNil)
	sel, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(sel.TableHints, HasLen, 0)
}

type testCase struct {
	src     string
	ok      bool
	restore string
}

type testErrMsgCase struct {
	src string
	ok  bool
	err error
}

func (s *testParserSuite) RunTest(c *C, table []testCase) {
	parser := parser.New()
	for _, t := range table {
		_, _, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if !t.ok {
			c.Assert(err, NotNil, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
	}
}

func (s *testParserSuite) RunRestoreTest(c *C, sourceSQLs, expectSQLs string) {
	var sb strings.Builder
	parser := parser.New()
	comment := Commentf("source %v", sourceSQLs)
	stmts, _, err := parser.Parse(sourceSQLs, "", "")
	c.Assert(err, IsNil, comment)
	restoreSQLs := ""
	for _, stmt := range stmts {
		sb.Reset()
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil, comment)
		restoreSQL := sb.String()
		comment = Commentf("source %v; restore %v", sourceSQLs, restoreSQL)
		restoreStmt, err := parser.ParseOneStmt(restoreSQL, "", "")
		c.Assert(err, IsNil, comment)
		CleanNodeText(stmt)
		CleanNodeText(restoreStmt)
		c.Assert(restoreStmt, DeepEquals, stmt, comment)
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL
	}
	comment = Commentf("restore %v; expect %v", restoreSQLs, expectSQLs)
	c.Assert(restoreSQLs, Equals, expectSQLs, comment)
}

func (s *testParserSuite) RunTestInRealAsFloatMode(c *C, table []testCase) {
	parser := parser.New()
	parser.SetSQLMode(mysql.ModeRealAsFloat)
	for _, t := range table {
		_, _, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if !t.ok {
			c.Assert(err, NotNil, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
		// restore correctness test
		if t.ok {
			s.RunRestoreTestInRealAsFloatMode(c, t.src, t.restore)
		}
	}
}

func (s *testParserSuite) RunRestoreTestInRealAsFloatMode(c *C, sourceSQLs, expectSQLs string) {
	var sb strings.Builder
	parser := parser.New()
	parser.SetSQLMode(mysql.ModeRealAsFloat)
	comment := Commentf("source %v", sourceSQLs)
	stmts, _, err := parser.Parse(sourceSQLs, "", "")
	c.Assert(err, IsNil, comment)
	restoreSQLs := ""
	for _, stmt := range stmts {
		sb.Reset()
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil, comment)
		restoreSQL := sb.String()
		comment = Commentf("source %v; restore %v", sourceSQLs, restoreSQL)
		restoreStmt, err := parser.ParseOneStmt(restoreSQL, "", "")
		c.Assert(err, IsNil, comment)
		CleanNodeText(stmt)
		CleanNodeText(restoreStmt)
		c.Assert(restoreStmt, DeepEquals, stmt, comment)
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL
	}
	comment = Commentf("restore %v; expect %v", restoreSQLs, expectSQLs)
	c.Assert(restoreSQLs, Equals, expectSQLs, comment)
}

func (s *testParserSuite) RunErrMsgTest(c *C, table []testErrMsgCase) {
	parser := parser.New()
	for _, t := range table {
		_, _, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if t.err != nil {
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}

func (s *testParserSuite) TestDMLStmt(c *C) {
	table := []testCase{
		{"", true, ""},
		{";", true, ""},
		{"INSERT INTO foo VALUES (1234)", true, "INSERT INTO `foo` VALUES (1234)"},
		{"INSERT INTO foo VALUES (1234, 5678)", true, "INSERT INTO `foo` VALUES (1234,5678)"},
		{"INSERT INTO t1 (SELECT * FROM t2)", true, "INSERT INTO `t1` SELECT * FROM `t2`"},
		// 15
		{"INSERT INTO foo VALUES (1 || 2)", true, "INSERT INTO `foo` VALUES (1 OR 2)"},
		{"INSERT INTO foo VALUES (1 | 2)", true, "INSERT INTO `foo` VALUES (1|2)"},
		{"INSERT INTO foo VALUES (false || true)", true, "INSERT INTO `foo` VALUES (FALSE OR TRUE)"},
		{"INSERT INTO foo VALUES (bar(5678))", true, "INSERT INTO `foo` VALUES (BAR(5678))"},
		// 20
		{"INSERT INTO foo VALUES ()", true, "INSERT INTO `foo` VALUES ()"},
		{"SELECT * FROM t", true, "SELECT * FROM `t`"},
		{"SELECT * FROM t AS u", true, "SELECT * FROM `t` AS `u`"},
		// 25
		{"SELECT * FROM t, v", true, "SELECT * FROM (`t`) JOIN `v`"},
		{"SELECT * FROM t AS u, v", true, "SELECT * FROM (`t` AS `u`) JOIN `v`"},
		{"SELECT * FROM t, v AS w", true, "SELECT * FROM (`t`) JOIN `v` AS `w`"},
		{"SELECT * FROM t AS u, v AS w", true, "SELECT * FROM (`t` AS `u`) JOIN `v` AS `w`"},
		{"SELECT * FROM foo, bar, foo", true, "SELECT * FROM ((`foo`) JOIN `bar`) JOIN `foo`"},
		// 30
		{"SELECT DISTINCTS * FROM t", false, ""},
		{"SELECT DISTINCT * FROM t", true, "SELECT DISTINCT * FROM `t`"},
		{"SELECT DISTINCTROW * FROM t", true, "SELECT DISTINCT * FROM `t`"},
		{"SELECT ALL * FROM t", true, "SELECT * FROM `t`"},
		{"SELECT DISTINCT ALL * FROM t", false, ""},
		{"SELECT DISTINCTROW ALL * FROM t", false, ""},
		{"INSERT INTO foo (a) VALUES (42)", true, "INSERT INTO `foo` (`a`) VALUES (42)"},
		{"INSERT INTO foo (a,) VALUES (42,)", false, ""},
		// 35
		{"INSERT INTO foo (a,b) VALUES (42,314)", true, "INSERT INTO `foo` (`a`,`b`) VALUES (42,314)"},
		{"INSERT INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"INSERT INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"INSERT INTO foo () VALUES ()", true, "INSERT INTO `foo` () VALUES ()"},
		{"INSERT INTO foo VALUE ()", true, "INSERT INTO `foo` VALUES ()"},

		// for issue 2402
		{"INSERT INTO tt VALUES (01000001783);", true, "INSERT INTO `tt` VALUES (1000001783)"},
		{"INSERT INTO tt VALUES (default);", true, "INSERT INTO `tt` VALUES (DEFAULT)"},

		{"REPLACE INTO foo VALUES (1 || 2)", true, "REPLACE INTO `foo` VALUES (1 OR 2)"},
		{"REPLACE INTO foo VALUES (1 | 2)", true, "REPLACE INTO `foo` VALUES (1|2)"},
		{"REPLACE INTO foo VALUES (false || true)", true, "REPLACE INTO `foo` VALUES (FALSE OR TRUE)"},
		{"REPLACE INTO foo VALUES (bar(5678))", true, "REPLACE INTO `foo` VALUES (BAR(5678))"},
		{"REPLACE INTO foo VALUES ()", true, "REPLACE INTO `foo` VALUES ()"},
		{"REPLACE INTO foo (a,b) VALUES (42,314)", true, "REPLACE INTO `foo` (`a`,`b`) VALUES (42,314)"},
		{"REPLACE INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"REPLACE INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"REPLACE INTO foo () VALUES ()", true, "REPLACE INTO `foo` () VALUES ()"},
		{"REPLACE INTO foo VALUE ()", true, "REPLACE INTO `foo` VALUES ()"},
		{"BEGIN", true, "START TRANSACTION"},
		// 45
		{"COMMIT", true, "COMMIT"},
		{"ROLLBACK", true, "ROLLBACK"},
		{`BEGIN;
			INSERT INTO foo VALUES (42, 3.14);
			INSERT INTO foo VALUES (-1, 2.78);
		COMMIT;`, true, "START TRANSACTION; INSERT INTO `foo` VALUES (42,3.14); INSERT INTO `foo` VALUES (-1,2.78); COMMIT"},
		{`BEGIN;
			INSERT INTO tmp SELECT * from bar;
			SELECT * from tmp;
		ROLLBACK;`, true, "START TRANSACTION; INSERT INTO `tmp` SELECT * FROM `bar`; SELECT * FROM `tmp`; ROLLBACK"},

		// qualified select
		{"SELECT a.b.c FROM t", true, "SELECT `a`.`b`.`c` FROM `t`"},
		{"SELECT a.b.*.c FROM t", false, ""},
		{"SELECT a.b.* FROM t", true, "SELECT `a`.`b`.* FROM `t`"},
		{"SELECT a FROM t", true, "SELECT `a` FROM `t`"},
		{"SELECT a.b.c.d FROM t", false, ""},

		// from join
		{"SELECT * from t1, t2, t3", true, "SELECT * FROM ((`t1`) JOIN `t2`) JOIN `t3`"},
		{"select * from t1 join t2 left join t3 on t2.id = t3.id", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` ON `t2`.`id`=`t3`.`id`"},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`) LEFT JOIN `t3` ON `t3`.`id`=`t2`.`id`"},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3", false, ""},

		// delete statement
		// single table syntax
		{"DELETE from t1", true, "DELETE FROM `t1`"},
		{"DELETE from t1.*", false, ""},
		{"DELETE LOW_priORITY from t1", true, "DELETE LOW_PRIORITY FROM `t1`"},
		{"DELETE quick from t1", true, "DELETE QUICK FROM `t1`"},
		{"DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a`"},
		{"delete from t1 where a=26", true, "DELETE FROM `t1` WHERE `a`=26"},
		{"DELETE from t1 where a=1 limit 1", true, "DELETE FROM `t1` WHERE `a`=1 LIMIT 1"},
		{"DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a LIMIT 1", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a` LIMIT 1"},
		{"DELETE FROM x.y z WHERE z.a > 0", true, "DELETE FROM `x`.`y` AS `z` WHERE `z`.`a`>0"},
		{"DELETE FROM t1 AS w WHERE a > 0", true, "DELETE FROM `t1` AS `w` WHERE `a`>0"},

		// for fail case
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false, ""},
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id order by t1.id;", false, ""},

		// for admin
		{"admin show ddl;", true, "ADMIN SHOW DDL"},
		{"admin show ddl jobs;", true, "ADMIN SHOW DDL JOBS"},
		{"admin show ddl jobs where id > 0;", true, "ADMIN SHOW DDL JOBS WHERE `id`>0"},
		{"admin show ddl jobs 20 where id=0;", true, "ADMIN SHOW DDL JOBS 20 WHERE `id`=0"},
		{"admin show ddl jobs -1;", false, ""},
		{"admin show ddl job queries 1", true, "ADMIN SHOW DDL JOB QUERIES 1"},
		{"admin show ddl job queries 1, 2, 3, 4", true, "ADMIN SHOW DDL JOB QUERIES 1, 2, 3, 4"},
		{"admin show t1 next_row_id", true, "ADMIN SHOW `t1` NEXT_ROW_ID"},
		{"admin check table t1, t2;", true, "ADMIN CHECK TABLE `t1`, `t2`"},
		{"admin check index tableName idxName;", true, "ADMIN CHECK INDEX `tableName` idxName"},
		{"admin check index tableName idxName (1, 2), (4, 5);", true, "ADMIN CHECK INDEX `tableName` idxName (1,2), (4,5)"},
		{"admin cancel ddl jobs 1", true, "ADMIN CANCEL DDL JOBS 1"},
		{"admin cancel ddl jobs 1, 2", true, "ADMIN CANCEL DDL JOBS 1, 2"},
		{"admin recover index t1 idx_a", true, "ADMIN RECOVER INDEX `t1` idx_a"},
		{"admin cleanup index t1 idx_a", true, "ADMIN CLEANUP INDEX `t1` idx_a"},
		{"admin show slow top 3", true, "ADMIN SHOW SLOW TOP 3"},
		{"admin show slow top internal 7", true, "ADMIN SHOW SLOW TOP INTERNAL 7"},
		{"admin show slow top all 9", true, "ADMIN SHOW SLOW TOP ALL 9"},
		{"admin show slow recent 11", true, "ADMIN SHOW SLOW RECENT 11"},

		// for insert ... set
		{"INSERT INTO t SET a=1,b=2", true, "INSERT INTO `t` SET `a`=1,`b`=2"},
		{"INSERT INTO t (a) SET a=1", false, ""},

		// for select with where clause
		{"SELECT * FROM t WHERE 1 = 1", true, "SELECT * FROM `t` WHERE 1=1"},

		// for dual
		{"select 1 from dual", true, "SELECT 1"},
		{"select 1 from dual limit 1", true, "SELECT 1 LIMIT 1"},
		{"select 1 as a from dual order by a", true, "SELECT 1 AS `a` ORDER BY `a`"},
		{"select 1 order by 1", true, "SELECT 1 ORDER BY 1"},

		// for https://github.com/pingcap/tidb/issues/1050
		{`SELECT /*!40001 SQL_NO_CACHE */ * FROM test WHERE 1 limit 0, 2000;`, true, "SELECT SQL_NO_CACHE * FROM `test` WHERE 1 LIMIT 0,2000"},

		{`ANALYZE TABLE t`, true, "ANALYZE TABLE `t`"},

		// for comments
		{`/** 20180417 **/ show databases;`, true, "SHOW DATABASES"},
		{`/* 20180417 **/ show databases;`, true, "SHOW DATABASES"},
		{`/** 20180417 */ show databases;`, true, "SHOW DATABASES"},
		{`/** 20180417 ******/ show databases;`, true, "SHOW DATABASES"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSetVariable(c *C) {
	table := []struct {
		Input    string
		Name     string
		IsGlobal bool
		IsSystem bool
	}{

		// Set system variable xx.xx, although xx.xx isn't a system variable, the parser should accept it.
		{"set xx.xx = 666", "xx.xx", false, true},
		// Set session system variable xx.xx
		{"set session xx.xx = 666", "xx.xx", false, true},
		{"set global xx.xx = 666", "xx.xx", true, true},

		{"set @@xx.xx = 666", "xx.xx", false, true},
		{"set @@session.xx.xx = 666", "xx.xx", false, true},
		{"set @@global.xx.xx = 666", "xx.xx", true, true},

		// Set user defined variable xx.xx
		{"set @xx.xx = 666", "xx.xx", false, false},
	}

	parser := parser.New()
	for _, t := range table {
		stmt, err := parser.ParseOneStmt(t.Input, "", "")
		c.Assert(err, IsNil)

		setStmt, ok := stmt.(*ast.SetStmt)
		c.Assert(ok, IsTrue)
		c.Assert(setStmt.Variables, HasLen, 1)

		v := setStmt.Variables[0]
		c.Assert(v.Name, Equals, t.Name)
		c.Assert(v.IsGlobal, Equals, t.IsGlobal)
		c.Assert(v.IsSystem, Equals, t.IsSystem)
	}

	_, err := parser.ParseOneStmt("set xx.xx.xx = 666", "", "")
	c.Assert(err, NotNil)
}

func (s *testParserSuite) TestExpression(c *C) {
	table := []testCase{
		// sign expression
		{"SELECT ++1", true, "SELECT ++1"},
		{"SELECT -*1", false, "SELECT -*1"},
		{"SELECT -+1", true, "SELECT -+1"},
		{"SELECT -1", true, "SELECT -1"},
		{"SELECT --1", true, "SELECT --1"},

		// for string literal
		{`select '''a''', """a"""`, true, "SELECT '''a''','\"a\"'"},
		{`select ''a''`, false, ""},
		{`select ""a""`, false, ""},
		{`select '''a''';`, true, "SELECT '''a'''"},
		{`select '\'a\'';`, true, "SELECT '''a'''"},
		{`select "\"a\"";`, true, "SELECT '\"a\"'"},
		{`select """a""";`, true, "SELECT '\"a\"'"},
		{`select _utf8"string";`, true, "SELECT _UTF8'string'"},
		{`select _binary"string";`, true, "SELECT _BINARY'string'"},
		{"select N'string'", true, "SELECT _UTF8'string'"},
		{"select n'string'", true, "SELECT _UTF8'string'"},
		// for comparison
		{"select 1 <=> 0, 1 <=> null, 1 = null", true, "SELECT 1<=>0,1<=>NULL,1=NULL"},
		// for date literal
		{"select date'1989-09-10'", true, "SELECT DATE '1989-09-10'"},
		{"select date 19890910", false, ""},
		// for time literal
		{"select time '00:00:00.111'", true, "SELECT TIME '00:00:00.111'"},
		{"select time 19890910", false, ""},
		// for timestamp literal
		{"select timestamp '1989-09-10 11:11:11'", true, "SELECT TIMESTAMP '1989-09-10 11:11:11'"},
		{"select timestamp 19890910", false, ""},

		// The ODBC syntax for time/date/timestamp literal.
		// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
		{"select {ts '1989-09-10 11:11:11'}", true, "SELECT '1989-09-10 11:11:11'"},
		{"select {d '1989-09-10'}", true, "SELECT '1989-09-10'"},
		{"select {t '00:00:00.111'}", true, "SELECT '00:00:00.111'"},
		// If the identifier is not in (t, d, ts), we just ignore it and consider the following expression as the value.
		// See: https://dev.mysql.com/doc/refman/5.7/en/expressions.html
		{"select {ts123 '1989-09-10 11:11:11'}", true, "SELECT '1989-09-10 11:11:11'"},
		{"select {ts123 123}", true, "SELECT 123"},
		{"select {ts123 1 xor 1}", true, "SELECT 1 XOR 1"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestBuiltin(c *C) {
	table := []testCase{
		// for builtin functions
		{"SELECT POW(1, 2)", true, "SELECT POW(1, 2)"},
		{"SELECT POW(1, 2, 1)", true, "SELECT POW(1, 2, 1)"}, // illegal number of arguments shall pass too
		{"SELECT POW(1, 0.5)", true, "SELECT POW(1, 0.5)"},
		{"SELECT POW(1, -1)", true, "SELECT POW(1, -1)"},
		{"SELECT POW(-1, 1)", true, "SELECT POW(-1, 1)"},
		{"SELECT RAND();", true, "SELECT RAND()"},
		{"SELECT RAND(1);", true, "SELECT RAND(1)"},
		{"SELECT MOD(10, 2);", true, "SELECT 10%2"},
		{"SELECT ROUND(-1.23);", true, "SELECT ROUND(-1.23)"},
		{"SELECT ROUND(1.23, 1);", true, "SELECT ROUND(1.23, 1)"},
		{"SELECT ROUND(1.23, 1, 1);", true, "SELECT ROUND(1.23, 1, 1)"},
		{"SELECT CEIL(-1.23);", true, "SELECT CEIL(-1.23)"},
		{"SELECT CEILING(1.23);", true, "SELECT CEILING(1.23)"},
		{"SELECT FLOOR(-1.23);", true, "SELECT FLOOR(-1.23)"},
		{"SELECT LN(1);", true, "SELECT LN(1)"},
		{"SELECT LN(1, 2);", true, "SELECT LN(1, 2)"},
		{"SELECT LOG(-2);", true, "SELECT LOG(-2)"},
		{"SELECT LOG(2, 65536);", true, "SELECT LOG(2, 65536)"},
		{"SELECT LOG(2, 65536, 1);", true, "SELECT LOG(2, 65536, 1)"},
		{"SELECT LOG2(2);", true, "SELECT LOG2(2)"},
		{"SELECT LOG2(2, 2);", true, "SELECT LOG2(2, 2)"},
		{"SELECT LOG10(10);", true, "SELECT LOG10(10)"},
		{"SELECT LOG10(10, 1);", true, "SELECT LOG10(10, 1)"},
		{"SELECT ABS(10, 1);", true, "SELECT ABS(10, 1)"},
		{"SELECT ABS(10);", true, "SELECT ABS(10)"},
		{"SELECT ABS();", true, "SELECT ABS()"},
		{"SELECT CONV(10+'10'+'10'+X'0a',10,10);", true, "SELECT CONV(10+'10'+'10'+x'0a', 10, 10)"},
		{"SELECT CONV();", true, "SELECT CONV()"},
		{"SELECT CRC32('MySQL');", true, "SELECT CRC32('MySQL')"},
		{"SELECT CRC32();", true, "SELECT CRC32()"},
		{"SELECT SIGN();", true, "SELECT SIGN()"},
		{"SELECT SIGN(0);", true, "SELECT SIGN(0)"},
		{"SELECT SQRT(0);", true, "SELECT SQRT(0)"},
		{"SELECT SQRT();", true, "SELECT SQRT()"},
		{"SELECT ACOS();", true, "SELECT ACOS()"},
		{"SELECT ACOS(1);", true, "SELECT ACOS(1)"},
		{"SELECT ACOS(1, 2);", true, "SELECT ACOS(1, 2)"},
		{"SELECT ASIN();", true, "SELECT ASIN()"},
		{"SELECT ASIN(1);", true, "SELECT ASIN(1)"},
		{"SELECT ASIN(1, 2);", true, "SELECT ASIN(1, 2)"},
		{"SELECT ATAN(0), ATAN(1), ATAN(1, 2);", true, "SELECT ATAN(0),ATAN(1),ATAN(1, 2)"},
		{"SELECT ATAN2(), ATAN2(1,2);", true, "SELECT ATAN2(),ATAN2(1, 2)"},
		{"SELECT COS(0);", true, "SELECT COS(0)"},
		{"SELECT COS(1);", true, "SELECT COS(1)"},
		{"SELECT COS(1, 2);", true, "SELECT COS(1, 2)"},
		{"SELECT COT();", true, "SELECT COT()"},
		{"SELECT COT(1);", true, "SELECT COT(1)"},
		{"SELECT COT(1, 2);", true, "SELECT COT(1, 2)"},
		{"SELECT DEGREES();", true, "SELECT DEGREES()"},
		{"SELECT DEGREES(0);", true, "SELECT DEGREES(0)"},
		{"SELECT EXP();", true, "SELECT EXP()"},
		{"SELECT EXP(1);", true, "SELECT EXP(1)"},
		{"SELECT PI();", true, "SELECT PI()"},
		{"SELECT PI(1);", true, "SELECT PI(1)"},
		{"SELECT RADIANS();", true, "SELECT RADIANS()"},
		{"SELECT RADIANS(1);", true, "SELECT RADIANS(1)"},
		{"SELECT SIN();", true, "SELECT SIN()"},
		{"SELECT SIN(1);", true, "SELECT SIN(1)"},
		{"SELECT TAN(1);", true, "SELECT TAN(1)"},
		{"SELECT TAN();", true, "SELECT TAN()"},
		{"SELECT TRUNCATE(1.223,1);", true, "SELECT TRUNCATE(1.223, 1)"},
		{"SELECT TRUNCATE();", true, "SELECT TRUNCATE()"},

		{"SELECT SUBSTR('Quadratically',5);", true, "SELECT SUBSTR('Quadratically', 5)"},
		{"SELECT SUBSTR('Quadratically',5, 3);", true, "SELECT SUBSTR('Quadratically', 5, 3)"},
		{"SELECT SUBSTR('Quadratically' FROM 5);", true, "SELECT SUBSTR('Quadratically', 5)"},
		{"SELECT SUBSTR('Quadratically' FROM 5 FOR 3);", true, "SELECT SUBSTR('Quadratically', 5, 3)"},

		{"SELECT SUBSTRING('Quadratically',5);", true, "SELECT SUBSTRING('Quadratically', 5)"},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true, "SELECT SUBSTRING('Quadratically', 5, 3)"},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true, "SELECT SUBSTRING('Quadratically', 5)"},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true, "SELECT SUBSTRING('Quadratically', 5, 3)"},

		{"SELECT LEAST(), LEAST(1, 2, 3);", true, "SELECT LEAST(),LEAST(1, 2, 3)"},
		// information functions
		{"SELECT DATABASE();", true, "SELECT DATABASE()"},
		{"SELECT SCHEMA();", true, "SELECT SCHEMA()"},
		{"SELECT USER();", true, "SELECT USER()"},
		{"SELECT USER(1);", true, "SELECT USER(1)"},
		{"SELECT CURRENT_USER();", true, "SELECT CURRENT_USER()"},
		{"SELECT CURRENT_ROLE();", true, "SELECT CURRENT_ROLE()"},
		{"SELECT CURRENT_USER;", true, "SELECT CURRENT_USER()"},
		{"SELECT CONNECTION_ID();", true, "SELECT CONNECTION_ID()"},
		{"SELECT VERSION();", true, "SELECT VERSION()"},
		{"SELECT BENCHMARK(1000000, AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(1000000, AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))"},
		{"SELECT BENCHMARK(AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))"},
		{"SELECT CHARSET('abc');", true, "SELECT CHARSET('abc')"},
		{"SELECT COERCIBILITY('abc');", true, "SELECT COERCIBILITY('abc')"},
		{"SELECT COERCIBILITY('abc', 'a');", true, "SELECT COERCIBILITY('abc', 'a')"},
		{"SELECT COLLATION('abc');", true, "SELECT COLLATION('abc')"},
		{"SELECT ROW_COUNT();", true, "SELECT ROW_COUNT()"},
		{"SELECT SESSION_USER();", true, "SELECT SESSION_USER()"},
		{"SELECT SYSTEM_USER();", true, "SELECT SYSTEM_USER()"},

		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true, "SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2)"},
		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true, "SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2)"},

		{`SELECT ASCII(), ASCII(""), ASCII("A"), ASCII(1);`, true, "SELECT ASCII(),ASCII(''),ASCII('A'),ASCII(1)"},

		{`SELECT LOWER("A"), UPPER("a")`, true, "SELECT LOWER('A'),UPPER('a')"},
		{`SELECT LCASE("A"), UCASE("a")`, true, "SELECT LCASE('A'),UCASE('a')"},

		{`SELECT REPLACE('www.mysql.com', 'w', 'Ww')`, true, "SELECT REPLACE('www.mysql.com', 'w', 'Ww')"},

		{`SELECT LOCATE('bar', 'foobarbar');`, true, "SELECT LOCATE('bar', 'foobarbar')"},
		{`SELECT LOCATE('bar', 'foobarbar', 5);`, true, "SELECT LOCATE('bar', 'foobarbar', 5)"},

		{`SELECT tidb_version();`, true, "SELECT TIDB_VERSION()"},
		{`SELECT tidb_is_ddl_owner();`, true, "SELECT TIDB_IS_DDL_OWNER()"},
		{`SELECT tidb_decode_plan();`, true, "SELECT TIDB_DECODE_PLAN()"},
		{`SELECT tidb_decode_key('abc');`, true, "SELECT TIDB_DECODE_KEY('abc')"},

		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"},

		// for row
		{"select row(1)", false, ""},
		{"select row(1, 1,)", false, ""},
		{"select (1, 1,)", false, ""},
		{"select row(1, 1) > row(1, 1), row(1, 1, 1) > row(1, 1, 1)", true, "SELECT ROW(1,1)>ROW(1,1),ROW(1,1,1)>ROW(1,1,1)"},
		{"Select (1, 1) > (1, 1)", true, "SELECT ROW(1,1)>ROW(1,1)"},
		{"create table t (`row` int)", true, "CREATE TABLE `t` (`row` INT)"},
		{"create table t (row int)", false, ""},

		// for last_insert_id
		{"SELECT last_insert_id();", true, "SELECT LAST_INSERT_ID()"},
		{"SELECT last_insert_id(1);", true, "SELECT LAST_INSERT_ID(1)"},

		// for bit_count
		{`SELECT BIT_COUNT(1);`, true, "SELECT BIT_COUNT(1)"},

		// select time
		{"select current_timestamp", true, "SELECT CURRENT_TIMESTAMP()"},
		{"select current_timestamp()", true, "SELECT CURRENT_TIMESTAMP()"},
		{"select current_timestamp(6)", true, "SELECT CURRENT_TIMESTAMP(6)"},
		{"select current_timestamp(null)", false, ""},
		{"select current_timestamp(-1)", false, ""},
		{"select current_timestamp(1.0)", false, ""},
		{"select current_timestamp('2')", false, ""},
		{"select now()", true, "SELECT NOW()"},
		{"select now(6)", true, "SELECT NOW(6)"},
		{"select sysdate(), sysdate(6)", true, "SELECT SYSDATE(),SYSDATE(6)"},
		{"SELECT time('01:02:03');", true, "SELECT TIME('01:02:03')"},
		{"SELECT time('01:02:03.1')", true, "SELECT TIME('01:02:03.1')"},
		{"SELECT time('20.1')", true, "SELECT TIME('20.1')"},
		// select current_time
		{"select current_time", true, "SELECT CURRENT_TIME()"},
		{"select current_time()", true, "SELECT CURRENT_TIME()"},
		{"select current_time(6)", true, "SELECT CURRENT_TIME(6)"},
		{"select current_time(-1)", false, ""},
		{"select current_time(1.0)", false, ""},
		{"select current_time('1')", false, ""},
		{"select current_time(null)", false, ""},
		{"select curtime()", true, "SELECT CURTIME()"},
		{"select curtime(6)", true, "SELECT CURTIME(6)"},
		{"select curtime(-1)", false, ""},
		{"select curtime(1.0)", false, ""},
		{"select curtime('1')", false, ""},
		{"select curtime(null)", false, ""},

		// select utc_timestamp
		{"select utc_timestamp", true, "SELECT UTC_TIMESTAMP()"},
		{"select utc_timestamp()", true, "SELECT UTC_TIMESTAMP()"},
		{"select utc_timestamp(6)", true, "SELECT UTC_TIMESTAMP(6)"},
		{"select utc_timestamp(-1)", false, ""},
		{"select utc_timestamp(1.0)", false, ""},
		{"select utc_timestamp('1')", false, ""},
		{"select utc_timestamp(null)", false, ""},

		// select utc_time
		{"select utc_time", true, "SELECT UTC_TIME()"},
		{"select utc_time()", true, "SELECT UTC_TIME()"},
		{"select utc_time(6)", true, "SELECT UTC_TIME(6)"},
		{"select utc_time(-1)", false, ""},
		{"select utc_time(1.0)", false, ""},
		{"select utc_time('1')", false, ""},
		{"select utc_time(null)", false, ""},

		// for microsecond, second, minute, hour
		{"SELECT MICROSECOND('2009-12-31 23:59:59.000010');", true, "SELECT MICROSECOND('2009-12-31 23:59:59.000010')"},
		{"SELECT SECOND('10:05:03');", true, "SELECT SECOND('10:05:03')"},
		{"SELECT MINUTE('2008-02-03 10:05:03');", true, "SELECT MINUTE('2008-02-03 10:05:03')"},
		{"SELECT HOUR(), HOUR('10:05:03');", true, "SELECT HOUR(),HOUR('10:05:03')"},

		// for date, day, weekday
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE()", true, "SELECT CURRENT_DATE(),CURRENT_DATE(),CURDATE()"},
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE(1)", false, ""},
		{"SELECT DATEDIFF('2003-12-31', '2003-12-30');", true, "SELECT DATEDIFF('2003-12-31', '2003-12-30')"},
		{"SELECT DATE('2003-12-31 01:02:03');", true, "SELECT DATE('2003-12-31 01:02:03')"},
		{"SELECT DATE();", true, "SELECT DATE()"},
		{"SELECT DATE('2003-12-31 01:02:03', '');", true, "SELECT DATE('2003-12-31 01:02:03', '')"},
		{`SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y');`, true, "SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y')"},
		{"SELECT DAY('2007-02-03');", true, "SELECT DAY('2007-02-03')"},
		{"SELECT DAYOFMONTH('2007-02-03');", true, "SELECT DAYOFMONTH('2007-02-03')"},
		{"SELECT DAYOFWEEK('2007-02-03');", true, "SELECT DAYOFWEEK('2007-02-03')"},
		{"SELECT DAYOFYEAR('2007-02-03');", true, "SELECT DAYOFYEAR('2007-02-03')"},
		{"SELECT DAYNAME('2007-02-03');", true, "SELECT DAYNAME('2007-02-03')"},
		{"SELECT FROM_DAYS(1423);", true, "SELECT FROM_DAYS(1423)"},
		{"SELECT WEEKDAY('2007-02-03');", true, "SELECT WEEKDAY('2007-02-03')"},

		// for utc_date
		{"SELECT UTC_DATE, UTC_DATE();", true, "SELECT UTC_DATE(),UTC_DATE()"},
		{"SELECT UTC_DATE(), UTC_DATE()+0", true, "SELECT UTC_DATE(),UTC_DATE()+0"},

		// for week, month, year
		{"SELECT WEEK();", true, "SELECT WEEK()"},
		{"SELECT WEEK('2007-02-03');", true, "SELECT WEEK('2007-02-03')"},
		{"SELECT WEEK('2007-02-03', 0);", true, "SELECT WEEK('2007-02-03', 0)"},
		{"SELECT WEEKOFYEAR('2007-02-03');", true, "SELECT WEEKOFYEAR('2007-02-03')"},
		{"SELECT MONTH('2007-02-03');", true, "SELECT MONTH('2007-02-03')"},
		{"SELECT MONTHNAME('2007-02-03');", true, "SELECT MONTHNAME('2007-02-03')"},
		{"SELECT YEAR('2007-02-03');", true, "SELECT YEAR('2007-02-03')"},
		{"SELECT YEARWEEK('2007-02-03');", true, "SELECT YEARWEEK('2007-02-03')"},
		{"SELECT YEARWEEK('2007-02-03', 0);", true, "SELECT YEARWEEK('2007-02-03', 0)"},

		// for ADDTIME, SUBTIME
		{"SELECT ADDTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT ADDTIME('01:00:00.999999', '02:00:00.999998')"},
		{"SELECT ADDTIME('02:00:00.999998');", true, "SELECT ADDTIME('02:00:00.999998')"},
		{"SELECT ADDTIME();", true, "SELECT ADDTIME()"},
		{"SELECT SUBTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT SUBTIME('01:00:00.999999', '02:00:00.999998')"},

		// for CONVERT_TZ
		{"SELECT CONVERT_TZ();", true, "SELECT CONVERT_TZ()"},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00');", true, "SELECT CONVERT_TZ('2004-01-01 12:00:00', '+00:00', '+10:00')"},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00', '+10:00');", true, "SELECT CONVERT_TZ('2004-01-01 12:00:00', '+00:00', '+10:00', '+10:00')"},
		// for LOCALTIME, LOCALTIMESTAMP
		{"SELECT LOCALTIME(), LOCALTIME(1)", true, "SELECT LOCALTIME(),LOCALTIME(1)"},
		{"SELECT LOCALTIMESTAMP(), LOCALTIMESTAMP(2)", true, "SELECT LOCALTIMESTAMP(),LOCALTIMESTAMP(2)"},

		// for MAKEDATE, MAKETIME
		{"SELECT MAKEDATE(2011,31);", true, "SELECT MAKEDATE(2011, 31)"},
		{"SELECT MAKETIME(12,15,30);", true, "SELECT MAKETIME(12, 15, 30)"},
		{"SELECT MAKEDATE();", true, "SELECT MAKEDATE()"},
		{"SELECT MAKETIME();", true, "SELECT MAKETIME()"},

		// for PERIOD_ADD, PERIOD_DIFF
		{"SELECT PERIOD_ADD(200801,2)", true, "SELECT PERIOD_ADD(200801, 2)"},
		{"SELECT PERIOD_DIFF(200802,200703)", true, "SELECT PERIOD_DIFF(200802, 200703)"},

		// for QUARTER
		{"SELECT QUARTER('2008-04-01');", true, "SELECT QUARTER('2008-04-01')"},

		// for SEC_TO_TIME
		{"SELECT SEC_TO_TIME(2378)", true, "SELECT SEC_TO_TIME(2378)"},

		// for TIME_FORMAT
		{`SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')`, true, "SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')"},

		// for TIME_TO_SEC
		{"SELECT TIME_TO_SEC('22:23:00')", true, "SELECT TIME_TO_SEC('22:23:00')"},

		// for TO_DAYS, TO_SECONDS
		{"SELECT TO_DAYS('2007-10-07')", true, "SELECT TO_DAYS('2007-10-07')"},
		{"SELECT TO_SECONDS('2009-11-29')", true, "SELECT TO_SECONDS('2009-11-29')"},

		// for LAST_DAY
		{"SELECT LAST_DAY('2003-02-05');", true, "SELECT LAST_DAY('2003-02-05')"},

		// for UTC_TIME
		{"SELECT UTC_TIME(), UTC_TIME(1)", true, "SELECT UTC_TIME(),UTC_TIME(1)"},

		// for from_unixtime
		{`select from_unixtime(1447430881)`, true, "SELECT FROM_UNIXTIME(1447430881)"},
		{`select from_unixtime(1447430881.123456)`, true, "SELECT FROM_UNIXTIME(1447430881.123456)"},
		{`select from_unixtime(1447430881.1234567)`, true, "SELECT FROM_UNIXTIME(1447430881.1234567)"},
		{`select from_unixtime(1447430881.9999999)`, true, "SELECT FROM_UNIXTIME(1447430881.9999999)"},
		{`select from_unixtime(1447430881, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.123456, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.123456, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.1234567, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.1234567, '%Y %D %M %h:%i:%s %x')"},

		{`SELECT RPAD('hi', 6, 'c');`, true, "SELECT RPAD('hi', 6, 'c')"},
		{`SELECT BIT_LENGTH('hi');`, true, "SELECT BIT_LENGTH('hi')"},
		{`SELECT CHAR_LENGTH('abc');`, true, "SELECT CHAR_LENGTH('abc')"},
		{`SELECT CHARACTER_LENGTH('abc');`, true, "SELECT CHARACTER_LENGTH('abc')"},
		{`SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');`, true, "SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')"},
		{`SELECT FIND_IN_SET('foo', 'foo,bar')`, true, "SELECT FIND_IN_SET('foo', 'foo,bar')"},
		{`SELECT FIND_IN_SET('foo')`, true, "SELECT FIND_IN_SET('foo')"}, // illegal number of argument still pass
		{`SELECT MAKE_SET(1,'a'), MAKE_SET(1,'a','b','c')`, true, "SELECT MAKE_SET(1, 'a'),MAKE_SET(1, 'a', 'b', 'c')"},
		{`SELECT MID('Sakila', -5, 3)`, true, "SELECT MID('Sakila', -5, 3)"},
		{`SELECT OCT(12)`, true, "SELECT OCT(12)"},
		{`SELECT OCTET_LENGTH('text')`, true, "SELECT OCTET_LENGTH('text')"},
		{`SELECT ORD('2')`, true, "SELECT ORD('2')"},
		{`SELECT POSITION('bar' IN 'foobarbar')`, true, "SELECT POSITION('bar' IN 'foobarbar')"},
		{`SELECT QUOTE('Don\'t!')`, true, "SELECT QUOTE('Don''t!')"},
		{`SELECT BIN(12)`, true, "SELECT BIN(12)"},
		{`SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')`, true, "SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')"},
		{`SELECT EXPORT_SET(5,'Y','N'), EXPORT_SET(5,'Y','N',','), EXPORT_SET(5,'Y','N',',',4)`, true, "SELECT EXPORT_SET(5, 'Y', 'N'),EXPORT_SET(5, 'Y', 'N', ','),EXPORT_SET(5, 'Y', 'N', ',', 4)"},
		{`SELECT FORMAT(), FORMAT(12332.2,2,'de_DE'), FORMAT(12332.123456, 4)`, true, "SELECT FORMAT(),FORMAT(12332.2, 2, 'de_DE'),FORMAT(12332.123456, 4)"},
		{`SELECT FROM_BASE64('abc')`, true, "SELECT FROM_BASE64('abc')"},
		{`SELECT TO_BASE64('abc')`, true, "SELECT TO_BASE64('abc')"},
		{`SELECT LOAD_FILE('/tmp/picture')`, true, "SELECT LOAD_FILE('/tmp/picture')"},
		{`SELECT LPAD('hi',4,'??')`, true, "SELECT LPAD('hi', 4, '??')"},
		{`SELECT LEFT("foobar", 3)`, true, "SELECT LEFT('foobar', 3)"},
		{`SELECT RIGHT("foobar", 3)`, true, "SELECT RIGHT('foobar', 3)"},

		// repeat
		{`SELECT REPEAT("a", 10);`, true, "SELECT REPEAT('a', 10)"},

		// for miscellaneous functions
		{`SELECT SLEEP(10);`, true, "SELECT SLEEP(10)"},
		{`SELECT ANY_VALUE(@arg);`, true, "SELECT ANY_VALUE(@`arg`)"},
		{`SELECT INET_ATON('10.0.5.9');`, true, "SELECT INET_ATON('10.0.5.9')"},
		{`SELECT INET_NTOA(167773449);`, true, "SELECT INET_NTOA(167773449)"},
		{`SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089');`, true, "SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089')"},
		{`SELECT INET6_NTOA(INET_NTOA(167773449));`, true, "SELECT INET6_NTOA(INET_NTOA(167773449))"},
		{`SELECT IS_FREE_LOCK(@str);`, true, "SELECT IS_FREE_LOCK(@`str`)"},
		{`SELECT IS_IPV4('10.0.5.9');`, true, "SELECT IS_IPV4('10.0.5.9')"},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`, true, "SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))"},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`, true, "SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'))"},
		{`SELECT IS_IPV6('10.0.5.9');`, true, "SELECT IS_IPV6('10.0.5.9')"},
		{`SELECT IS_USED_LOCK(@str);`, true, "SELECT IS_USED_LOCK(@`str`)"},
		{`SELECT MASTER_POS_WAIT(@log_name, @log_pos), MASTER_POS_WAIT(@log_name, @log_pos, @timeout), MASTER_POS_WAIT(@log_name, @log_pos, @timeout, @channel_name);`, true, "SELECT MASTER_POS_WAIT(@`log_name`, @`log_pos`),MASTER_POS_WAIT(@`log_name`, @`log_pos`, @`timeout`),MASTER_POS_WAIT(@`log_name`, @`log_pos`, @`timeout`, @`channel_name`)"},
		{`SELECT NAME_CONST('myname', 14);`, true, "SELECT NAME_CONST('myname', 14)"},
		{`SELECT RELEASE_ALL_LOCKS();`, true, "SELECT RELEASE_ALL_LOCKS()"},
		{`SELECT UUID();`, true, "SELECT UUID()"},
		{`SELECT UUID_SHORT()`, true, "SELECT UUID_SHORT()"},
		// test illegal arguments
		{`SELECT SLEEP();`, true, "SELECT SLEEP()"},
		{`SELECT ANY_VALUE();`, true, "SELECT ANY_VALUE()"},
		{`SELECT INET_ATON();`, true, "SELECT INET_ATON()"},
		{`SELECT INET_NTOA();`, true, "SELECT INET_NTOA()"},
		{`SELECT INET6_ATON();`, true, "SELECT INET6_ATON()"},
		{`SELECT INET6_NTOA(INET_NTOA());`, true, "SELECT INET6_NTOA(INET_NTOA())"},
		{`SELECT IS_FREE_LOCK();`, true, "SELECT IS_FREE_LOCK()"},
		{`SELECT IS_IPV4();`, true, "SELECT IS_IPV4()"},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON());`, true, "SELECT IS_IPV4_COMPAT(INET6_ATON())"},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON());`, true, "SELECT IS_IPV4_MAPPED(INET6_ATON())"},
		{`SELECT IS_IPV6()`, true, "SELECT IS_IPV6()"},
		{`SELECT IS_USED_LOCK();`, true, "SELECT IS_USED_LOCK()"},
		{`SELECT MASTER_POS_WAIT();`, true, "SELECT MASTER_POS_WAIT()"},
		{`SELECT NAME_CONST();`, true, "SELECT NAME_CONST()"},
		{`SELECT RELEASE_ALL_LOCKS(1);`, true, "SELECT RELEASE_ALL_LOCKS(1)"},
		{`SELECT UUID(1);`, true, "SELECT UUID(1)"},
		{`SELECT UUID_SHORT(1)`, true, "SELECT UUID_SHORT(1)"},
		// for aggregate functions
		{`select avg(), avg(c1,c2) from t;`, false, "SELECT AVG(),AVG(`c1`, `c2`) FROM `t`"},
		{`select avg(distinct c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinctrow c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinct all c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinctrow all c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(c2) from t;`, true, "SELECT AVG(`c2`) FROM `t`"},
		{`select max(c1,c2) from t;`, false, ""},
		{`select max(distinct c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinctrow c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinct all c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinctrow all c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(c2) from t;`, true, "SELECT MAX(`c2`) FROM `t`"},
		{`select min(c1,c2) from t;`, false, ""},
		{`select min(distinct c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinctrow c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinct all c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinctrow all c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(c2) from t;`, true, "SELECT MIN(`c2`) FROM `t`"},
		{`select sum(c1,c2) from t;`, false, ""},
		{`select sum(distinct c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinctrow c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinct all c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinctrow all c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(c2) from t;`, true, "SELECT SUM(`c2`) FROM `t`"},
		{`select count(c1) from t;`, true, "SELECT COUNT(`c1`) FROM `t`"},
		{`select count(distinct *) from t;`, false, ""},
		{`select count(distinctrow *) from t;`, false, ""},
		{`select count(*) from t;`, true, "SELECT COUNT(1) FROM `t`"},
		{`select count(distinct c1, c2) from t;`, true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"},
		{`select count(distinctrow c1, c2) from t;`, true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"},
		{`select count(c1, c2) from t;`, false, ""},
		{`select count(all c1) from t;`, true, "SELECT COUNT(`c1`) FROM `t`"},
		{`select count(distinct all c1) from t;`, false, ""},
		{`select count(distinctrow all c1) from t;`, false, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIdentifier(c *C) {
	table := []testCase{
		// for quote identifier
		{"select `a`, `a.b`, `a b` from t", true, "SELECT `a`,`a.b`,`a b` FROM `t`"},
		// for unquoted identifier
		{"create table MergeContextTest$Simple (value integer not null, primary key (value))", true, "CREATE TABLE `MergeContextTest$Simple` (`value` INT NOT NULL,PRIMARY KEY(`value`))"},
		// for as
		{"select 1 as a, 1 as `a`, 1 as \"a\", 1 as 'a'", true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select 1 as a, 1 as "a", 1 as 'a'`, true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select 1 a, 1 "a", 1 'a'`, true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select * from t as "a"`, false, ""},
		{`select * from t a`, true, "SELECT * FROM `t` AS `a`"},
		// reserved keyword can't be used as identifier directly, but A.B pattern is an exception
		{`select COUNT from DESC`, false, ""},
		{`select COUNT from SELECT.DESC`, true, "SELECT `COUNT` FROM `SELECT`.`DESC`"},
		{"use `select`", true, "USE `select`"},
		{"use select", false, "USE `select`"},
		{`select * from t as a`, true, "SELECT * FROM `t` AS `a`"},
		{"select 1 full, 1 row, 1 abs", false, ""},
		{"select 1 full, 1 `row`, 1 abs", true, "SELECT 1 AS `full`,1 AS `row`,1 AS `abs`"},
		{"select * from t full, t1 row, t2 abs", false, ""},
		{"select * from t full, t1 `row`, t2 abs", true, "SELECT * FROM ((`t` AS `full`) JOIN `t1` AS `row`) JOIN `t2` AS `abs`"},
		// for issue 1878, identifiers may begin with digit.
		{"create database 123test", true, "CREATE DATABASE `123test`"},
		{"create database 123", false, "CREATE DATABASE `123`"},
		{"create database `123`", true, "CREATE DATABASE `123`"},
		{"create database `12``3`", true, "CREATE DATABASE `12``3`"},
		{"create table `123` (123a1 int)", true, "CREATE TABLE `123` (`123a1` INT)"},
		{"create table 123 (123a1 int)", false, ""},
		{fmt.Sprintf("select * from t%cble", 0), false, ""},
		// for issue 3954, should NOT be recognized as identifiers.
		{`select .78+123`, true, "SELECT 0.78+123"},
		{`select .78+.21`, true, "SELECT 0.78+0.21"},
		{`select .78-123`, true, "SELECT 0.78-123"},
		{`select .78-.21`, true, "SELECT 0.78-0.21"},
		{`select .78--123`, true, "SELECT 0.78--123"},
		{`select .78*123`, true, "SELECT 0.78*123"},
		{`select .78*.21`, true, "SELECT 0.78*0.21"},
		{`select .78/123`, true, "SELECT 0.78/123"},
		{`select .78/.21`, true, "SELECT 0.78/0.21"},
		{`select .78,123`, true, "SELECT 0.78,123"},
		{`select .78,.21`, true, "SELECT 0.78,0.21"},
		{`select .78 , 123`, true, "SELECT 0.78,123"},
		{`select .78.123`, false, ""},
		{`select .78#123`, true, "SELECT 0.78"},
		{`insert float_test values(.67, 'string');`, true, "INSERT INTO `float_test` VALUES (0.67,'string')"},
		{`select .78'123'`, true, "SELECT 0.78 AS `123`"},
		{"select .78`123`", true, "SELECT 0.78 AS `123`"},
		{`select .78"123"`, true, "SELECT 0.78 AS `123`"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestDDL(c *C) {
	table := []testCase{
		{"CREATE", false, ""},
		{"CREATE TABLE", false, ""},
		{"CREATE TABLE foo (", false, ""},
		{"CREATE TABLE foo ()", false, ""},
		{"CREATE TABLE foo ();", false, ""},
		{"CREATE TABLE foo.* (a varchar(50), b int);", false, ""},
		{"CREATE TABLE foo (a varchar(50), b int);", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true, "CREATE TABLE `foo` (`a` TINYINT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true, "CREATE TABLE `foo` (`a` BIGINT UNSIGNED,`b` TINYINT(1))"},
		{"CREATE TABLE foo (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false, ""},
		{"CREATE TABLE foo (a int, b float); CREATE TABLE bar (x double, y float)", true, "CREATE TABLE `foo` (`a` INT,`b` FLOAT); CREATE TABLE `bar` (`x` DOUBLE,`y` FLOAT)"},
		{"CREATE TABLE foo (a bytes)", false, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", false, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (name CHAR(50) BINARY);", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE foo (id varchar(50) collate utf8_bin);", true, "CREATE TABLE `foo` (`id` VARCHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET UTF8)", true, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY CHARACTER set utf8)", false, ""},
		{"CREATE TABLE foo (name CHAR(50) BINARY CHARACTER SET utf8 COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8 COLLATE utf8_bin)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 COLLATE utf8_bin COLLATE ascii_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8 COLLATE utf8_bin COLLATE ascii_bin)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)"},
		{"CREATE TABLE foo (a.b, b);", false, ""},
		{"CREATE TABLE foo (a, b.c);", false, ""},
		{"CREATE TABLE (name CHAR(50) BINARY)", false, ""},
		// for create temporary table
		{"CREATE TEMPORARY TABLE t (a varchar(50), b int);", true, "CREATE TEMPORARY TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TEMPORARY TABLE t LIKE t1", true, "CREATE TEMPORARY TABLE `t` LIKE `t1`"},
		{"DROP TEMPORARY TABLE t", true, "DROP TEMPORARY TABLE `t`"},
		// test use key word as column name
		{"CREATE TABLE foo (pump varchar(50), b int);", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (drainer varchar(50), b int);", true, "CREATE TABLE `foo` (`drainer` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (node_id varchar(50), b int);", true, "CREATE TABLE `foo` (`node_id` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (node_state varchar(50), b int);", true, "CREATE TABLE `foo` (`node_state` VARCHAR(50),`b` INT)"},

		{"create database xxx", true, "CREATE DATABASE `xxx`"},
		{"create database if exists xxx", false, ""},
		{"create database if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},

		{"create schema xxx", true, "CREATE DATABASE `xxx`"},
		{"create schema if exists xxx", false, ""},
		{"create schema if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},
		// for drop database/schema/table/view/stats
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"drop database if exists xxx", true, "DROP DATABASE IF EXISTS `xxx`"},
		{"drop database if not exists xxx", false, ""},
		{"drop schema xxx", true, "DROP DATABASE `xxx`"},
		{"drop schema if exists xxx", true, "DROP DATABASE IF EXISTS `xxx`"},
		{"drop schema if not exists xxx", false, ""},
		{"drop table", false, "DROP TABLE"},
		{"drop table xxx", true, "DROP TABLE `xxx`"},
		{"drop table xxx, yyy", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop tables xxx", true, "DROP TABLE `xxx`"},
		{"drop tables xxx, yyy", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop table if exists xxx", true, "DROP TABLE IF EXISTS `xxx`"},
		{"drop table if exists xxx, yyy", true, "DROP TABLE IF EXISTS `xxx`, `yyy`"},
		{"drop table if not exists xxx", false, ""},
		{"drop table xxx restrict", true, "DROP TABLE `xxx`"},
		{"drop table xxx, yyy cascade", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop table if exists xxx restrict", true, "DROP TABLE IF EXISTS `xxx`"},
		{`create table t (c int KEY);`, true, "CREATE TABLE `t` (`c` INT PRIMARY KEY)"},
		// for issue pingcap/parser#310
		{`CREATE TABLE t (a DECIMAL(20,0), b DECIMAL(30), c FLOAT(25,0))`, true, "CREATE TABLE `t` (`a` DECIMAL(20,0),`b` DECIMAL(30),`c` FLOAT(25,0))"},
		// Create table with multiple index options.
		{`create table t (c int, index ci (c) USING BTREE COMMENT "123");`, true, "CREATE TABLE `t` (`c` INT,INDEX `ci`(`c`) USING BTREE COMMENT '123')"},
		// for default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true, "CREATE TABLE `sbtest` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`k` INT UNSIGNED DEFAULT '0' NOT NULL,`c` CHAR(120) DEFAULT '' NOT NULL,`pad` CHAR(60) DEFAULT '' NOT NULL,PRIMARY KEY(`id`))"},
		{"create table test (create_date TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT now());", true, "CREATE TABLE `test` (`create_date` TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT CURRENT_TIMESTAMP())"},
		{"create table ts (t int, v timestamp(3) default CURRENT_TIMESTAMP(3));", true, "CREATE TABLE `ts` (`t` INT,`v` TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// Create table with primary key name.
		{"create table if not exists `t` (`id` int not null auto_increment comment '消息ID', primary key `pk_id` (`id`) );", true, "CREATE TABLE IF NOT EXISTS `t` (`id` INT NOT NULL AUTO_INCREMENT COMMENT '消息ID',PRIMARY KEY `pk_id`(`id`))"},
		// Create table with no option is valid for parser
		{"create table a", true, "CREATE TABLE `a` "},

		{"create table t (a timestamp default now)", false, ""},
		{"create table t (a timestamp default now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"},
		{"create table t (a timestamp default now() on update now)", false, ""},
		{"create table t (a timestamp default now() on update now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())"},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE t.* ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS (a SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS (a SMALLINT UNSIGNED, b varchar(255))", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED"},
		{"ALTER TABLE t DISABLE KEYS", true, "ALTER TABLE `t` DISABLE KEYS"},
		{"ALTER TABLE t ENABLE KEYS", true, "ALTER TABLE `t` ENABLE KEYS"},
		{"ALTER TABLE t MODIFY COLUMN a varchar(255)", true, "ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255)"},
		{"ALTER TABLE t MODIFY COLUMN IF EXISTS a varchar(255)", true, "ALTER TABLE `t` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255)", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN IF EXISTS a b varchar(255)", true, "ALTER TABLE `t` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		// For alter table rename column statement.
		{"ALTER TABLE t RENAME COLUMN a TO b", true, "ALTER TABLE `t` RENAME COLUMN `a` TO `b`"},

		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER TABLE t ALTER a SET DEFAULT 1", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT CURRENT_TIMESTAMP", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT NOW()", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1+1", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT (CURRENT_TIMESTAMP())", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (CURRENT_TIMESTAMP())"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT (NOW())", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (NOW())"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT (1+1)", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT (1+1)"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT (1)", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER TABLE t ALTER COLUMN a DROP DEFAULT", true, "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"},
		{"ALTER TABLE t ALTER a DROP DEFAULT", true, "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"},
		{"ALTER TABLE t ADD FULLTEXT KEY `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD FULLTEXT `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD FULLTEXT INDEX `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD INDEX (a) USING BTREE COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING BTREE COMMENT 'a'"},
		{"ALTER TABLE t ADD INDEX IF NOT EXISTS (a) USING BTREE COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
		{"ALTER TABLE t ADD INDEX (a) USING RTREE COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING RTREE COMMENT 'a'"},
		{"ALTER TABLE t ADD KEY (a) USING HASH COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING HASH COMMENT 'a'"},
		{"ALTER TABLE t ADD KEY IF NOT EXISTS (a) USING HASH COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX IF NOT EXISTS(`a`) USING HASH COMMENT 'a'"},
		{"ALTER TABLE t ADD PRIMARY KEY ident USING RTREE ( a DESC , b   )", true, "ALTER TABLE `t` ADD PRIMARY KEY `ident`(`a`, `b`) USING RTREE"},
		{"ALTER TABLE t ADD KEY USING RTREE   ( a ) ", true, "ALTER TABLE `t` ADD INDEX(`a`) USING RTREE"},
		{"ALTER TABLE t ADD KEY USING RTREE ( ident ASC , ident ( 123 ) )", true, "ALTER TABLE `t` ADD INDEX(`ident`, `ident`(123)) USING RTREE"},
		{"ALTER TABLE t ADD PRIMARY KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE INDEX (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t FORCE", true, "ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ "},
		{"ALTER TABLE t DROP INDEX;", false, "ALTER TABLE `t` DROP INDEX"},
		{"ALTER TABLE t DROP INDEX a", true, "ALTER TABLE `t` DROP INDEX `a`"},
		{"ALTER TABLE t DROP INDEX IF EXISTS a", true, "ALTER TABLE `t` DROP INDEX IF EXISTS `a`"},

		// For alter table alter index statement
		{"ALTER TABLE t ALTER INDEX a INVISIBLE", true, "ALTER TABLE `t` ALTER INDEX `a` INVISIBLE"},
		{"ALTER TABLE t ALTER INDEX a VISIBLE", true, "ALTER TABLE `t` ALTER INDEX `a` VISIBLE"},

		{"ALTER TABLE t DROP FOREIGN KEY a", true, "ALTER TABLE `t` DROP FOREIGN KEY `a`"},
		{"ALTER TABLE t DROP FOREIGN KEY IF EXISTS a", true, "ALTER TABLE `t` DROP FOREIGN KEY IF EXISTS `a`"},
		{"ALTER TABLE t DROP COLUMN a CASCADE", true, "ALTER TABLE `t` DROP COLUMN `a`"},
		{"ALTER TABLE t DROP COLUMN IF EXISTS a CASCADE", true, "ALTER TABLE `t` DROP COLUMN IF EXISTS `a`"},

		// For #6405
		{"ALTER TABLE t RENAME KEY a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},
		{"ALTER TABLE t RENAME INDEX a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},
		// For create index statement
		{"CREATE INDEX idx ON t (a)", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX IF NOT EXISTS idx ON t (a)", true, "CREATE INDEX IF NOT EXISTS `idx` ON `t` (`a`)"},
		{"CREATE UNIQUE INDEX idx ON t (a)", true, "CREATE UNIQUE INDEX `idx` ON `t` (`a`)"},
		{"CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a)", true, "CREATE UNIQUE INDEX IF NOT EXISTS `idx` ON `t` (`a`)"},
		{"CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE BTREE", true, "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE"},
		{"CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE HASH", true, "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING HASH"},
		{"CREATE UNIQUE INDEX ident ON d_n.t_n ( ident , ident ASC ) TYPE RTREE", true, "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING RTREE"},
		{"CREATE UNIQUE INDEX ident TYPE BTREE ON d_n.t_n ( ident , ident ASC )", true, "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE"},
		{"CREATE UNIQUE INDEX ident USING BTREE ON d_n.t_n ( ident , ident ASC )", true, "CREATE UNIQUE INDEX `ident` ON `d_n`.`t_n` (`ident`, `ident`) USING BTREE"},
		{"CREATE SPATIAL INDEX idx ON t (a)", true, "CREATE SPATIAL INDEX `idx` ON `t` (`a`)"},
		{"CREATE SPATIAL INDEX IF NOT EXISTS idx ON t (a)", true, "CREATE SPATIAL INDEX IF NOT EXISTS `idx` ON `t` (`a`)"},
		{"CREATE FULLTEXT INDEX idx ON t (a)", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`)"},
		{"CREATE FULLTEXT INDEX IF NOT EXISTS idx ON t (a)", true, "CREATE FULLTEXT INDEX IF NOT EXISTS `idx` ON `t` (`a`)"},
		{"CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident`"},
		{"CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident comment 'string'", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'"},
		{"CREATE FULLTEXT INDEX idx ON t (a) comment 'string' with parser ident", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'"},
		{"CREATE INDEX idx ON t (a) USING HASH", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH"},
		{"CREATE INDEX idx ON t (a) COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) COMMENT 'foo'"},
		{"CREATE INDEX idx ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx USING BTREE ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx USING BTREE ON t (a)", true, "CREATE INDEX `idx` ON `t` (`a`) USING BTREE"},
		{"CREATE INDEX idx ON t ( a ) VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE"},
		{"CREATE INDEX idx ON t ( a ) INVISIBLE VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) VISIBLE INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE"},
		{"CREATE INDEX idx ON t ( a ) USING HASH VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) USING HASH INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH INVISIBLE"},

		//For dorp index statement
		{"drop index a on t", true, "DROP INDEX `a` ON `t`"},
		{"drop index a on db.t", true, "DROP INDEX `a` ON `db`.`t`"},
		{"drop index a on db.`tb-ttb`", true, "DROP INDEX `a` ON `db`.`tb-ttb`"},
		{"drop index if exists a on t", true, "DROP INDEX IF EXISTS `a` ON `t`"},
		{"drop index if exists a on db.t", true, "DROP INDEX IF EXISTS `a` ON `db`.`t`"},
		{"drop index if exists a on db.`tb-ttb`", true, "DROP INDEX IF EXISTS `a` ON `db`.`tb-ttb`"},

		// for truncate statement
		{"TRUNCATE TABLE t1", true, "TRUNCATE TABLE `t1`"},
		{"TRUNCATE t1", true, "TRUNCATE TABLE `t1`"},

		// for empty alert table index
		{"ALTER TABLE t ADD INDEX () ", false, ""},
		{"ALTER TABLE t ADD UNIQUE ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE INDEX ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE KEY ()", false, ""},

		// for issue 4538
		{"create table a (process double)", true, "CREATE TABLE `a` (`process` DOUBLE)"},

		// for issue 4740
		{"create table t (a int1, b int2, c int3, d int4, e int8)", true, "CREATE TABLE `t` (`a` TINYINT,`b` SMALLINT,`c` MEDIUMINT,`d` INT,`e` BIGINT)"},

		// for issue 5918
		{"create table t (lv long varchar null)", true, "CREATE TABLE `t` (`lv` MEDIUMTEXT NULL)"},

		// special table name
		{"CREATE TABLE cdp_test.`test2-1` (id int(11) DEFAULT NULL,key(id));", true, "CREATE TABLE `cdp_test`.`test2-1` (`id` INT(11) DEFAULT NULL,INDEX(`id`))"},
		{"CREATE TABLE miantiao (`扁豆焖面`       INT(11));", true, "CREATE TABLE `miantiao` (`扁豆焖面` INT(11))"},

		// for generated column definition
		{"create table t (a timestamp, b timestamp as (a) not null on update current_timestamp);", false, ""},
		{"create table t (a bigint, b bigint as (a) primary key auto_increment);", false, ""},
		{"create table t (a bigint, b bigint as (a) not null default 10);", false, ""},
		{"create table t (a bigint, b bigint as (a+1) not null);", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL)"},
		{"create table t (a bigint, b bigint as (a+1) not null);", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL)"},
		{"create table t (a bigint, b bigint as (a+1) not null comment 'ttt');", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL COMMENT 'ttt')"},
		{"alter table t add column (f timestamp as (a+1) default '2019-01-01 11:11:11');", false, ""},
		{"alter table t modify column f int as (a+1) default 55;", false, ""},

		// for column format
		{"create table t (a int column_format fixed)", true, "CREATE TABLE `t` (`a` INT COLUMN_FORMAT FIXED)"},
		{"create table t (a int column_format default)", true, "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DEFAULT)"},
		{"create table t (a int column_format dynamic)", true, "CREATE TABLE `t` (`a` INT COLUMN_FORMAT DYNAMIC)"},
		{"alter table t modify column a bigint column_format default", true, "ALTER TABLE `t` MODIFY COLUMN `a` BIGINT COLUMN_FORMAT DEFAULT"},

		// for character vary syntax
		{"create table t (a character varying(1));", true, "CREATE TABLE `t` (`a` VARCHAR(1))"},
		{"create table t (a character varying(255));", true, "CREATE TABLE `t` (`a` VARCHAR(255))"},
		{"create table t (a char varying(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a varcharacter(1));", true, "CREATE TABLE `t` (`a` VARCHAR(1))"},
		{"create table t (a varcharacter(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a varcharacter(1), b varcharacter(255));", true, "CREATE TABLE `t` (`a` VARCHAR(1),`b` VARCHAR(255))"},
		{"create table t (a char);", true, "CREATE TABLE `t` (`a` CHAR)"},
		{"create table t (a character);", true, "CREATE TABLE `t` (`a` CHAR)"},
		{"create table t (a character varying(50), b int);", true, "CREATE TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		{"create table t (a character, b int);", true, "CREATE TABLE `t` (`a` CHAR,`b` INT)"},
		{"create table t (a national character varying(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a national char varying(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a national char);", true, "CREATE TABLE `t` (`a` CHAR)"},
		{"create table t (a national character);", true, "CREATE TABLE `t` (`a` CHAR)"},
		{"create table t (a nchar);", true, "CREATE TABLE `t` (`a` CHAR)"},
		{"create table t (a nchar varchar(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a nchar varcharacter(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a national varchar);", false, ""},
		{"create table t (a national varchar(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a national varcharacter(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a nchar varying(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table t (a nvarchar(50));", true, "CREATE TABLE `t` (`a` VARCHAR(50))"},
		{"create table nchar (a int);", true, "CREATE TABLE `nchar` (`a` INT)"},
		{"create table nchar (a int, b nchar);", true, "CREATE TABLE `nchar` (`a` INT,`b` CHAR)"},
		{"create table nchar (a int, b nchar(50));", true, "CREATE TABLE `nchar` (`a` INT,`b` CHAR(50))"},
		// Test keyword `SERIAL`
		{"create table t (a serial);", true, "CREATE TABLE `t` (`a` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY)"},
		{"create table t (a serial null);", true, "CREATE TABLE `t` (`a` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY NULL)"},
		{"create table t (b int, a serial);", true, "CREATE TABLE `t` (`b` INT,`a` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE KEY)"},
		{"create table t (a int serial default value);", true, "CREATE TABLE `t` (`a` INT NOT NULL AUTO_INCREMENT UNIQUE KEY)"},
		{"create table t (a int serial default value null);", true, "CREATE TABLE `t` (`a` INT NOT NULL AUTO_INCREMENT UNIQUE KEY NULL)"},
		{"create table t (a bigint serial default value);", true, "CREATE TABLE `t` (`a` BIGINT NOT NULL AUTO_INCREMENT UNIQUE KEY)"},
		{"create table t (a smallint serial default value);", true, "CREATE TABLE `t` (`a` SMALLINT NOT NULL AUTO_INCREMENT UNIQUE KEY)"},

		// for LONG syntax
		{"create table t (a long);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT)"},
		{"create table t (a long varchar);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT)"},
		{"create table t (a long varcharacter);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT)"},
		{"create table t (a long char varying);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT)"},
		{"create table t (a long character varying);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT)"},
		{"create table t (a mediumtext, b long varchar, c long, d long varcharacter, e long char varying, f long character varying, g long);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT,`b` MEDIUMTEXT,`c` MEDIUMTEXT,`d` MEDIUMTEXT,`e` MEDIUMTEXT,`f` MEDIUMTEXT,`g` MEDIUMTEXT)"},
		{"create table t (a long varbinary);", true, "CREATE TABLE `t` (`a` MEDIUMBLOB)"},
		{"create table t (a long char varying, b long varbinary);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT,`b` MEDIUMBLOB)"},
		{"create table t (a long char set utf8);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET UTF8)"},
		{"create table t (a long char varying char set utf8);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET UTF8)"},
		{"create table t (a long character set utf8);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET UTF8)"},
		{"create table t (a long character varying character set utf8);", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET UTF8)"},
		// for issue 501
		{"ALTER TABLE t IMPORT TABLESPACE;", true, "ALTER TABLE `t` IMPORT TABLESPACE"},
		{"ALTER TABLE t DISCARD TABLESPACE;", true, "ALTER TABLE `t` DISCARD TABLESPACE"},
		{"ALTER TABLE db.t IMPORT TABLESPACE;", true, "ALTER TABLE `db`.`t` IMPORT TABLESPACE"},
		{"ALTER TABLE db.t DISCARD TABLESPACE;", true, "ALTER TABLE `db`.`t` DISCARD TABLESPACE"},

		// for CONSTRAINT syntax, see issue 413
		{"ALTER TABLE t ADD ( CHECK ( true ) )", true, "ALTER TABLE `t` ADD COLUMN (CHECK(TRUE) ENFORCED)"},
		{"ALTER TABLE t ADD ( CONSTRAINT CHECK ( true ) )", true, "ALTER TABLE `t` ADD COLUMN (CHECK(TRUE) ENFORCED)"},
		{"ALTER TABLE t ADD COLUMN ( CONSTRAINT ident CHECK ( 1>2 ) NOT ENFORCED )", true, "ALTER TABLE `t` ADD COLUMN (CONSTRAINT `ident` CHECK(1>2) NOT ENFORCED)"},
		{"alter table t add column (b int, constraint c unique key (b))", true, "ALTER TABLE `t` ADD COLUMN (`b` INT, UNIQUE `c`(`b`))"},
		{"ALTER TABLE t ADD COLUMN ( CONSTRAINT CHECK ( true ) )", true, "ALTER TABLE `t` ADD COLUMN (CHECK(TRUE) ENFORCED)"},
		{"ALTER TABLE t ADD COLUMN ( CONSTRAINT CHECK ( true ) ENFORCED , CHECK ( true ) )", true, "ALTER TABLE `t` ADD COLUMN (CHECK(TRUE) ENFORCED, CHECK(TRUE) ENFORCED)"},
		{"ALTER TABLE t ADD COLUMN (a1 int, CONSTRAINT b1 CHECK (a1>0))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, CONSTRAINT `b1` CHECK(`a1`>0) ENFORCED)"},
		{"ALTER TABLE t ADD COLUMN (a1 int, a2 int, CONSTRAINT b1 CHECK (a1>0), CONSTRAINT b2 CHECK (a2<10))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, `a2` INT, CONSTRAINT `b1` CHECK(`a1`>0) ENFORCED, CONSTRAINT `b2` CHECK(`a2`<10) ENFORCED)"},
		{"ALTER TABLE `t` ADD COLUMN (`a1` INT, PRIMARY KEY (`a1`))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, PRIMARY KEY(`a1`))"},
		{"ALTER TABLE t ADD (a1 int, CONSTRAINT PRIMARY KEY (a1))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, PRIMARY KEY(`a1`))"},
		{"ALTER TABLE t ADD (a1 int, a2 int, PRIMARY KEY (a1), UNIQUE (a2))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, `a2` INT, PRIMARY KEY(`a1`), UNIQUE(`a2`))"},
		{"ALTER TABLE t ADD (a1 int, a2 int, PRIMARY KEY (a1), CONSTRAINT b2 UNIQUE (a2))", true, "ALTER TABLE `t` ADD COLUMN (`a1` INT, `a2` INT, PRIMARY KEY(`a1`), UNIQUE `b2`(`a2`))"},
		// for issue 537
		{"CREATE TABLE IF NOT EXISTS table_ident (a SQL_TSI_YEAR(4), b SQL_TSI_YEAR);", true, "CREATE TABLE IF NOT EXISTS `table_ident` (`a` YEAR(4),`b` YEAR)"},
		{`CREATE TABLE IF NOT EXISTS table_ident (ident1 BOOL COMMENT "text_string" unique, ident2 SQL_TSI_YEAR(4) ZEROFILL);`, true, "CREATE TABLE IF NOT EXISTS `table_ident` (`ident1` TINYINT(1) COMMENT 'text_string' UNIQUE KEY,`ident2` YEAR(4))"},
		{"create table t (y sql_tsi_year(4), y1 sql_tsi_year)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},
		{"create table t (y sql_tsi_year(4) unsigned zerofill zerofill, y1 sql_tsi_year signed unsigned zerofill)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},

		// for issue 529
		{"create table t (a text byte ascii)", false, ""},
		{"create table t (a text byte charset latin1)", false, ""},
		{"create table t (a longtext ascii)", true, "CREATE TABLE `t` (`a` LONGTEXT CHARACTER SET LATIN1)"},
		{"create table t (a mediumtext ascii)", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET LATIN1)"},
		{"create table t (a tinytext ascii)", true, "CREATE TABLE `t` (`a` TINYTEXT CHARACTER SET LATIN1)"},
		{"create table t (a text byte)", true, "CREATE TABLE `t` (`a` TEXT)"},
		{"create table t (a long byte, b text ascii)", true, "CREATE TABLE `t` (`a` MEDIUMTEXT,`b` TEXT CHARACTER SET LATIN1)"},
		{"create table t (a text ascii, b mediumtext ascii, c int)", true, "CREATE TABLE `t` (`a` TEXT CHARACTER SET LATIN1,`b` MEDIUMTEXT CHARACTER SET LATIN1,`c` INT)"},
		{"create table t (a int, b text ascii, c mediumtext ascii)", true, "CREATE TABLE `t` (`a` INT,`b` TEXT CHARACTER SET LATIN1,`c` MEDIUMTEXT CHARACTER SET LATIN1)"},
		{"create table t (a long ascii, b long ascii)", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET LATIN1,`b` MEDIUMTEXT CHARACTER SET LATIN1)"},
		{"create table t (a long character set utf8mb4, b long charset utf8mb4, c long char set utf8mb4)", true, "CREATE TABLE `t` (`a` MEDIUMTEXT CHARACTER SET UTF8MB4,`b` MEDIUMTEXT CHARACTER SET UTF8MB4,`c` MEDIUMTEXT CHARACTER SET UTF8MB4)"},

		{"create table t (a int STORAGE MEMORY, b varchar(255) STORAGE MEMORY)", true, "CREATE TABLE `t` (`a` INT STORAGE MEMORY,`b` VARCHAR(255) STORAGE MEMORY)"},
		{"create table t (a int storage DISK, b varchar(255) STORAGE DEFAULT)", true, "CREATE TABLE `t` (`a` INT STORAGE DISK,`b` VARCHAR(255) STORAGE DEFAULT)"},
		{"create table t (a int STORAGE DEFAULT, b varchar(255) STORAGE DISK)", true, "CREATE TABLE `t` (`a` INT STORAGE DEFAULT,`b` VARCHAR(255) STORAGE DISK)"},

		// for issue 555
		{"create table t (a fixed(6, 3), b fixed key)", true, "CREATE TABLE `t` (`a` DECIMAL(6,3),`b` DECIMAL PRIMARY KEY)"},
		{"create table t (a numeric, b fixed(6))", true, "CREATE TABLE `t` (`a` DECIMAL,`b` DECIMAL(6))"},
		{"create table t (a fixed(65, 30) zerofill, b numeric, c fixed(65) unsigned zerofill)", true, "CREATE TABLE `t` (`a` DECIMAL(65,30) UNSIGNED ZEROFILL,`b` DECIMAL,`c` DECIMAL(65) UNSIGNED ZEROFILL)"},

		// create table with expression index
		{"create table a(a int, key(lower(a)));", false, ""},
		{"create table a(a int, key(a+1));", false, ""},
		{"create table a(a int, key(a, a+1));", false, ""},
		{"create table a(a int, b int, key((a+1), (b+1)));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1), (`b`+1)))"},
		{"create table a(a int, b int, key(a, (b+1)));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX(`a`, (`b`+1)))"},
		{"create table a(a int, b int, key((a+1), b));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1), `b`))"},
		{"create table a(a int, b int, key((a + 1) desc));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1)))"},

		// for auto_random
		{"create table t (a bigint auto_random(3) primary key, b varchar(255))", true, "CREATE TABLE `t` (`a` BIGINT AUTO_RANDOM(3) PRIMARY KEY,`b` VARCHAR(255))"},
		{"create table t (a bigint auto_random primary key, b varchar(255))", true, "CREATE TABLE `t` (`a` BIGINT AUTO_RANDOM PRIMARY KEY,`b` VARCHAR(255))"},
		{"create table t (a bigint primary key auto_random(4), b varchar(255))", true, "CREATE TABLE `t` (`a` BIGINT PRIMARY KEY AUTO_RANDOM(4),`b` VARCHAR(255))"},
		{"create table t (a bigint primary key auto_random(3) primary key unique, b varchar(255))", true, "CREATE TABLE `t` (`a` BIGINT PRIMARY KEY AUTO_RANDOM(3) PRIMARY KEY UNIQUE KEY,`b` VARCHAR(255))"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestType(c *C) {
	table := []testCase{
		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"},

		// for hexadecimal
		{"select x'0a', X'11', 0x11", true, "SELECT x'0a',x'11',x'11'"},
		{"select x'13181C76734725455A'", true, "SELECT x'13181c76734725455a'"},
		{"select x'0xaa'", false, ""},
		{"select 0X11", false, ""},
		{"select 0x4920616D2061206C6F6E672068657820737472696E67", true, "SELECT x'4920616d2061206c6f6e672068657820737472696e67'"},

		// for bit
		{"select 0b01, 0b0, b'11', B'11'", true, "SELECT b'1',b'0',b'11',b'11'"},
		// 0B01 and 0b21 are identifiers, the following two statement could parse.
		// {"select 0B01", false, ""},
		// {"select 0b21", false, ""},

		// for enum and set type
		{"create table t (c1 enum('a', 'b'), c2 set('a', 'b'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))"},
		{"create table t (c1 enum)", false, ""},
		{"create table t (c1 set)", false, ""},

		// for blob and text field length
		{"create table t (c1 blob(1024), c2 text(1024))", true, "CREATE TABLE `t` (`c1` BLOB(1024),`c2` TEXT(1024))"},

		// for year
		{"create table t (y year(4), y1 year)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},
		{"create table t (y year(4) unsigned zerofill zerofill, y1 year signed unsigned zerofill)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},

		// for national
		{"create table t (c1 national char(2), c2 national varchar(2))", true, "CREATE TABLE `t` (`c1` CHAR(2),`c2` VARCHAR(2))"},

		// for json type
		{`create table t (a JSON);`, true, "CREATE TABLE `t` (`a` JSON)"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestCommentErrMsg(c *C) {
	table := []testErrMsgCase{
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", false, errors.New("near '/*' and b = 'p'' at line 1")},
		{"delete from t where a = 7 or\n 1=1/*' and b = 'p'", false, errors.New("near '/*' and b = 'p'' at line 2")},
		{"select 1/*", false, errors.New("near '/*' at line 1")},
		{"select 1/* comment */", false, nil},
	}
	s.RunErrMsgTest(c, table)
}

func (s *testParserSuite) TestSQLResult(c *C) {
	table := []testCase{
		{`select SQL_BIG_RESULT c1 from t group by c1`, true, "SELECT SQL_BIG_RESULT `c1` FROM `t` GROUP BY `c1`"},
		{`select SQL_SMALL_RESULT c1 from t group by c1`, true, "SELECT SQL_SMALL_RESULT `c1` FROM `t` GROUP BY `c1`"},
		{`select SQL_BUFFER_RESULT * from t`, true, "SELECT SQL_BUFFER_RESULT * FROM `t`"},
		{`select sql_small_result sql_big_result sql_buffer_result 1`, true, "SELECT SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT 1"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSQLNoCache(c *C) {
	table := []testCase{
		{`select SQL_NO_CACHE * from t`, false, ""},
		{`select SQL_CACHE * from t`, true, "SELECT * FROM `t`"},
		{`select * from t`, true, "SELECT * FROM `t`"},
	}

	parser := parser.New()
	for _, tt := range table {
		stmt, _, err := parser.Parse(tt.src, "", "")
		c.Assert(err, IsNil)

		sel := stmt[0].(*ast.SelectStmt)
		c.Assert(sel.SelectStmtOpts.SQLCache, Equals, tt.ok)
	}
}

func (s *testParserSuite) TestEscape(c *C) {
	table := []testCase{
		{`select """;`, false, ""},
		{`select """";`, true, "SELECT '\"'"},
		{`select "汉字";`, true, "SELECT '汉字'"},
		{`select 'abc"def';`, true, "SELECT 'abc\"def'"},
		{`select 'a\r\n';`, true, "SELECT 'a\r\n'"},
		{`select "\a\r\n"`, true, "SELECT 'a\r\n'"},
		{`select "\xFF"`, true, "SELECT 'xFF'"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestInsertStatementMemoryAllocation(c *C) {
	sql := "insert t values (1)" + strings.Repeat(",(1)", 1000)
	var oldStats, newStats runtime.MemStats
	runtime.ReadMemStats(&oldStats)
	_, err := parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	runtime.ReadMemStats(&newStats)
	c.Assert(int(newStats.TotalAlloc-oldStats.TotalAlloc), Less, 1024*500)
}

func (s *testParserSuite) TestExplain(c *C) {
	table := []testCase{
		{"explain select c1 from t1", true, "EXPLAIN FORMAT = 'row' SELECT `c1` FROM `t1`"},
		{"explain insert into t values (1), (2), (3)", true, "EXPLAIN FORMAT = 'row' INSERT INTO `t` VALUES (1),(2),(3)"},
		{"explain replace into foo values (1 || 2)", true, "EXPLAIN FORMAT = 'row' REPLACE INTO `foo` VALUES (1 OR 2)"},
		{"DESC SCHE.TABL", true, "DESC `SCHE`.`TABL`"},
		{"DESC SCHE.TABL COLUM", true, "DESC `SCHE`.`TABL` `COLUM`"},
		{"DESCRIBE SCHE.TABL COLUM", true, "DESC `SCHE`.`TABL` `COLUM`"},
		{"EXPLAIN ANALYZE SELECT 1", true, "EXPLAIN ANALYZE SELECT 1"},
		{"EXPLAIN FORMAT = 'dot' SELECT 1", true, "EXPLAIN FORMAT = 'dot' SELECT 1"},
		{"EXPLAIN FORMAT = 'row' SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		{"EXPLAIN FORMAT = 'ROW' SELECT 1", true, "EXPLAIN FORMAT = 'ROW' SELECT 1"},
		{"EXPLAIN SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		{"EXPLAIN FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		{"EXPLAIN FOR connection 42", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 42"},
		{"EXPLAIN FORMAT = 'dot' FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'dot' FOR CONNECTION 1"},
		{"EXPLAIN FORMAT = 'row' FOR connection 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		{"EXPLAIN FORMAT = TRADITIONAL FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		{"EXPLAIN FORMAT = TRADITIONAL SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		{"EXPLAIN FORMAT = JSON FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'json' FOR CONNECTION 1"},
		{"EXPLAIN FORMAT = JSON SELECT 1", true, "EXPLAIN FORMAT = 'json' SELECT 1"},
		{"EXPLAIN FORMAT = 'hint' SELECT 1", true, "EXPLAIN FORMAT = 'hint' SELECT 1"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestParseShowOpenTables(c *C) {
	table := []testCase{
		{"SHOW OPEN TABLES", true, "SHOW OPEN TABLES"},
		{"SHOW OPEN TABLES IN test", true, "SHOW OPEN TABLES IN `test`"},
		{"SHOW OPEN TABLES FROM test", true, "SHOW OPEN TABLES IN `test`"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSQLModeANSIQuotes(c *C) {
	parser := parser.New()
	parser.SetSQLMode(mysql.ModeANSIQuotes)
	tests := []string{
		`CREATE TABLE "table" ("id" int)`,
		`select * from t "tt"`,
	}
	for _, test := range tests {
		_, _, err := parser.Parse(test, "", "")
		c.Assert(err, IsNil)
	}
}

func (s *testParserSuite) TestAnalyze(c *C) {
	table := []testCase{
		{"analyze table t1", true, "ANALYZE TABLE `t1`"},
		{"analyze table t1.*", false, ""},
		{"analyze table t,t1", true, "ANALYZE TABLE `t`,`t1`"},
		{"analyze table t1 index", true, "ANALYZE TABLE `t1` INDEX"},
		{"analyze table t1 index a", true, "ANALYZE TABLE `t1` INDEX `a`"},
		{"analyze table t1 index a,b", true, "ANALYZE TABLE `t1` INDEX `a`,`b`"},
		{"analyze table t with 4 buckets", true, "ANALYZE TABLE `t` WITH 4 BUCKETS"},
		{"analyze table t with 4 topn", true, "ANALYZE TABLE `t` WITH 4 TOPN"},
		{"analyze table t with 4 cmsketch width", true, "ANALYZE TABLE `t` WITH 4 CMSKETCH WIDTH"},
		{"analyze table t with 4 cmsketch depth", true, "ANALYZE TABLE `t` WITH 4 CMSKETCH DEPTH"},
		{"analyze table t with 4 samples", true, "ANALYZE TABLE `t` WITH 4 SAMPLES"},
		{"analyze table t with 4 buckets, 4 topn, 4 cmsketch width, 4 cmsketch depth, 4 samples", true, "ANALYZE TABLE `t` WITH 4 BUCKETS, 4 TOPN, 4 CMSKETCH WIDTH, 4 CMSKETCH DEPTH, 4 SAMPLES"},
		{"analyze table t index a with 4 buckets", true, "ANALYZE TABLE `t` INDEX `a` WITH 4 BUCKETS"},
		{"analyze incremental table t index", true, "ANALYZE INCREMENTAL TABLE `t` INDEX"},
		{"analyze incremental table t index idx", true, "ANALYZE INCREMENTAL TABLE `t` INDEX `idx`"},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSetTransaction(c *C) {
	// Set transaction is equivalent to setting the global or session value of tx_isolation.
	// For example:
	// SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED
	// SET SESSION tx_isolation='READ-COMMITTED'
	tests := []struct {
		input    string
		isGlobal bool
		value    string
	}{
		{
			"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			false, "READ-COMMITTED",
		},
		{
			"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			true, "REPEATABLE-READ",
		},
	}
	parser := parser.New()
	for _, t := range tests {
		stmt1, err := parser.ParseOneStmt(t.input, "", "")
		c.Assert(err, IsNil)
		setStmt := stmt1.(*ast.SetStmt)
		vars := setStmt.Variables[0]
		c.Assert(vars.Name, Equals, "tx_isolation")
		c.Assert(vars.IsGlobal, Equals, t.isGlobal)
		c.Assert(vars.IsSystem, Equals, true)
		c.Assert(vars.Value.(ast.ValueExpr).GetValue(), Equals, t.value)
	}
}

func (s *testParserSuite) TestSideEffect(c *C) {
	// This test cover a bug that parse an error SQL doesn't leave the parser in a
	// clean state, cause the following SQL parse fail.
	parser := parser.New()
	_, err := parser.ParseOneStmt("create table t /*!50100 'abc', 'abc' */;", "", "")
	c.Assert(err, NotNil)

	_, err = parser.ParseOneStmt("show tables;", "", "")
	c.Assert(err, IsNil)
}

// See https://github.com/pingcap/tidb/parser/issue/94
func (s *testParserSuite) TestQuotedSystemVariables(c *C) {
	parser := parser.New()

	st, err := parser.ParseOneStmt(
		"select @@Sql_Mode, @@`SQL_MODE`, @@session.`sql_mode`, @@global.`s ql``mode`, @@session.'sql\\nmode', @@local.\"sql\\\"mode\";",
		"",
		"",
	)
	c.Assert(err, IsNil)
	ss := st.(*ast.SelectStmt)
	expected := []*ast.VariableExpr{
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: false,
		},
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: false,
		},
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          "s ql`mode",
			IsGlobal:      true,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          "sql\nmode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          `sql"mode`,
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
	}

	c.Assert(len(ss.Fields.Fields), Equals, len(expected))
	for i, field := range ss.Fields.Fields {
		ve := field.Expr.(*ast.VariableExpr)
		cmt := Commentf("field %d, ve = %v", i, ve)
		c.Assert(ve.Name, Equals, expected[i].Name, cmt)
		c.Assert(ve.IsGlobal, Equals, expected[i].IsGlobal, cmt)
		c.Assert(ve.IsSystem, Equals, expected[i].IsSystem, cmt)
		c.Assert(ve.ExplicitScope, Equals, expected[i].ExplicitScope, cmt)
	}
}

// See https://github.com/pingcap/tidb/parser/issue/95
func (s *testParserSuite) TestQuotedVariableColumnName(c *C) {
	parser := parser.New()

	st, err := parser.ParseOneStmt(
		"select @abc, @`abc`, @'aBc', @\"AbC\", @6, @`6`, @'6', @\"6\", @@sql_mode, @@`sql_mode`, @;",
		"",
		"",
	)
	c.Assert(err, IsNil)
	ss := st.(*ast.SelectStmt)
	expected := []string{
		"@abc",
		"@`abc`",
		"@'aBc'",
		`@"AbC"`,
		"@6",
		"@`6`",
		"@'6'",
		`@"6"`,
		"@@sql_mode",
		"@@`sql_mode`",
		"@",
	}

	c.Assert(len(ss.Fields.Fields), Equals, len(expected))
	for i, field := range ss.Fields.Fields {
		c.Assert(field.Text(), Equals, expected[i])
	}
}

// CleanNodeText set the text of node and all child node empty.
// For test only.
func CleanNodeText(node ast.Node) {
	var cleaner nodeTextCleaner
	node.Accept(&cleaner)
}

// nodeTextCleaner clean the text of a node and it's child node.
// For test only.
type nodeTextCleaner struct {
}

// Enter implements Visitor interface.
func (checker *nodeTextCleaner) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	in.SetText("")
	switch node := in.(type) {
	case *ast.CreateTableStmt:
		for _, col := range node.Cols {
			col.Tp.Charset = strings.ToUpper(col.Tp.Charset)
			col.Tp.Collate = strings.ToUpper(col.Tp.Collate)
		}
	case *ast.Constraint:
		if node.Option != nil {
			if node.Option.KeyBlockSize == 0x0 && node.Option.Tp == 0 && node.Option.Comment == "" {
				node.Option = nil
			}
		}
	case *ast.FuncCallExpr:
		node.FnName.O = strings.ToLower(node.FnName.O)
	case *ast.AggregateFuncExpr:
		node.F = strings.ToLower(node.F)
	case *ast.SelectField:
		node.Offset = 0
	case *driver.ValueExpr:
	case *ast.AlterTableStmt:
		var specs []*ast.AlterTableSpec
		for _, v := range node.Specs {
			if v.Tp != 0 && !(v.Tp == ast.AlterTableOption) {
				specs = append(specs, v)
			}
		}
		node.Specs = specs
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *nodeTextCleaner) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
