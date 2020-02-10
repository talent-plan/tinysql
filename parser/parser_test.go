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
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
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

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	_, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
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

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	st, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ss, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(len(ss.Fields.Fields), Equals, 1)
	cv, ok := ss.Fields.Fields[0].Expr.(*ast.FuncCastExpr)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, ast.CastConvertFunction)

	// for query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		st, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil)
		ss, ok = st.(*ast.SelectStmt)
		c.Assert(ok, IsTrue)
	}

	// for issue #961
	src = "create table t (c int key);"
	st, err = parser.ParseOneStmt(src, "", "")
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
		{"select * from t1 join t2 left join t3 using (id)", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` USING (`id`)"},
		{"select * from t1 right join t2 using (id) left join t3 using (id)", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` USING (`id`)) LEFT JOIN `t3` USING (`id`)"},
		{"select * from t1 right join t2 using (id) left join t3", false, ""},
		{"select * from t1 natural join t2", true, "SELECT * FROM `t1` NATURAL JOIN `t2`"},
		{"select * from t1 natural right join t2", true, "SELECT * FROM `t1` NATURAL RIGHT JOIN `t2`"},
		{"select * from t1 natural left outer join t2", true, "SELECT * FROM `t1` NATURAL LEFT JOIN `t2`"},
		{"select * from t1 natural inner join t2", false, ""},
		{"select * from t1 natural cross join t2", false, ""},

		// for straight_join
		{"select * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 left join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` LEFT JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 right join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"},

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

		// for show table regions.
		{"show table t1 regions", true, "SHOW TABLE `t1` REGIONS"},
		{"show table t1 regions where a=1", true, "SHOW TABLE `t1` REGIONS WHERE `a`=1"},
		{"show table t1", false, ""},
		{"show table t1 index idx1 regions", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS"},
		{"show table t1 index idx1 regions where a=2", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS WHERE `a`=2"},
		{"show table t1 index idx1", false, ""},
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

		{"SELECT CONVERT('111', SIGNED);", true, "SELECT CONVERT('111', SIGNED)"},

		{"SELECT LEAST(), LEAST(1, 2, 3);", true, "SELECT LEAST(),LEAST(1, 2, 3)"},

		{"SELECT INTERVAL(1, 0, 1, 2)", true, "SELECT INTERVAL(1, 0, 1, 2)"},
		{"SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY);", true, "SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)"},

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

		// for cast with charset
		{"SELECT *, CAST(data AS CHAR CHARACTER SET utf8) FROM t;", true, "SELECT *,CAST(`data` AS CHAR CHARSET UTF8) FROM `t`"},

		// for cast as JSON
		{"SELECT *, CAST(data AS JSON) FROM t;", true, "SELECT *,CAST(`data` AS JSON) FROM `t`"},

		// for cast as signed int, fix issue #3691.
		{"select cast(1 as signed int);", true, "SELECT CAST(1 AS SIGNED)"},

		// for cast as double
		{"select cast(1 as double);", true, "SELECT CAST(1 AS DOUBLE)"},

		// for cast as float
		{"select cast(1 as float);", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(0));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(24));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(25));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(53));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(54));", false, ""},

		// for cast as real
		{"select cast(1 as real);", true, "SELECT CAST(1 AS DOUBLE)"},

		// for last_insert_id
		{"SELECT last_insert_id();", true, "SELECT LAST_INSERT_ID()"},
		{"SELECT last_insert_id(1);", true, "SELECT LAST_INSERT_ID(1)"},

		// for binary operator
		{"SELECT binary 'a';", true, "SELECT BINARY 'a'"},

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
		{"SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');", true, "SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001')"},
		{"SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');", true, "SELECT TIMESTAMPDIFF(MONTH, '2003-02-01', '2003-05-01')"},
		{"SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');", true, "SELECT TIMESTAMPDIFF(YEAR, '2002-05-01', '2001-01-01')"},
		{"SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');", true, "SELECT TIMESTAMPDIFF(MINUTE, '2003-02-01', '2003-05-01 12:05:55')"},

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

		// for GET_FORMAT
		{"SELECT GET_FORMAT(DATE, 'USA');", true, "SELECT GET_FORMAT(DATE, 'USA')"},
		{"SELECT GET_FORMAT(DATETIME, 'USA');", true, "SELECT GET_FORMAT(DATETIME, 'USA')"},
		{"SELECT GET_FORMAT(TIME, 'USA');", true, "SELECT GET_FORMAT(TIME, 'USA')"},
		{"SELECT GET_FORMAT(TIMESTAMP, 'USA');", true, "SELECT GET_FORMAT(DATETIME, 'USA')"},

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

		// for TIMESTAMPADD
		{"SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(WEEK, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_SECOND,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(SECOND, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_MINUTE,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(MINUTE, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_HOUR,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(HOUR, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_DAY,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(DAY, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_WEEK,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(WEEK, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_MONTH,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(MONTH, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_QUARTER,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(QUARTER, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_YEAR,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(YEAR, 1, '2003-01-02')"},
		{"SELECT TIMESTAMPADD(SQL_TSI_MICROSECOND,1,'2003-01-02');", false, ""},
		{"SELECT TIMESTAMPADD(MICROSECOND,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(MICROSECOND, 1, '2003-01-02')"},

		// for TO_DAYS, TO_SECONDS
		{"SELECT TO_DAYS('2007-10-07')", true, "SELECT TO_DAYS('2007-10-07')"},
		{"SELECT TO_SECONDS('2009-11-29')", true, "SELECT TO_SECONDS('2009-11-29')"},

		// for LAST_DAY
		{"SELECT LAST_DAY('2003-02-05');", true, "SELECT LAST_DAY('2003-02-05')"},

		// for UTC_TIME
		{"SELECT UTC_TIME(), UTC_TIME(1)", true, "SELECT UTC_TIME(),UTC_TIME(1)"},

		// for time extract
		{`select extract(microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(week from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(WEEK FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(month from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MONTH FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(quarter from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(QUARTER FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(year from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(YEAR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(second_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(SECOND_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_hour from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_HOUR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(year_month from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(YEAR_MONTH FROM '2011-11-11 10:10:10.123456')"},

		// for from_unixtime
		{`select from_unixtime(1447430881)`, true, "SELECT FROM_UNIXTIME(1447430881)"},
		{`select from_unixtime(1447430881.123456)`, true, "SELECT FROM_UNIXTIME(1447430881.123456)"},
		{`select from_unixtime(1447430881.1234567)`, true, "SELECT FROM_UNIXTIME(1447430881.1234567)"},
		{`select from_unixtime(1447430881.9999999)`, true, "SELECT FROM_UNIXTIME(1447430881.9999999)"},
		{`select from_unixtime(1447430881, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.123456, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.123456, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.1234567, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.1234567, '%Y %D %M %h:%i:%s %x')"},

		// for issue 224
		{`SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin;`, true, "SELECT CAST('test collated returns' AS CHAR CHARSET UTF8)"},

		// for string functions
		// trim
		{`SELECT TRIM('  bar   ');`, true, "SELECT TRIM('  bar   ')"},
		{`SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');`, true, "SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx')"},
		{`SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');`, true, "SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx')"},
		{`SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');`, true, "SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz')"},
		{`SELECT LTRIM(' foo ');`, true, "SELECT LTRIM(' foo ')"},
		{`SELECT RTRIM(' bar ');`, true, "SELECT RTRIM(' bar ')"},

		{`SELECT RPAD('hi', 6, 'c');`, true, "SELECT RPAD('hi', 6, 'c')"},
		{`SELECT BIT_LENGTH('hi');`, true, "SELECT BIT_LENGTH('hi')"},
		{`SELECT CHAR(65);`, true, "SELECT CHAR_FUNC(65, NULL)"},
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
		{`SELECT INSERT(), INSERT('Quadratic', 3, 4, 'What'), INSTR('foobarbar', 'bar')`, true, "SELECT INSERT_FUNC(),INSERT_FUNC('Quadratic', 3, 4, 'What'),INSTR('foobarbar', 'bar')"},
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
		// interval
		{`select "2011-11-11 10:10:10.123456" + interval 10 second`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select "2011-11-11 10:10:10.123456" - interval 10 second`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		// for date_add
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select date_add("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		{`select date_add("2011-11-11 10:10:10.123456", interval 10 sql_tsi_microsecond)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 sql_tsi_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 sql_tsi_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 sql_tsi_hour)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 sql_tsi_day)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 sql_tsi_week)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 sql_tsi_month)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 sql_tsi_quarter)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 sql_tsi_year)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},

		// for strcmp
		{`select strcmp('abc', 'def')`, true, "SELECT STRCMP('abc', 'def')"},

		// for adddate
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select adddate("2011-11-11 10:10:10.123456", 10)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", 0.10)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", "11,11")`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11,11' DAY)"},

		// for date_sub
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select date_sub("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		// for subdate
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select subdate("2011-11-11 10:10:10.123456", 10)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", 0.10)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", "11,11")`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11,11' DAY)"},

		// for unix_timestamp
		{`select unix_timestamp()`, true, "SELECT UNIX_TIMESTAMP()"},
		{`select unix_timestamp('2015-11-13 10:20:19.012')`, true, "SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012')"},

		// for misc functions
		{`SELECT GET_LOCK('lock1',10);`, true, "SELECT GET_LOCK('lock1', 10)"},
		{`SELECT RELEASE_LOCK('lock1');`, true, "SELECT RELEASE_LOCK('lock1')"},

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

		// for encryption and compression functions
		{`select AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3'))`, true, "SELECT AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3'))"},
		{`select AES_DECRYPT(@crypt_str,@key_str)`, true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`)"},
		{`select AES_DECRYPT(@crypt_str,@key_str,@init_vector);`, true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`, @`init_vector`)"},
		{`SELECT COMPRESS('');`, true, "SELECT COMPRESS('')"},
		{`SELECT DECODE(@crypt_str, @pass_str);`, true, "SELECT DECODE(@`crypt_str`, @`pass_str`)"},
		{`SELECT DES_DECRYPT(@crypt_str), DES_DECRYPT(@crypt_str, @key_str);`, true, "SELECT DES_DECRYPT(@`crypt_str`),DES_DECRYPT(@`crypt_str`, @`key_str`)"},
		{`SELECT DES_ENCRYPT(@str), DES_ENCRYPT(@key_num);`, true, "SELECT DES_ENCRYPT(@`str`),DES_ENCRYPT(@`key_num`)"},
		{`SELECT ENCODE('cleartext', CONCAT('my_random_salt','my_secret_password'));`, true, "SELECT ENCODE('cleartext', CONCAT('my_random_salt', 'my_secret_password'))"},
		{`SELECT ENCRYPT('hello'), ENCRYPT('hello', @salt);`, true, "SELECT ENCRYPT('hello'),ENCRYPT('hello', @`salt`)"},
		{`SELECT MD5('testing');`, true, "SELECT MD5('testing')"},
		{`SELECT OLD_PASSWORD(@str);`, true, "SELECT OLD_PASSWORD(@`str`)"},
		{`SELECT PASSWORD(@str);`, true, "SELECT PASSWORD_FUNC(@`str`)"},
		{`SELECT RANDOM_BYTES(@len);`, true, "SELECT RANDOM_BYTES(@`len`)"},
		{`SELECT SHA1('abc');`, true, "SELECT SHA1('abc')"},
		{`SELECT SHA('abc');`, true, "SELECT SHA('abc')"},
		{`SELECT SHA2('abc', 224);`, true, "SELECT SHA2('abc', 224)"},
		{`SELECT UNCOMPRESS('any string');`, true, "SELECT UNCOMPRESS('any string')"},
		{`SELECT UNCOMPRESSED_LENGTH(@compressed_string);`, true, "SELECT UNCOMPRESSED_LENGTH(@`compressed_string`)"},
		{`SELECT VALIDATE_PASSWORD_STRENGTH(@str);`, true, "SELECT VALIDATE_PASSWORD_STRENGTH(@`str`)"},

		// For JSON functions.
		{`SELECT JSON_EXTRACT();`, true, "SELECT JSON_EXTRACT()"},
		{`SELECT JSON_UNQUOTE();`, true, "SELECT JSON_UNQUOTE()"},
		{`SELECT JSON_TYPE('[123]');`, true, "SELECT JSON_TYPE('[123]')"},
		{`SELECT JSON_TYPE();`, true, "SELECT JSON_TYPE()"},

		// For two json grammar sugar.
		{`SELECT a->'$.a' FROM t`, true, "SELECT JSON_EXTRACT(`a`, '$.a') FROM `t`"},
		{`SELECT a->>'$.a' FROM t`, true, "SELECT JSON_UNQUOTE(JSON_EXTRACT(`a`, '$.a')) FROM `t`"},
		{`SELECT '{}'->'$.a' FROM t`, false, ""},
		{`SELECT '{}'->>'$.a' FROM t`, false, ""},
		{`SELECT a->3 FROM t`, false, ""},
		{`SELECT a->>3 FROM t`, false, ""},

		// Test that quoted identifier can be a function name.
		{"SELECT `uuid`()", true, "SELECT UUID()"},
	}
	s.RunTest(c, table)

	// Test in REAL_AS_FLOAT SQL mode.
	table2 := []testCase{
		// for cast as float
		{"select cast(1 as float);", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(0));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(24));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(25));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(53));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(54));", false, ""},

		// for cast as real
		{"select cast(1 as real);", true, "SELECT CAST(1 AS FLOAT)"},
	}
	s.RunTestInRealAsFloatMode(c, table2)
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
		// for table option
		{"create table t (c int) avg_row_length = 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table t (c int) avg_row_length 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table t (c int) checksum = 0", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 0"},
		{"create table t (c int) checksum 1", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 1"},
		{"create table t (c int) table_checksum = 0", true, "CREATE TABLE `t` (`c` INT) TABLE_CHECKSUM = 0"},
		{"create table t (c int) table_checksum 1", true, "CREATE TABLE `t` (`c` INT) TABLE_CHECKSUM = 1"},
		{"create table t (c int) compression = 'NONE'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'NONE'"},
		{"create table t (c int) compression 'lz4'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'lz4'"},
		{"create table t (c int) connection = 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table t (c int) connection 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table t (c int) key_block_size = 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table t (c int) key_block_size 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table t (c int) max_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table t (c int) max_rows 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table t (c int) min_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table t (c int) min_rows 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table t (c int) password = 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table t (c int) password 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table t (c int) DELAY_KEY_WRITE=1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table t (c int) DELAY_KEY_WRITE 1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table t (c int) ROW_FORMAT = default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table t (c int) ROW_FORMAT default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table t (c int) ROW_FORMAT = fixed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = FIXED"},
		{"create table t (c int) ROW_FORMAT = compressed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPRESSED"},
		{"create table t (c int) ROW_FORMAT = compact", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPACT"},
		{"create table t (c int) ROW_FORMAT = redundant", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = REDUNDANT"},
		{"create table t (c int) ROW_FORMAT = dynamic", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DYNAMIC"},
		{"create table t (c int) STATS_PERSISTENT = default", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) STATS_PERSISTENT = 0", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) STATS_PERSISTENT = 1", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) STATS_SAMPLE_PAGES 0", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 0"},
		{"create table t (c int) STATS_SAMPLE_PAGES 10", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 10"},
		{"create table t (c int) STATS_SAMPLE_PAGES = 10", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = 10"},
		{"create table t (c int) STATS_SAMPLE_PAGES = default", true, "CREATE TABLE `t` (`c` INT) STATS_SAMPLE_PAGES = DEFAULT"},
		{"create table t (c int) PACK_KEYS = 1", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table t (c int) PACK_KEYS = 0", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table t (c int) PACK_KEYS = DEFAULT", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table t (c int) STORAGE DISK", true, "CREATE TABLE `t` (`c` INT) STORAGE DISK"},
		{"create table t (c int) STORAGE MEMORY", true, "CREATE TABLE `t` (`c` INT) STORAGE MEMORY"},
		{"create table t (c int) SECONDARY_ENGINE null", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = NULL"},
		{"create table t (c int) SECONDARY_ENGINE = innodb", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = 'innodb'"},
		{"create table t (c int) SECONDARY_ENGINE 'null'", true, "CREATE TABLE `t` (`c` INT) SECONDARY_ENGINE = 'null'"},
		{`create table testTableCompression (c VARCHAR(15000)) compression="ZLIB";`, true, "CREATE TABLE `testTableCompression` (`c` VARCHAR(15000)) COMPRESSION = 'ZLIB'"},
		{`create table t1 (c1 int) compression="zlib";`, true, "CREATE TABLE `t1` (`c1` INT) COMPRESSION = 'zlib'"},

		// for check clause
		{"create table t (c1 bool, c2 bool, check (c1 in (0, 1)) not enforced, check (c2 in (0, 1)))", true, "CREATE TABLE `t` (`c1` TINYINT(1),`c2` TINYINT(1),CHECK(`c1` IN (0,1)) NOT ENFORCED,CHECK(`c2` IN (0,1)) ENFORCED)"},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED,`First_Name` VARCHAR(30))"},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0) not enforced, SS varchar(30) check(ss='test') enforced);", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) NOT ENFORCED,`SS` VARCHAR(30) CHECK(`ss`='test') ENFORCED)"},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0) not null, First_Name varchar(30) comment 'string' not null);", true, "CREATE TABLE `Customer` (`SD` INT CHECK(`SD`>0) ENFORCED NOT NULL,`First_Name` VARCHAR(30) COMMENT 'string' NOT NULL)"},
		{"CREATE TABLE Customer (SD integer comment 'string' CHECK (SD > 0) not null);", true, "CREATE TABLE `Customer` (`SD` INT COMMENT 'string' CHECK(`SD`>0) ENFORCED NOT NULL)"},
		{"CREATE TABLE Customer (SD integer comment 'string' not enforced, First_Name varchar(30));", false, ""},
		{"CREATE TABLE Customer (SD integer not enforced, First_Name varchar(30));", false, ""},

		{"create database xxx", true, "CREATE DATABASE `xxx`"},
		{"create database if exists xxx", false, ""},
		{"create database if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},

		// for create database with encryption
		{"create database xxx encryption = 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"create database xxx encryption 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"create database xxx default encryption = 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"create database xxx default encryption 'N'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'N'"},
		{"create database xxx encryption = 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx encryption 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx default encryption = 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx default encryption 'Y'", true, "CREATE DATABASE `xxx` ENCRYPTION = 'Y'"},
		{"create database xxx encryption = N", false, ""},

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
		// for issue 974
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET UTF8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		// for issue 975
		{`CREATE TABLE test_data (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(255) NOT NULL,
		amount decimal(19,2) DEFAULT NULL,
		charge_id varchar(32) DEFAULT NULL,
		paid_amount decimal(19,2) DEFAULT NULL,
		transaction_no varchar(64) DEFAULT NULL,
		wx_mp_app_id varchar(32) DEFAULT NULL,
		contacts varchar(50) DEFAULT NULL,
		deliver_fee decimal(19,2) DEFAULT NULL,
		deliver_info varchar(255) DEFAULT NULL,
		deliver_time varchar(255) DEFAULT NULL,
		description varchar(255) DEFAULT NULL,
		invoice varchar(255) DEFAULT NULL,
		order_from int(11) DEFAULT NULL,
		order_state int(11) NOT NULL,
		packing_fee decimal(19,2) DEFAULT NULL,
		payment_time datetime DEFAULT NULL,
		payment_type int(11) DEFAULT NULL,
		phone varchar(50) NOT NULL,
		store_employee_id bigint(20) DEFAULT NULL,
		store_id bigint(20) NOT NULL,
		user_id bigint(20) NOT NULL,
		payment_mode int(11) NOT NULL,
		current_latitude double NOT NULL,
		current_longitude double NOT NULL,
		address_latitude double NOT NULL,
		address_longitude double NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT food_order_ibfk_1 FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		CONSTRAINT food_order_ibfk_2 FOREIGN KEY (store_id) REFERENCES waimaiqa.store (id),
		CONSTRAINT food_order_ibfk_3 FOREIGN KEY (store_employee_id) REFERENCES waimaiqa.store_employee (id),
		UNIQUE FK_UNIQUE_charge_id USING BTREE (charge_id) comment '',
		INDEX FK_eqst2x1xisn3o0wbrlahnnqq8 USING BTREE (store_employee_id) comment '',
		INDEX FK_8jcmec4kb03f4dod0uqwm54o9 USING BTREE (store_id) comment '',
		INDEX FK_a3t0m9apja9jmrn60uab30pqd USING BTREE (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `test_data` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(255) NOT NULL,`amount` DECIMAL(19,2) DEFAULT NULL,`charge_id` VARCHAR(32) DEFAULT NULL,`paid_amount` DECIMAL(19,2) DEFAULT NULL,`transaction_no` VARCHAR(64) DEFAULT NULL,`wx_mp_app_id` VARCHAR(32) DEFAULT NULL,`contacts` VARCHAR(50) DEFAULT NULL,`deliver_fee` DECIMAL(19,2) DEFAULT NULL,`deliver_info` VARCHAR(255) DEFAULT NULL,`deliver_time` VARCHAR(255) DEFAULT NULL,`description` VARCHAR(255) DEFAULT NULL,`invoice` VARCHAR(255) DEFAULT NULL,`order_from` INT(11) DEFAULT NULL,`order_state` INT(11) NOT NULL,`packing_fee` DECIMAL(19,2) DEFAULT NULL,`payment_time` DATETIME DEFAULT NULL,`payment_type` INT(11) DEFAULT NULL,`phone` VARCHAR(50) NOT NULL,`store_employee_id` BIGINT(20) DEFAULT NULL,`store_id` BIGINT(20) NOT NULL,`user_id` BIGINT(20) NOT NULL,`payment_mode` INT(11) NOT NULL,`current_latitude` DOUBLE NOT NULL,`current_longitude` DOUBLE NOT NULL,`address_latitude` DOUBLE NOT NULL,`address_longitude` DOUBLE NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `food_order_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),CONSTRAINT `food_order_ibfk_2` FOREIGN KEY (`store_id`) REFERENCES `waimaiqa`.`store`(`id`),CONSTRAINT `food_order_ibfk_3` FOREIGN KEY (`store_employee_id`) REFERENCES `waimaiqa`.`store_employee`(`id`),UNIQUE `FK_UNIQUE_charge_id`(`charge_id`) USING BTREE,INDEX `FK_eqst2x1xisn3o0wbrlahnnqq8`(`store_employee_id`) USING BTREE,INDEX `FK_8jcmec4kb03f4dod0uqwm54o9`(`store_id`) USING BTREE,INDEX `FK_a3t0m9apja9jmrn60uab30pqd`(`user_id`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 95 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{`create table t (c int KEY);`, true, "CREATE TABLE `t` (`c` INT PRIMARY KEY)"},
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{"CREATE TABLE address (\r\nid bigint(20) NOT NULL AUTO_INCREMENT,\r\ncreate_at datetime NOT NULL,\r\ndeleted tinyint(1) NOT NULL,\r\nupdate_at datetime NOT NULL,\r\nversion bigint(20) DEFAULT NULL,\r\naddress varchar(128) NOT NULL,\r\naddress_detail varchar(128) NOT NULL,\r\ncellphone varchar(16) NOT NULL,\r\nlatitude double NOT NULL,\r\nlongitude double NOT NULL,\r\nname varchar(16) NOT NULL,\r\nsex tinyint(1) NOT NULL,\r\nuser_id bigint(20) NOT NULL,\r\nPRIMARY KEY (id),\r\nCONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,\r\nINDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''\r\n) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;", true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
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
		// Create table with like.
		{"create table a like b", true, "CREATE TABLE `a` LIKE `b`"},
		{"create table a (id int REFERENCES a (id) ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) ON delete set null on update CASCADE)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE SET NULL ON UPDATE CASCADE)"},
		{"create table a (id int REFERENCES a (id) ON update set default on delete RESTRICT)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE RESTRICT ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) MATCH FULL ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH FULL ON DELETE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) MATCH PARTIAL ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH PARTIAL ON UPDATE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) MATCH SIMPLE ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH SIMPLE ON UPDATE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) ON update set default on update CURRENT_TIMESTAMP)", false, ""},
		{"create table a (id int REFERENCES a (id) ON delete set default on update CURRENT_TIMESTAMP)", false, ""},
		{"create table a (like b)", true, "CREATE TABLE `a` LIKE `b`"},
		{"create table if not exists a like b", true, "CREATE TABLE IF NOT EXISTS `a` LIKE `b`"},
		{"create table if not exists a (like b)", true, "CREATE TABLE IF NOT EXISTS `a` LIKE `b`"},
		{"create table if not exists a like (b)", false, ""},
		{"create table a (t int) like b", false, ""},
		{"create table a (t int) like (b)", false, ""},
		// Create table with no option is valid for parser
		{"create table a", true, "CREATE TABLE `a` "},

		{"create table t (a timestamp default now)", false, ""},
		{"create table t (a timestamp default now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"},
		{"create table t (a timestamp default now() on update now)", false, ""},
		{"create table t (a timestamp default now() on update now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())"},
		{"CREATE TABLE t (c TEXT) default CHARACTER SET utf8, default COLLATE utf8_general_ci;", true, "CREATE TABLE `t` (`c` TEXT) DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"CREATE TABLE t (c TEXT) shard_row_id_bits = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1"},
		{"CREATE TABLE t (c TEXT) shard_row_id_bits = 1, PRE_SPLIT_REGIONS = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1 PRE_SPLIT_REGIONS = 1"},
		// Create table with ON UPDATE CURRENT_TIMESTAMP(6), specify fraction part.
		{"CREATE TABLE IF NOT EXISTS `general_log` (`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` mediumtext NOT NULL,`thread_id` bigint(20) unsigned NOT NULL,`server_id` int(10) unsigned NOT NULL,`command_type` varchar(64) NOT NULL,`argument` mediumblob NOT NULL) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'", true, "CREATE TABLE IF NOT EXISTS `general_log` (`event_time` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` MEDIUMTEXT NOT NULL,`thread_id` BIGINT(20) UNSIGNED NOT NULL,`server_id` INT(10) UNSIGNED NOT NULL,`command_type` VARCHAR(64) NOT NULL,`argument` MEDIUMBLOB NOT NULL) ENGINE = CSV DEFAULT CHARACTER SET = UTF8 COMMENT = 'General log'"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// For reference_definition in column_definition.
		{"CREATE TABLE followers ( f1 int NOT NULL REFERENCES user_profiles (uid) );", true, "CREATE TABLE `followers` (`f1` INT NOT NULL REFERENCES `user_profiles`(`uid`))"},

		// For table option `ENCRYPTION`
		{"create table t (a int) encryption = 'n';", true, "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'n'"},
		{"create table t (a int) encryption 'n';", true, "CREATE TABLE `t` (`a` INT) ENCRYPTION = 'n'"},
		{"alter table t encryption = 'y';", true, "ALTER TABLE `t` ENCRYPTION = 'y'"},
		{"alter table t encryption 'y';", true, "ALTER TABLE `t` ENCRYPTION = 'y'"},

		// for alter database/schema/table
		{"ALTER DATABASE t CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		{"ALTER DATABASE CHARACTER SET = 'utf8'", true, "ALTER DATABASE CHARACTER SET = utf8"},
		{"ALTER DATABASE t DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		{"ALTER SCHEMA t DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		{"ALTER SCHEMA DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE CHARACTER SET = utf8"},
		{"ALTER SCHEMA t DEFAULT CHARSET = 'UTF8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},

		{"ALTER DATABASE t COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		{"ALTER DATABASE COLLATE = 'utf8_bin'", true, "ALTER DATABASE COLLATE = utf8_bin"},
		{"ALTER DATABASE t DEFAULT COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		{"ALTER SCHEMA t DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		{"ALTER SCHEMA DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE COLLATE = utf8_bin"},
		{"ALTER SCHEMA `` DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE `` COLLATE = utf8_bin"},

		{"ALTER DATABASE t CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` CHARACTER SET = utf8mb4 COLLATE = utf8_bin"},
		{
			"ALTER DATABASE t DEFAULT CHARSET = 'utf8mb4' DEFAULT COLLATE = 'utf8mb4_general_ci' CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
			true,
			"ALTER DATABASE `t` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci CHARACTER SET = utf8 COLLATE = utf8mb4_bin",
		},
		{"ALTER DATABASE DEFAULT CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'", true, "ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8_bin"},
		{
			"ALTER DATABASE DEFAULT CHARSET = 'utf8mb4' DEFAULT COLLATE = 'utf8mb4_general_ci' CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
			true,
			"ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci CHARACTER SET = utf8 COLLATE = utf8mb4_bin",
		},

		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE t.* ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS (a SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS (a SMALLINT UNSIGNED, b varchar(255))", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED FIRST)", false, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED FIRST", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED FIRST"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED AFTER `b`"},
		{"ALTER TABLE t ADD COLUMN IF NOT EXISTS a SMALLINT UNSIGNED AFTER b", true, "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `a` SMALLINT UNSIGNED AFTER `b`"},
		{"ALTER TABLE t DISABLE KEYS", true, "ALTER TABLE `t` DISABLE KEYS"},
		{"ALTER TABLE t ENABLE KEYS", true, "ALTER TABLE `t` ENABLE KEYS"},
		{"ALTER TABLE t MODIFY COLUMN a varchar(255)", true, "ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255)"},
		{"ALTER TABLE t MODIFY COLUMN IF EXISTS a varchar(255)", true, "ALTER TABLE `t` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255)", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN IF EXISTS a b varchar(255)", true, "ALTER TABLE `t` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) CHARACTER SET UTF8 BINARY", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) BINARY CHARACTER SET UTF8"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) FIRST", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) FIRST"},

		// For alter table rename statement.
		{"ALTER TABLE db.t RENAME to db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE db.t RENAME db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE db.t RENAME = db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE db.t RENAME as db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE t RENAME to t1", true, "ALTER TABLE `t` RENAME AS `t1`"},
		{"ALTER TABLE t RENAME t1", true, "ALTER TABLE `t` RENAME AS `t1`"},
		{"ALTER TABLE t RENAME = t1", true, "ALTER TABLE `t` RENAME AS `t1`"},
		{"ALTER TABLE t RENAME as t1", true, "ALTER TABLE `t` RENAME AS `t1`"},

		// For #499, alter table order by
		{"ALTER TABLE t_n ORDER BY ident", true, "ALTER TABLE `t_n` ORDER BY `ident`"},
		{"ALTER TABLE t_n ORDER BY ident ASC", true, "ALTER TABLE `t_n` ORDER BY `ident`"},
		{"ALTER TABLE t_n ORDER BY ident DESC", true, "ALTER TABLE `t_n` ORDER BY `ident` DESC"},
		{"ALTER TABLE t_n ORDER BY ident1, ident2", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`"},
		{"ALTER TABLE t_n ORDER BY ident1 ASC, ident2", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`"},
		{"ALTER TABLE t_n ORDER BY ident1 ASC, ident2 ASC", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`"},
		{"ALTER TABLE t_n ORDER BY ident1 ASC, ident2 DESC", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2` DESC"},
		{"ALTER TABLE t_n ORDER BY ident1 DESC, ident2", true, "ALTER TABLE `t_n` ORDER BY `ident1` DESC,`ident2`"},
		{"ALTER TABLE t_n ORDER BY ident1 DESC, ident2 ASC", true, "ALTER TABLE `t_n` ORDER BY `ident1` DESC,`ident2`"},
		{"ALTER TABLE t_n ORDER BY ident1 DESC, ident2 DESC", true, "ALTER TABLE `t_n` ORDER BY `ident1` DESC,`ident2` DESC"},
		{"ALTER TABLE t_n ORDER BY ident1, ident2, ident3", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`,`ident3`"},
		{"ALTER TABLE t_n ORDER BY ident1, ident2, ident3 ASC", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`,`ident3`"},
		{"ALTER TABLE t_n ORDER BY ident1, ident2, ident3 DESC", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`,`ident3` DESC"},
		{"ALTER TABLE t_n ORDER BY ident1 ASC, ident2 ASC, ident3 ASC", true, "ALTER TABLE `t_n` ORDER BY `ident1`,`ident2`,`ident3`"},
		{"ALTER TABLE t_n ORDER BY ident1 DESC, ident2 DESC, ident3 DESC", true, "ALTER TABLE `t_n` ORDER BY `ident1` DESC,`ident2` DESC,`ident3` DESC"},

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
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=none", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = NONE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=default", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=shared", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = SHARED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=exclusive", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = EXCLUSIVE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock none", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = NONE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock default", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock shared", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = SHARED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock exclusive", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = EXCLUSIVE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=NONE", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = NONE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=DEFAULT", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=SHARED", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = SHARED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=EXCLUSIVE", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = EXCLUSIVE"},
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
		{"ALTER TABLE t ADD CONSTRAINT fk_t2_id FOREIGN KEY (t2_id) REFERENCES t(id)", true, "ALTER TABLE `t` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY (`t2_id`) REFERENCES `t`(`id`)"},
		{"ALTER TABLE t ADD CONSTRAINT fk_t2_id FOREIGN KEY IF NOT EXISTS (t2_id) REFERENCES t(id)", true, "ALTER TABLE `t` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t`(`id`)"},
		{"ALTER TABLE t ADD CONSTRAINT c_1 CHECK (1+1) NOT ENFORCED, ADD UNIQUE (a)", true, "ALTER TABLE `t` ADD CONSTRAINT `c_1` CHECK(1+1) NOT ENFORCED, ADD UNIQUE(`a`)"},
		{"ALTER TABLE t ADD CONSTRAINT c_1 CHECK (1+1) ENFORCED, ADD UNIQUE (a)", true, "ALTER TABLE `t` ADD CONSTRAINT `c_1` CHECK(1+1) ENFORCED, ADD UNIQUE(`a`)"},
		{"ALTER TABLE t ADD CONSTRAINT c_1 CHECK (1+1), ADD UNIQUE (a)", true, "ALTER TABLE `t` ADD CONSTRAINT `c_1` CHECK(1+1) ENFORCED, ADD UNIQUE(`a`)"},
		{"ALTER TABLE t ENGINE ''", true, "ALTER TABLE `t` ENGINE = ''"},
		{"ALTER TABLE t ENGINE = ''", true, "ALTER TABLE `t` ENGINE = ''"},
		{"ALTER TABLE t ENGINE = 'innodb'", true, "ALTER TABLE `t` ENGINE = innodb"},
		{"ALTER TABLE t ENGINE = innodb", true, "ALTER TABLE `t` ENGINE = innodb"},
		{"ALTER TABLE `db`.`t` ENGINE = ``", true, "ALTER TABLE `db`.`t` ENGINE = ''"},
		{"ALTER TABLE t INSERT_METHOD = FIRST", true, "ALTER TABLE `t` INSERT_METHOD = 'FIRST'"},
		{"ALTER TABLE t INSERT_METHOD LAST", true, "ALTER TABLE `t` INSERT_METHOD = 'LAST'"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT, ENGINE = '', default COLLATE = UTF8_GENERAL_CI", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT, ENGINE = '', DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"ALTER TABLE t ENGINE = '', COMMENT='', default COLLATE = UTF8_GENERAL_CI", true, "ALTER TABLE `t` ENGINE = '', COMMENT = '', DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"ALTER TABLE t ENGINE = '', ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` ENGINE = '', ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t default COLLATE = UTF8_GENERAL_CI, ENGINE = '', ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` DEFAULT COLLATE = UTF8_GENERAL_CI, ENGINE = '', ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t shard_row_id_bits = 1", true, "ALTER TABLE `t` SHARD_ROW_ID_BITS = 1"},
		{"ALTER TABLE t AUTO_INCREMENT 3", true, "ALTER TABLE `t` AUTO_INCREMENT = 3"},
		{"ALTER TABLE t AUTO_INCREMENT = 3", true, "ALTER TABLE `t` AUTO_INCREMENT = 3"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = DEFAULT;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = DEFAULT"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = INPLACE;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INPLACE"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = COPY;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = COPY"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL, ALGORITHM = INSTANT;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INSTANT"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET UTF8;", true, "ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"},
		{"ALTER TABLE t CONVERT TO CHARSET UTF8;", true, "ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN;", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},
		{"ALTER TABLE t CONVERT TO CHARSET UTF8 COLLATE UTF8_BIN;", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},

		// alter table convert to character set default, issue #498
		{"alter table d_n.t_n convert to character set default", true, "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT"},
		{"alter table d_n.t_n convert to charset default", true, "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT"},
		{"alter table d_n.t_n convert to char set default", true, "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT"},
		{"alter table d_n.t_n convert to character set default collate utf8mb4_0900_ai_ci", true, "ALTER TABLE `d_n`.`t_n` CONVERT TO CHARACTER SET DEFAULT COLLATE UTF8MB4_0900_AI_CI"},

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
		{`ALTER TABLE testTableCompression COMPRESSION="LZ4";`, true, "ALTER TABLE `testTableCompression` COMPRESSION = 'LZ4'"},
		{`ALTER TABLE t1 COMPRESSION="zlib";`, true, "ALTER TABLE `t1` COMPRESSION = 'zlib'"},
		{"ALTER TABLE t1", true, "ALTER TABLE `t1`"},
		{"ALTER TABLE t1 ,", false, ""},

		// For #6405
		{"ALTER TABLE t RENAME KEY a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},
		{"ALTER TABLE t RENAME INDEX a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},

		// For #497, support `ALTER TABLE ALTER CHECK` and `ALTER TABLE DROP CHECK` syntax
		{"ALTER TABLE d_n.t_n DROP CHECK ident;", true, "ALTER TABLE `d_n`.`t_n` DROP CHECK `ident`"},
		{"ALTER TABLE t_n LOCK = DEFAULT , DROP CHECK ident;", true, "ALTER TABLE `t_n` LOCK = DEFAULT, DROP CHECK `ident`"},
		{"ALTER TABLE t_n ALTER CHECK ident ENFORCED;", true, "ALTER TABLE `t_n` ALTER CHECK `ident` ENFORCED"},
		{"ALTER TABLE t_n ALTER CHECK ident NOT ENFORCED;", true, "ALTER TABLE `t_n` ALTER CHECK `ident` NOT ENFORCED"},

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
		{"CREATE FULLTEXT INDEX idx ON t (a) WITH PARSER ident comment 'string' lock default", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`) WITH PARSER `ident` COMMENT 'string'"},
		{"CREATE INDEX idx ON t (a) USING HASH", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH"},
		{"CREATE INDEX idx ON t (a) COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) COMMENT 'foo'"},
		{"CREATE INDEX idx ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx ON t (a) LOCK=NONE", true, "CREATE INDEX `idx` ON `t` (`a`) LOCK = NONE"},
		{"CREATE INDEX idx USING BTREE ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx USING BTREE ON t (a)", true, "CREATE INDEX `idx` ON `t` (`a`) USING BTREE"},
		{"CREATE INDEX idx ON t ( a ) VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE"},
		{"CREATE INDEX idx ON t ( a ) INVISIBLE VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) VISIBLE INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) INVISIBLE"},
		{"CREATE INDEX idx ON t ( a ) USING HASH VISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH VISIBLE"},
		{"CREATE INDEX idx ON t ( a ) USING HASH INVISIBLE", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH INVISIBLE"},

		// For create index with algorithm
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = DEFAULT", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM DEFAULT", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = INPLACE", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM INPLACE", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = COPY", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = COPY"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM COPY", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = COPY"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = DEFAULT LOCK = DEFAULT", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx ON t ( a ) LOCK = DEFAULT ALGORITHM = DEFAULT", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = INPLACE LOCK = EXCLUSIVE", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE"},
		{"CREATE INDEX idx ON t ( a ) LOCK = EXCLUSIVE ALGORITHM = INPLACE", true, "CREATE INDEX `idx` ON `t` (`a`) ALGORITHM = INPLACE LOCK = EXCLUSIVE"},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM = ident", false, ""},
		{"CREATE INDEX idx ON t ( a ) ALGORITHM ident", false, ""},

		//For dorp index statement
		{"drop index a on t", true, "DROP INDEX `a` ON `t`"},
		{"drop index a on db.t", true, "DROP INDEX `a` ON `db`.`t`"},
		{"drop index a on db.`tb-ttb`", true, "DROP INDEX `a` ON `db`.`tb-ttb`"},
		{"drop index if exists a on t", true, "DROP INDEX IF EXISTS `a` ON `t`"},
		{"drop index if exists a on db.t", true, "DROP INDEX IF EXISTS `a` ON `db`.`t`"},
		{"drop index if exists a on db.`tb-ttb`", true, "DROP INDEX IF EXISTS `a` ON `db`.`tb-ttb`"},
		{"drop index idx on t algorithm = default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t algorithm default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t algorithm = inplace", true, "DROP INDEX `idx` ON `t` ALGORITHM = INPLACE"},
		{"drop index idx on t algorithm inplace", true, "DROP INDEX `idx` ON `t` ALGORITHM = INPLACE"},
		{"drop index idx on t lock = default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t lock default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t lock = shared", true, "DROP INDEX `idx` ON `t` LOCK = SHARED"},
		{"drop index idx on t lock shared", true, "DROP INDEX `idx` ON `t` LOCK = SHARED"},
		{"drop index idx on t algorithm = default lock = default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t lock = default algorithm = default", true, "DROP INDEX `idx` ON `t`"},
		{"drop index idx on t algorithm = inplace lock = exclusive", true, "DROP INDEX `idx` ON `t` ALGORITHM = INPLACE LOCK = EXCLUSIVE"},
		{"drop index idx on t lock = exclusive algorithm = inplace", true, "DROP INDEX `idx` ON `t` ALGORITHM = INPLACE LOCK = EXCLUSIVE"},
		{"drop index idx on t algorithm = algorithm_type", false, ""},
		{"drop index idx on t algorithm algorithm_type", false, ""},
		{"drop index idx on t lock = lock_type", false, ""},
		{"drop index idx on t lock lock_type", false, ""},

		// for rename table statement
		{"RENAME TABLE t TO t1", true, "RENAME TABLE `t` TO `t1`"},
		{"RENAME TABLE t t1", false, "RENAME TABLE `t` TO `t1`"},
		{"RENAME TABLE d.t TO d1.t1", true, "RENAME TABLE `d`.`t` TO `d1`.`t1`"},
		{"RENAME TABLE t1 TO t2, t3 TO t4", true, "RENAME TABLE `t1` TO `t2`, `t3` TO `t4`"},

		// for truncate statement
		{"TRUNCATE TABLE t1", true, "TRUNCATE TABLE `t1`"},
		{"TRUNCATE t1", true, "TRUNCATE TABLE `t1`"},

		// for empty alert table index
		{"ALTER TABLE t ADD INDEX () ", false, ""},
		{"ALTER TABLE t ADD UNIQUE ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE INDEX ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE KEY ()", false, ""},

		// for keyword `SECONDARY_LOAD`, `SECONDARY_UNLOAD`
		{"ALTER TABLE d_n.t_n SECONDARY_LOAD", true, "ALTER TABLE `d_n`.`t_n` SECONDARY_LOAD"},
		{"ALTER TABLE d_n.t_n SECONDARY_UNLOAD", true, "ALTER TABLE `d_n`.`t_n` SECONDARY_UNLOAD"},
		{"ALTER TABLE t_n LOCK = DEFAULT , SECONDARY_LOAD", true, "ALTER TABLE `t_n` LOCK = DEFAULT, SECONDARY_LOAD"},
		{"ALTER TABLE d_n.t_n ALGORITHM = DEFAULT , SECONDARY_LOAD", true, "ALTER TABLE `d_n`.`t_n` ALGORITHM = DEFAULT, SECONDARY_LOAD"},
		{"ALTER TABLE d_n.t_n ALGORITHM = DEFAULT , SECONDARY_UNLOAD", true, "ALTER TABLE `d_n`.`t_n` ALGORITHM = DEFAULT, SECONDARY_UNLOAD"},

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

		// for references without IndexColNameList
		{"alter table t add column a double (4,2) zerofill references b match full on update set null first", true, "ALTER TABLE `t` ADD COLUMN `a` DOUBLE(4,2) UNSIGNED ZEROFILL REFERENCES `b` MATCH FULL ON UPDATE SET NULL FIRST"},
		{"alter table d_n.t_n add constraint foreign key ident (ident(1)) references d_n.t_n match full on delete set null", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`(1)) REFERENCES `d_n`.`t_n` MATCH FULL ON DELETE SET NULL"},
		{"alter table t_n add constraint ident foreign key (ident,ident(1)) references t_n match full on update set null on delete restrict", true, "ALTER TABLE `t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`, `ident`(1)) REFERENCES `t_n` MATCH FULL ON DELETE RESTRICT ON UPDATE SET NULL"},
		{"alter table d_n.t_n add constraint foreign key (ident asc) references d_n.t_n match simple on update cascade on delete cascade", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT FOREIGN KEY (`ident`) REFERENCES `d_n`.`t_n` MATCH SIMPLE ON DELETE CASCADE ON UPDATE CASCADE"},

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
		{"alter table t_n storage disk , modify ident national varcharacter(12) column_format fixed first;", true, "ALTER TABLE `t_n` STORAGE DISK, MODIFY COLUMN `ident` VARCHAR(12) COLUMN_FORMAT FIXED FIRST"},

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
		{"alter table t_n change column ident ident long char varying binary charset utf8 first , tablespace ident", true, "ALTER TABLE `t_n` CHANGE COLUMN `ident` `ident` MEDIUMTEXT BINARY CHARACTER SET UTF8 FIRST, TABLESPACE = `ident`"},
		{"alter table t_n change column ident ident long character varying binary charset utf8 first , tablespace ident", true, "ALTER TABLE `t_n` CHANGE COLUMN `ident` `ident` MEDIUMTEXT BINARY CHARACTER SET UTF8 FIRST, TABLESPACE = `ident`"},

		// for STATS_AUTO_RECALC syntax
		{"create table t (a int) stats_auto_recalc 2;", false, ""},
		{"create table t (a int) stats_auto_recalc = 10;", false, ""},
		{"create table t (a int) stats_auto_recalc 0;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = 0"},
		{"create table t (a int) stats_auto_recalc default;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = DEFAULT"},
		{"create table t (a int) stats_auto_recalc = 0;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = 0"},
		{"create table t (a int) stats_auto_recalc = 1;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = 1"},
		{"create table t (a int) stats_auto_recalc=default;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = DEFAULT"},
		{"create table t (a int) stats_persistent = 1, stats_auto_recalc = 1;", true, "CREATE TABLE `t` (`a` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */  STATS_AUTO_RECALC = 1"},
		{"create table t (a int) stats_auto_recalc = 1, stats_sample_pages = 25;", true, "CREATE TABLE `t` (`a` INT) STATS_AUTO_RECALC = 1 STATS_SAMPLE_PAGES = 25"},
		{"alter table t modify a bigint, ENGINE=InnoDB, stats_auto_recalc = 0", true, "ALTER TABLE `t` MODIFY COLUMN `a` BIGINT, ENGINE = InnoDB, STATS_AUTO_RECALC = 0"},
		{"create table stats_auto_recalc (a int);", true, "CREATE TABLE `stats_auto_recalc` (`a` INT)"},
		{"create table stats_auto_recalc (a int) stats_auto_recalc=1;", true, "CREATE TABLE `stats_auto_recalc` (`a` INT) STATS_AUTO_RECALC = 1"},

		// for TYPE/USING syntax
		{"create table t (a int, primary key type type btree (a));", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY `type`(`a`) USING BTREE)"},
		{"create table t (a int, primary key type btree (a));", false, ""},
		{"create table t (a int, primary key using btree (a));", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) USING BTREE)"},
		{"create table t (a int, primary key (a) type btree);", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) USING BTREE)"},
		{"create table t (a int, primary key (a) using btree);", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) USING BTREE)"},
		{"create table t (a int, unique index type type btree (a));", true, "CREATE TABLE `t` (`a` INT,UNIQUE `type`(`a`) USING BTREE)"},
		{"create table t (a int, unique index type using btree (a));", true, "CREATE TABLE `t` (`a` INT,UNIQUE `type`(`a`) USING BTREE)"},
		{"create table t (a int, unique index type btree (a));", false, ""},
		{"create table t (a int, unique index using btree (a));", true, "CREATE TABLE `t` (`a` INT,UNIQUE(`a`) USING BTREE)"},
		{"create table t (a int, unique index (a) using btree);", true, "CREATE TABLE `t` (`a` INT,UNIQUE(`a`) USING BTREE)"},
		{"create table t (a int, unique key (a) using btree);", true, "CREATE TABLE `t` (`a` INT,UNIQUE(`a`) USING BTREE)"},
		{"create table t (a int, index type type btree (a));", true, "CREATE TABLE `t` (`a` INT,INDEX `type`(`a`) USING BTREE)"},
		{"create table t (a int, index type btree (a));", false, ""},
		{"create table t (a int, index type using btree (a));", true, "CREATE TABLE `t` (`a` INT,INDEX `type`(`a`) USING BTREE)"},
		{"create table t (a int, index using btree (a));", true, "CREATE TABLE `t` (`a` INT,INDEX(`a`) USING BTREE)"},

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
		{"ALTER TABLE ident ADD ( CONSTRAINT FOREIGN KEY ident ( EXECUTE ( 123 ) ) REFERENCES t ( a ) MATCH SIMPLE ON DELETE CASCADE ON UPDATE SET NULL )", true, "ALTER TABLE `ident` ADD COLUMN (CONSTRAINT `ident` FOREIGN KEY (`EXECUTE`(123)) REFERENCES `t`(`a`) MATCH SIMPLE ON DELETE CASCADE ON UPDATE SET NULL)"},
		// for CONSTRAINT cont'd, the following tests are for another aspect of the incompatibility
		{"ALTER TABLE t ADD COLUMN a DATE CHECK ( a > 0 ) FIRST", true, "ALTER TABLE `t` ADD COLUMN `a` DATE CHECK(`a`>0) ENFORCED FIRST"},
		{"ALTER TABLE t ADD a1 int CONSTRAINT ident CHECK ( a1 > 1 ) REFERENCES b ON DELETE CASCADE ON UPDATE CASCADE;", true, "ALTER TABLE `t` ADD COLUMN `a1` INT CHECK(`a1`>1) ENFORCED REFERENCES `b` ON DELETE CASCADE ON UPDATE CASCADE"},
		{"ALTER TABLE t ADD COLUMN a DATE CONSTRAINT CHECK ( a > 0 ) FIRST", true, "ALTER TABLE `t` ADD COLUMN `a` DATE CHECK(`a`>0) ENFORCED FIRST"},
		{"ALTER TABLE t ADD a TINYBLOB CONSTRAINT ident CHECK ( 1>2 ) REFERENCES b ON DELETE CASCADE ON UPDATE CASCADE", true, "ALTER TABLE `t` ADD COLUMN `a` TINYBLOB CHECK(1>2) ENFORCED REFERENCES `b` ON DELETE CASCADE ON UPDATE CASCADE"},
		{"ALTER TABLE t ADD a2 int CONSTRAINT ident CHECK (a2 > 1) ENFORCED", true, "ALTER TABLE `t` ADD COLUMN `a2` INT CHECK(`a2`>1) ENFORCED"},
		{"ALTER TABLE t ADD a2 int CONSTRAINT ident CHECK (a2 > 1) NOT ENFORCED", true, "ALTER TABLE `t` ADD COLUMN `a2` INT CHECK(`a2`>1) NOT ENFORCED"},
		{"ALTER TABLE t ADD a2 int CONSTRAINT ident primary key REFERENCES b ON DELETE CASCADE ON UPDATE CASCADE;", false, ""},
		{"ALTER TABLE t ADD a2 int CONSTRAINT ident primary key (a2))", false, ""},
		{"ALTER TABLE t ADD a2 int CONSTRAINT ident unique key (a2))", false, ""},

		{"ALTER TABLE t SET TIFLASH REPLICA 2 LOCATION LABELS 'a','b'", true, "ALTER TABLE `t` SET TIFLASH REPLICA 2 LOCATION LABELS 'a', 'b'"},
		{"ALTER TABLE t SET TIFLASH REPLICA 0", true, "ALTER TABLE `t` SET TIFLASH REPLICA 0"},

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

func (s *testParserSuite) TestComment(c *C) {
	table := []testCase{
		{"create table t (c int comment 'comment')", true, "CREATE TABLE `t` (`c` INT COMMENT 'comment')"},
		{"create table t (c int) comment = 'comment'", true, "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'"},
		{"create table t (c int) comment 'comment'", true, "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'"},
		{"create table t (c int) comment comment", false, ""},
		{"create table t (comment text)", true, "CREATE TABLE `t` (`comment` TEXT)"},
		// for comment in query
		{"/*comment*/ /*comment*/ select c /* this is a comment */ from t;", true, "SELECT `c` FROM `t`"},
		// for unclosed comment
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", false, ""},

		{"create table t (ssl int)", false, ""},
		{"create table t (require int)", false, ""},
		{"create table t (account int)", true, "CREATE TABLE `t` (`account` INT)"},
		{"create table t (expire int)", true, "CREATE TABLE `t` (`expire` INT)"},
		{"create table t (cipher int)", true, "CREATE TABLE `t` (`cipher` INT)"},
		{"create table t (issuer int)", true, "CREATE TABLE `t` (`issuer` INT)"},
		{"create table t (never int)", true, "CREATE TABLE `t` (`never` INT)"},
		{"create table t (subject int)", true, "CREATE TABLE `t` (`subject` INT)"},
		{"create table t (x509 int)", true, "CREATE TABLE `t` (`x509` INT)"},
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

func (s *testParserSuite) TestLikeEscape(c *C) {
	table := []testCase{
		// for like escape
		{`select "abc_" like "abc\\_" escape ''`, true, "SELECT 'abc_' LIKE 'abc\\_'"},
		{`select "abc_" like "abc\\_" escape '\\'`, true, "SELECT 'abc_' LIKE 'abc\\_'"},
		{`select "abc_" like "abc\\_" escape '||'`, false, ""},
		{`select "abc" like "escape" escape '+'`, true, "SELECT 'abc' LIKE 'escape' ESCAPE '+'"},
		{"select '''_' like '''_' escape ''''", true, "SELECT '''_' LIKE '''_' ESCAPE ''''"},
	}

	s.RunTest(c, table)
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

func (s *testParserSuite) TestTimestampDiffUnit(c *C) {
	// Test case for timestampdiff unit.
	// TimeUnit should be unified to upper case.
	parser := parser.New()
	stmt, _, err := parser.Parse("SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(month,'2003-02-01','2003-05-01');", "", "")
	c.Assert(err, IsNil)
	ss := stmt[0].(*ast.SelectStmt)
	fields := ss.Fields.Fields
	c.Assert(len(fields), Equals, 2)
	expr := fields[0].Expr
	f, ok := expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(*ast.TimeUnitExpr).Unit, Equals, ast.TimeUnitMonth)

	expr = fields[1].Expr
	f, ok = expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(*ast.TimeUnitExpr).Unit, Equals, ast.TimeUnitMonth)

	// Test Illegal TimeUnit for TimestampDiff
	table := []testCase{
		{"SELECT TIMESTAMPDIFF(SECOND_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_HOUR,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(YEAR_MONTH,'2003-02-01','2003-05-01')", false, ""},
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

func (s *testParserSuite) TestDDLStatements(c *C) {
	parser := parser.New()
	// Tests that whatever the charset it is define, we always assign utf8 charset and utf8_bin collate.
	createTableStr := `CREATE TABLE t (
		a varchar(64) binary,
		b char(10) charset utf8 collate utf8_general_ci,
		c text charset latin1) ENGINE=innoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`
	stmts, _, err := parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt := stmts[0].(*ast.CreateTableStmt)
	c.Assert(mysql.HasBinaryFlag(stmt.Cols[0].Tp.Flag), IsTrue)
	for _, colDef := range stmt.Cols[1:] {
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsFalse)
	}
	for _, tblOpt := range stmt.Options {
		switch tblOpt.Tp {
		case ast.TableOptionCharset:
			c.Assert(tblOpt.StrValue, Equals, "utf8")
		case ast.TableOptionCollate:
			c.Assert(tblOpt.StrValue, Equals, "utf8_bin")
		}
	}
	createTableStr = `CREATE TABLE t (
		a varbinary(64),
		b binary(10),
		c blob)`
	stmts, _, err = parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt = stmts[0].(*ast.CreateTableStmt)
	for _, colDef := range stmt.Cols {
		c.Assert(colDef.Tp.Charset, Equals, charset.CharsetBin)
		c.Assert(colDef.Tp.Collate, Equals, charset.CollationBin)
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsTrue)
	}
	// Test set collate for all column types
	createTableStr = `CREATE TABLE t (
		c_int int collate utf8_bin,
		c_real real collate utf8_bin,
		c_float float collate utf8_bin,
		c_bool bool collate utf8_bin,
		c_char char collate utf8_bin,
		c_binary binary collate utf8_bin,
		c_varchar varchar(2) collate utf8_bin,
		c_year year collate utf8_bin,
		c_date date collate utf8_bin,
		c_time time collate utf8_bin,
		c_datetime datetime collate utf8_bin,
		c_timestamp timestamp collate utf8_bin,
		c_tinyblob tinyblob collate utf8_bin,
		c_blob blob collate utf8_bin,
		c_mediumblob mediumblob collate utf8_bin,
		c_longblob longblob collate utf8_bin,
		c_bit bit collate utf8_bin,
		c_long_varchar long varchar collate utf8_bin,
		c_tinytext tinytext collate utf8_bin,
		c_text text collate utf8_bin,
		c_mediumtext mediumtext collate utf8_bin,
		c_longtext longtext collate utf8_bin,
		c_decimal decimal collate utf8_bin,
		c_numeric numeric collate utf8_bin,
		c_enum enum('1') collate utf8_bin,
		c_set set('1') collate utf8_bin,
		c_json json collate utf8_bin)`
	_, _, err = parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
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

func (s *testParserSuite) TestCharset(c *C) {
	parser := parser.New()

	st, err := parser.ParseOneStmt("ALTER SCHEMA GLOBAL DEFAULT CHAR SET utf8mb4", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.AlterDatabaseStmt), NotNil)
	st, err = parser.ParseOneStmt("ALTER DATABASE CHAR SET = utf8mb4", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.AlterDatabaseStmt), NotNil)
	st, err = parser.ParseOneStmt("ALTER DATABASE DEFAULT CHAR SET = utf8mb4", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.AlterDatabaseStmt), NotNil)
}

func (s *testParserSuite) TestFulltextSearch(c *C) {
	parser := parser.New()

	st, err := parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search')", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH() AGAINST('search')", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST()", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN)", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN BOOLEAN MODE WITH QUERY EXPANSION)", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN NATURAL LANGUAGE MODE)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer := bytes.NewBufferString("")
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\")")

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN BOOLEAN MODE)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer.Reset()
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\" IN BOOLEAN MODE)")

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' WITH QUERY EXPANSION)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer.Reset()
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\" WITH QUERY EXPANSION)")
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
		for _, opt := range node.Options {
			switch opt.Tp {
			case ast.TableOptionCharset:
				opt.StrValue = strings.ToUpper(opt.StrValue)
			case ast.TableOptionCollate:
				opt.StrValue = strings.ToUpper(opt.StrValue)
			}
		}
		for _, col := range node.Cols {
			col.Tp.Charset = strings.ToUpper(col.Tp.Charset)
			col.Tp.Collate = strings.ToUpper(col.Tp.Collate)

			for i, option := range col.Options {
				if option.Tp == 0 && option.Expr == nil && option.Stored == false && option.Refer == nil {
					col.Options = append(col.Options[:i], col.Options[i+1:]...)
				}
			}
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
			if v.Tp != 0 && !(v.Tp == ast.AlterTableOption && len(v.Options) == 0) {
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
