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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
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

func (s *testIntegrationSuite) TestFuncREPEAT(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS table_string;")
	tk.MustExec("CREATE TABLE table_string(a CHAR(20), b VARCHAR(20), c TINYTEXT, d TEXT(20), e MEDIUMTEXT, f LONGTEXT, g BIGINT);")
	tk.MustExec("INSERT INTO table_string (a, b, c, d, e, f, g) VALUES ('a', 'b', 'c', 'd', 'e', 'f', 2);")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("SELECT REPEAT(a, g), REPEAT(b, g), REPEAT(c, g), REPEAT(d, g), REPEAT(e, g), REPEAT(f, g) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, NULL), REPEAT(b, NULL), REPEAT(c, NULL), REPEAT(d, NULL), REPEAT(e, NULL), REPEAT(f, NULL) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, 2), REPEAT(b, 2), REPEAT(c, 2), REPEAT(d, 2), REPEAT(e, 2), REPEAT(f, 2) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, -1), REPEAT(b, -2), REPEAT(c, -2), REPEAT(d, -2), REPEAT(e, -2), REPEAT(f, -2) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 0), REPEAT(b, 0), REPEAT(c, 0), REPEAT(d, 0), REPEAT(e, 0), REPEAT(f, 0) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 16777217), REPEAT(b, 16777217), REPEAT(c, 16777217), REPEAT(d, 16777217), REPEAT(e, 16777217), REPEAT(f, 16777217) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestFuncLpadAndRpad(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec(`USE test;`)
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(10), b CHAR(10));`)
	tk.MustExec(`INSERT INTO t SELECT "中文", "abc";`)
	result := tk.MustQuery(`SELECT LPAD(a, 11, "a"), LPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("a中文\x00\x00\x00\x00 ab"))
	result = tk.MustQuery(`SELECT RPAD(a, 11, "a"), RPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("中文\x00\x00\x00\x00a ab"))
	result = tk.MustQuery(`SELECT LPAD("中文", 5, "字符"), LPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("字符字中文 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", 5, "字符"), RPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("中文字符字 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", -5, "字符"), RPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`SELECT LPAD("中文", -5, "字符"), LPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
}

func (s *testIntegrationSuite) TestConvertToBit(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a varchar(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a binary(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a datetime)")
	tk.MustExec(`insert t1 value ('09-01-01')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("20090101000000"))
}

func (s *testIntegrationSuite2) TestStringBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()
	var err error

	// for length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select length(a), length(b), length(c), length(d), length(e), length(f), length(null) from t")
	result.Check(testkit.Rows("1 3 19 8 6 2 <nil>"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20))")
	tk.MustExec(`insert into t values("tidb  "), (concat("a  ", "b  "))`)
	result = tk.MustQuery("select a, length(a) from t")
	result.Check(testkit.Rows("tidb 4", "a  b 4"))

	// for concat
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat(a, b, c, d, e) from t")
	result.Check(testkit.Rows("11.12017-01-01 12:01:0112:01:01abcdef"))
	result = tk.MustQuery("select concat(null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("drop table if exists t")
	// Fix issue 9123
	tk.MustExec("create table t(a char(32) not null, b float default '0') engine=innodb default charset=utf8mb4")
	tk.MustExec("insert into t value('0a6f9d012f98467f8e671e9870044528', 208.867)")
	result = tk.MustQuery("select concat_ws( ',', b) from t where a = '0a6f9d012f98467f8e671e9870044528';")
	result.Check(testkit.Rows("208.867"))

	// for concat_ws
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat_ws('|', a, b, c, d, e) from t")
	result.Check(testkit.Rows("1|1.1|2017-01-01 12:01:01|12:01:01|abcdef"))
	result = tk.MustQuery("select concat_ws(null, null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(',', 'a', 'b')")
	result.Check(testkit.Rows("a,b"))
	result = tk.MustQuery("select concat_ws(',','First name',NULL,'Last Name')")
	result.Check(testkit.Rows("First name,Last Name"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a tinyint(2), b varchar(10));`)
	tk.MustExec(`insert into t values (1, 'a'), (12, 'a'), (126, 'a'), (127, 'a')`)
	tk.MustQuery(`select concat_ws('#', a, b) from t;`).Check(testkit.Rows(
		`1#a`,
		`12#a`,
		`126#a`,
		`127#a`,
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(3))")
	tk.MustExec("insert into t values('a')")
	result = tk.MustQuery(`select concat_ws(',', a, 'test') = 'a\0\0,test' from t`)
	result.Check(testkit.Rows("1"))

	// for ascii
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010)`)
	result = tk.MustQuery("select ascii(a), ascii(b), ascii(c), ascii(d), ascii(e), ascii(f) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10"))
	result = tk.MustQuery("select ascii('123'), ascii(123), ascii(''), ascii('你好'), ascii(NULL)")
	result.Check(testkit.Rows("49 49 0 228 <nil>"))

	// for lower
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f binary(3), g binary(3))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 'aa', 'BB')`)
	result = tk.MustQuery("select lower(a), lower(b), lower(c), lower(d), lower(e), lower(f), lower(g), lower(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 abcdef aa\x00 BB\x00 <nil>"))

	// for upper
	result = tk.MustQuery("select upper(a), upper(b), upper(c), upper(d), upper(e), upper(f), upper(g), upper(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 ABCDEF aa\x00 BB\x00 <nil>"))

	// for strcmp
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values("123", 123, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select strcmp(a, "123"), strcmp(b, "123"), strcmp(c, "12.34"), strcmp(d, "2017-01-01 12:01:01"), strcmp(e, "12:01:01") from t`)
	result.Check(testkit.Rows("0 0 0 0 0"))
	result = tk.MustQuery(`select strcmp("1", "123"), strcmp("123", "1"), strcmp("123", "45"), strcmp("123", null), strcmp(null, "123")`)
	result.Check(testkit.Rows("-1 1 -1 <nil> <nil>"))
	result = tk.MustQuery(`select strcmp("", "123"), strcmp("123", ""), strcmp("", ""), strcmp("", null), strcmp(null, "")`)
	result.Check(testkit.Rows("-1 1 0 <nil> <nil>"))

	// for left
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('abcde', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery("select left(a, 2), left(b, 2), left(c, 2), left(d, 2), left(e, 2) from t")
	result.Check(testkit.Rows("ab 12 12 20 12"))
	result = tk.MustQuery(`select left("abc", 0), left("abc", -1), left(NULL, 1), left("abc", NULL)`)
	result.Check(testkit.Rows("  <nil> <nil>"))
	result = tk.MustQuery(`select left("abc", "a"), left("abc", 1.9), left("abc", 1.2)`)
	result.Check(testkit.Rows(" ab a"))
	result = tk.MustQuery(`select left("中文abc", 2), left("中文abc", 3), left("中文abc", 4)`)
	result.Check(testkit.Rows("中文 中文a 中文ab"))
	// for right, reuse the table created for left
	result = tk.MustQuery("select right(a, 3), right(b, 3), right(c, 3), right(d, 3), right(e, 3) from t")
	result.Check(testkit.Rows("cde 234 .34 :01 :01"))
	result = tk.MustQuery(`select right("abcde", 0), right("abcde", -1), right("abcde", 100), right(NULL, 1), right("abcde", NULL)`)
	result.Check(testkit.Rows("  abcde <nil> <nil>"))
	result = tk.MustQuery(`select right("abcde", "a"), right("abcde", 1.9), right("abcde", 1.2)`)
	result.Check(testkit.Rows(" de e"))
	result = tk.MustQuery(`select right("中文abc", 2), right("中文abc", 4), right("中文abc", 5)`)
	result.Check(testkit.Rows("bc 文abc 中文abc"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select left(a, 3), left(a, 6), left(a, 7) from t`)
	result.Check(testkit.Rows("中 中文 中文a"))
	result = tk.MustQuery(`select right(a, 2), right(a, 7) from t`)
	result.Check(testkit.Rows("c\x00 文abc\x00"))

	// for ord
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select ord(a), ord(b), ord(c), ord(d), ord(e), ord(f), ord(g), ord(h), ord(i) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10 53 52 116"))
	result = tk.MustQuery("select ord('123'), ord(123), ord(''), ord('你好'), ord(NULL), ord('👍')")
	result.Check(testkit.Rows("49 49 0 14990752 <nil> 4036989325"))
	result = tk.MustQuery("select ord(X''), ord(X'6161'), ord(X'e4bd'), ord(X'e4bda0'), ord(_ascii'你'), ord(_latin1'你')")
	result.Check(testkit.Rows("0 97 228 228 228 228"))

	// for space
	result = tk.MustQuery(`select space(0), space(2), space(-1), space(1.1), space(1.9)`)
	result.Check(testutil.RowsWithSep(",", ",  ,, ,  "))
	result = tk.MustQuery(`select space("abc"), space("2"), space("1.1"), space(''), space(null)`)
	result.Check(testutil.RowsWithSep(",", ",  , ,,<nil>"))

	// for replace
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.mysql.com', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select replace(a, 'mysql', 'pingcap'), replace(b, 2, 55), replace(c, 34, 0), replace(d, '-', '/'), replace(e, '01', '22') from t`)
	result.Check(testutil.RowsWithSep(",", "www.pingcap.com,15534,12.0,2017/01/01 12:01:01,12:22:22"))
	result = tk.MustQuery(`select replace('aaa', 'a', ''), replace(null, 'a', 'b'), replace('a', null, 'b'), replace('a', 'b', null)`)
	result.Check(testkit.Rows(" <nil> <nil> <nil>"))

	// for tobase64
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h blob(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "512", "abc")`)
	result = tk.MustQuery("select to_base64(a), to_base64(b), to_base64(c), to_base64(d), to_base64(e), to_base64(f), to_base64(g), to_base64(h), to_base64(null) from t")
	result.Check(testkit.Rows("MQ== MS4x MjAxNy0wMS0wMSAxMjowMTowMQ== MTI6MDE6MDE= YWJjZGVm ABU= NTEyAAAAAAAAAAAAAAAAAAAAAAA= YWJj <nil>"))

	// for from_base64
	result = tk.MustQuery(`select from_base64("abcd"), from_base64("asc")`)
	result.Check(testkit.Rows("i\xb7\x1d <nil>"))
	result = tk.MustQuery(`select from_base64("MQ=="), from_base64(1234)`)
	result.Check(testkit.Rows("1 \xd7m\xf8"))

	// for substr
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('Sakila', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substr(a, 3), substr(b, 2, 3), substr(c, -3), substr(d, -8), substr(e, -3, 100) from t`)
	result.Check(testkit.Rows("kila 234 .45 12:01:01 :01"))
	result = tk.MustQuery(`select substr('Sakila', 100), substr('Sakila', -100), substr('Sakila', -5, 3), substr('Sakila', 2, -1)`)
	result.Check(testutil.RowsWithSep(",", ",,aki,"))
	result = tk.MustQuery(`select substr('foobarbar' from 4), substr('Sakila' from -4 for 2)`)
	result.Check(testkit.Rows("barbar ki"))
	result = tk.MustQuery(`select substr(null, 2, 3), substr('foo', null, 3), substr('foo', 2, null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select substr('中文abc', 2), substr('中文abc', 3), substr("中文abc", 1, 2)`)
	result.Check(testkit.Rows("文abc abc 中文"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select substr(a, 4), substr(a, 1, 3), substr(a, 1, 6) from t`)
	result.Check(testkit.Rows("文abc\x00 中 中文"))
	result = tk.MustQuery(`select substr("string", -1), substr("string", -2), substr("中文", -1), substr("中文", -2) from t`)
	result.Check(testkit.Rows("g ng 文 中文"))

	// for bit_length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h varbinary(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "g", "h")`)
	result = tk.MustQuery("select bit_length(a), bit_length(b), bit_length(c), bit_length(d), bit_length(e), bit_length(f), bit_length(g), bit_length(h), bit_length(null) from t")
	result.Check(testkit.Rows("8 24 152 64 48 16 160 8 <nil>"))

	// for substring_index
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substring_index(a, '.', 2), substring_index(b, '.', 2), substring_index(c, '.', -1), substring_index(d, '-', 1), substring_index(e, ':', -2) from t`)
	result.Check(testkit.Rows("www.pingcap 12345 45 2017 01:01"))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', '.', 0), substring_index('www.pingcap.com', '.', 100), substring_index('www.pingcap.com', '.', -100)`)
	result.Check(testkit.Rows(" www.pingcap.com www.pingcap.com"))
	tk.MustQuery(`select substring_index('xyz', 'abc', 9223372036854775808)`).Check(testkit.Rows(``))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', 'd', 1), substring_index('www.pingcap.com', '', 1), substring_index('', '.', 1)`)
	result.Check(testutil.RowsWithSep(",", "www.pingcap.com,,"))
	result = tk.MustQuery(`select substring_index(null, '.', 1), substring_index('www.pingcap.com', null, 1), substring_index('www.pingcap.com', '.', null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for hex
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f decimal(5, 2), g bit(4))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", 123.45, 0b1100)`)
	result = tk.MustQuery(`select hex(a), hex(b), hex(c), hex(d), hex(e), hex(f), hex(g) from t`)
	result.Check(testkit.Rows("7777772E70696E676361702E636F6D 3039 7B 323031372D30312D30312031323A30313A3031 31323A30313A3031 7B C"))
	result = tk.MustQuery(`select hex('abc'), hex('你好'), hex(12), hex(12.3), hex(12.8)`)
	result.Check(testkit.Rows("616263 E4BDA0E5A5BD C C D"))
	result = tk.MustQuery(`select hex(-1), hex(-12.3), hex(-12.8), hex(0x12), hex(null)`)
	result.Check(testkit.Rows("FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFF4 FFFFFFFFFFFFFFF3 12 <nil>"))

	// for unhex
	result = tk.MustQuery(`select unhex('4D7953514C'), unhex('313233'), unhex(313233), unhex('')`)
	result.Check(testkit.Rows("MySQL 123 123 "))
	result = tk.MustQuery(`select unhex('string'), unhex('你好'), unhex(123.4), unhex(null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	// for ltrim and rtrim
	result = tk.MustQuery(`select ltrim('   bar   '), ltrim('bar'), ltrim(''), ltrim(null)`)
	result.Check(testutil.RowsWithSep(",", "bar   ,bar,,<nil>"))
	result = tk.MustQuery(`select rtrim('   bar   '), rtrim('bar'), rtrim(''), rtrim(null)`)
	result.Check(testutil.RowsWithSep(",", "   bar,bar,,<nil>"))
	result = tk.MustQuery(`select ltrim("\t   bar   "), ltrim("   \tbar"), ltrim("\n  bar"), ltrim("\r  bar")`)
	result.Check(testutil.RowsWithSep(",", "\t   bar   ,\tbar,\n  bar,\r  bar"))
	result = tk.MustQuery(`select rtrim("   bar   \t"), rtrim("bar\t   "), rtrim("bar   \n"), rtrim("bar   \r")`)
	result.Check(testutil.RowsWithSep(",", "   bar   \t,bar\t,bar   \n,bar   \r"))

	// for reverse
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(6));`)
	tk.MustExec(`INSERT INTO t VALUES("中文");`)
	result = tk.MustQuery(`SELECT a, REVERSE(a), REVERSE("中文"), REVERSE("123 ") FROM t;`)
	result.Check(testkit.Rows("中文 \x87\x96歸\xe4 文中  321"))
	result = tk.MustQuery(`SELECT REVERSE(123), REVERSE(12.09) FROM t;`)
	result.Check(testkit.Rows("321 90.21"))

	// for trim
	result = tk.MustQuery(`select trim('   bar   '), trim(leading 'x' from 'xxxbarxxx'), trim(trailing 'xyz' from 'barxxyz'), trim(both 'x' from 'xxxbarxxx')`)
	result.Check(testkit.Rows("bar barxxx barx bar"))
	result = tk.MustQuery(`select trim('\t   bar\n   '), trim('   \rbar   \t')`)
	result.Check(testutil.RowsWithSep(",", "\t   bar\n,\rbar   \t"))
	result = tk.MustQuery(`select trim(leading from '   bar'), trim('x' from 'xxxbarxxx'), trim('x' from 'bar'), trim('' from '   bar   ')`)
	result.Check(testutil.RowsWithSep(",", "bar,bar,bar,   bar   "))
	result = tk.MustQuery(`select trim(''), trim('x' from '')`)
	result.Check(testutil.RowsWithSep(",", ","))
	result = tk.MustQuery(`select trim(null from 'bar'), trim('x' from null), trim(null), trim(leading null from 'bar')`)
	// FIXME: the result for trim(leading null from 'bar') should be <nil>, current is 'bar'
	result.Check(testkit.Rows("<nil> <nil> <nil> bar"))

	// for locate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f binary(5))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", "HelLo")`)
	result = tk.MustQuery(`select locate(".ping", a), locate(".ping", a, 5) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("234", b), locate("235", b, 10) from t`)
	result.Check(testkit.Rows("2 0"))
	result = tk.MustQuery(`select locate(".45", c), locate(".35", b) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("El", f), locate("ll", f), locate("lL", f), locate("Lo", f), locate("lo", f) from t`)
	result.Check(testkit.Rows("0 0 3 4 0"))
	result = tk.MustQuery(`select locate("01 12", d) from t`)
	result.Check(testkit.Rows("9"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 2)`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select locate("文", "中文字符串")`)
	result.Check(testkit.Rows("2"))

	// for bin
	result = tk.MustQuery(`select bin(-1);`)
	result.Check(testkit.Rows("1111111111111111111111111111111111111111111111111111111111111111"))
	result = tk.MustQuery(`select bin(5);`)
	result.Check(testkit.Rows("101"))
	result = tk.MustQuery(`select bin("中文");`)
	result.Check(testkit.Rows("0"))

	// for character_length
	result = tk.MustQuery(`select character_length(null), character_length("Hello"), character_length("a中b文c"),
	character_length(123), character_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))

	// for char_length
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a中b文c"), char_length(123),char_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a 中 b 文 c"), char_length("НОЧЬ НА ОКРАИНЕ МОСКВЫ");`)
	result.Check(testkit.Rows("<nil> 5 9 22"))
	// for char_length, binary string type
	result = tk.MustQuery(`select char_length(null), char_length(binary("Hello")), char_length(binary("a 中 b 文 c")), char_length(binary("НОЧЬ НА ОКРАИНЕ МОСКВЫ"));`)
	result.Check(testkit.Rows("<nil> 5 13 41"))

	// for elt
	result = tk.MustQuery(`select elt(0, "abc", "def"), elt(2, "hello", "中文", "tidb"), elt(4, "hello", "中文",
	"tidb");`)
	result.Check(testkit.Rows("<nil> 中文 <nil>"))

	// for instr
	result = tk.MustQuery(`select instr("中国", "国"), instr("中国", ""), instr("abc", ""), instr("", ""), instr("", "abc");`)
	result.Check(testkit.Rows("2 1 1 1 0"))
	result = tk.MustQuery(`select instr("中国", null), instr(null, ""), instr(null, null);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(20), b char(20));`)
	tk.MustExec(`insert into t values("中国", cast("国" as binary)), ("中国", ""), ("abc", ""), ("", ""), ("", "abc");`)
	result = tk.MustQuery(`select instr(a, b) from t;`)
	result.Check(testkit.Rows("4", "1", "1", "1", "0"))

	// for oct
	result = tk.MustQuery(`select oct("aaaa"), oct("-1.9"),  oct("-9999999999999999999999999"), oct("9999999999999999999999999");`)
	result.Check(testkit.Rows("0 1777777777777777777777 1777777777777777777777 1777777777777777777777"))
	result = tk.MustQuery(`select oct(-1.9), oct(1.9), oct(-1), oct(1), oct(-9999999999999999999999999), oct(9999999999999999999999999);`)
	result.Check(testkit.Rows("1777777777777777777777 1 1777777777777777777777 1 1777777777777777777777 1777777777777777777777"))

	// #issue 4356
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (b BIT(8));")
	tk.MustExec(`INSERT INTO t SET b = b'11111111';`)
	tk.MustExec(`INSERT INTO t SET b = b'1010';`)
	tk.MustExec(`INSERT INTO t SET b = b'0101';`)
	result = tk.MustQuery(`SELECT b+0, BIN(b), OCT(b), HEX(b) FROM t;`)
	result.Check(testkit.Rows("255 11111111 377 FF", "10 1010 12 A", "5 101 5 5"))

	// for find_in_set
	result = tk.MustQuery(`select find_in_set("", ""), find_in_set("", ","), find_in_set("中文", "字符串,中文"), find_in_set("b,", "a,b,c,d");`)
	result.Check(testkit.Rows("0 1 2 0"))
	result = tk.MustQuery(`select find_in_set(NULL, ""), find_in_set("", NULL), find_in_set(1, "2,3,1");`)
	result.Check(testkit.Rows("<nil> <nil> 3"))

	// for make_set
	result = tk.MustQuery(`select make_set(0, "12"), make_set(3, "aa", "11"), make_set(3, NULL, "中文"), make_set(NULL, "aa");`)
	result.Check(testkit.Rows(" aa,11 中文 <nil>"))

	// for convert
	result = tk.MustQuery(`select convert("123" using "binary"), convert("中文" using "binary"), convert("中文" using "utf8"), convert("中文" using "utf8mb4"), convert(cast("中文" as binary) using "utf8");`)
	result.Check(testkit.Rows("123 中文 中文 中文 中文"))
	// Charset 866 does not have a default collation configured currently, so this will return error.
	err = tk.ExecToErr(`select convert("123" using "866");`)
	c.Assert(err.Error(), Equals, "[parser:1115]Unknown character set: '866'")
	// Test case in issue #4436.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a char(20));")
	err = tk.ExecToErr("select convert(a using a) from t;")
	c.Assert(err.Error(), Equals, "[parser:1115]Unknown character set: 'a'")

	// for insert
	result = tk.MustQuery(`select insert("中文", 1, 1, cast("aaa" as binary)), insert("ba", -1, 1, "aaa"), insert("ba", 1, 100, "aaa"), insert("ba", 100, 1, "aaa");`)
	result.Check(testkit.Rows("aaa文 ba aaa ba"))
	result = tk.MustQuery(`select insert("bb", NULL, 1, "aa"), insert("bb", 1, NULL, "aa"), insert(NULL, 1, 1, "aaa"), insert("bb", 1, 1, NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("bb", 0, 1, NULL), INSERT("bb", 0, NULL, "aaa");`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("中文", 0, 1, NULL), INSERT("中文", 0, NULL, "aaa");`)
	result.Check(testkit.Rows("<nil> <nil>"))

	// for export_set
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", -1);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(NULL, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 1);`)
	result.Check(testkit.Rows("1"))

	// for format
	result = tk.MustQuery(`select format(12332.1, 4), format(12332.2, 0), format(12332.2, 2,'en_US');`)
	result.Check(testkit.Rows("12,332.1000 12,332 12,332.20"))
	result = tk.MustQuery(`select format(NULL, 4), format(12332.2, NULL);`)
	result.Check(testkit.Rows("<nil> <nil>"))
	rs, err := tk.Exec(`select format(12332.2, 2,'es_EC');`)
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "not support for the specific locale")
	c.Assert(rs.Close(), IsNil)

	// for field
	result = tk.MustQuery(`select field(1, 2, 1), field(1, 0, NULL), field(1, NULL, 2, 1), field(NULL, 1, 2, NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "0", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "abc", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("abc", "a", 1), field(1.3, "1.3", 1.5);`)
	result.Check(testkit.Rows("1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(11, 8), b decimal(11,8))")
	tk.MustExec("insert into t values('114.57011441','38.04620115'), ('-38.04620119', '38.04620115');")
	result = tk.MustQuery("select a,b,concat_ws(',',a,b) from t")
	result.Check(testkit.Rows("114.57011441 38.04620115 114.57011441,38.04620115",
		"-38.04620119 38.04620115 -38.04620119,38.04620115"))
}

func (s *testIntegrationSuite) TestOpBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for logicAnd
	result := tk.MustQuery("select 1 && 1, 1 && 0, 0 && 1, 0 && 0, 2 && -1, null && 1, '1a' && 'a'")
	result.Check(testkit.Rows("1 0 0 0 1 <nil> 0"))
	// for bitNeg
	result = tk.MustQuery("select ~123, ~-123, ~null")
	result.Check(testkit.Rows("18446744073709551492 122 <nil>"))
	// for logicNot
	result = tk.MustQuery("select !1, !123, !0, !null")
	result.Check(testkit.Rows("0 0 1 <nil>"))
	// for logicalXor
	result = tk.MustQuery("select 1 xor 1, 1 xor 0, 0 xor 1, 0 xor 0, 2 xor -1, null xor 1, '1a' xor 'a'")
	result.Check(testkit.Rows("0 1 1 0 0 <nil> 1"))
	// for bitAnd
	result = tk.MustQuery("select 123 & 321, -123 & 321, null & 1")
	result.Check(testkit.Rows("65 257 <nil>"))
	// for bitOr
	result = tk.MustQuery("select 123 | 321, -123 | 321, null | 1")
	result.Check(testkit.Rows("379 18446744073709551557 <nil>"))
	// for bitXor
	result = tk.MustQuery("select 123 ^ 321, -123 ^ 321, null ^ 1")
	result.Check(testkit.Rows("314 18446744073709551300 <nil>"))
	// for leftShift
	result = tk.MustQuery("select 123 << 2, -123 << 2, null << 1")
	result.Check(testkit.Rows("492 18446744073709551124 <nil>"))
	// for rightShift
	result = tk.MustQuery("select 123 >> 2, -123 >> 2, null >> 1")
	result.Check(testkit.Rows("30 4611686018427387873 <nil>"))
	// for logicOr
	result = tk.MustQuery("select 1 || 1, 1 || 0, 0 || 1, 0 || 0, 2 || -1, null || 1, '1a' || 'a'")
	result.Check(testkit.Rows("1 1 1 0 1 1 1"))
	// for unaryPlus
	result = tk.MustQuery(`select +1, +0, +(-9), +(-0.001), +0.999, +null, +"aaa"`)
	result.Check(testkit.Rows("1 0 -9 -0.001 0.999 <nil> aaa"))
	// for unaryMinus
	tk.MustExec("drop table if exists f")
	tk.MustExec("create table f(a decimal(65,0))")
	tk.MustExec("insert into f value (-17000000000000000000)")
	result = tk.MustQuery("select a from f")
	result.Check(testkit.Rows("-17000000000000000000"))
}

func (s *testIntegrationSuite) TestControlBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for ifnull
	result := tk.MustQuery("select ifnull(1, 2)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, 2)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select ifnull(1, null)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, null)")
	result.Check(testkit.Rows("<nil>"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a bigint not null)")
	result = tk.MustQuery("select ifnull(max(a),0) from t1")
	result.Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a decimal(20,4))")
	tk.MustExec("create table t2(a decimal(20,4))")
	tk.MustExec("insert into t1 select 1.2345")
	tk.MustExec("insert into t2 select 1.2345")

	result = tk.MustQuery(`select sum(ifnull(a, 0)) from (
	select ifnull(a, 0) as a from t1
	union all
	select ifnull(a, 0) as a from t2
	) t;`)
	result.Check(testkit.Rows("2.4690"))

	// for if
	result = tk.MustQuery(`select IF(0,"ERROR","this"),IF(1,"is","ERROR"),IF(NULL,"ERROR","a"),IF(1,2,3)|0,IF(1,2.0,3.0)+0;`)
	result.Check(testkit.Rows("this is a 2 2.0"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (st varchar(255) NOT NULL, u int(11) NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES ('a',1),('A',1),('aa',1),('AA',1),('a',1),('aaa',0),('BBB',0);")
	result = tk.MustQuery("select if(1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	result = tk.MustQuery("select if(u=1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a varchar(255), b time, c int)")
	tk.MustExec("INSERT INTO t1 VALUE('abc', '12:00:00', 0)")
	tk.MustExec("INSERT INTO t1 VALUE('1abc', '00:00:00', 1)")
	tk.MustExec("INSERT INTO t1 VALUE('0abc', '12:59:59', 0)")
	result = tk.MustQuery("select if(a, b, c), if(b, a, c), if(c, a, b) from t1")
	result.Check(testkit.Rows("0 abc 12:00:00", "00:00:00 1 1abc", "0 0abc 12:59:59"))
	result = tk.MustQuery("select if(1, 1.0, 1)")
	result.Check(testkit.Rows("1.0"))
	// FIXME: MySQL returns `1.0`.
	result = tk.MustQuery("select if(1, 1, 1.0)")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select if(count(*), cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Rows("2000-01-01"))
	tk.MustQuery("select if(count(*)=0, cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Rows("2011-01-01"))
	tk.MustQuery("select if(count(*), cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Rows("[]"))
	tk.MustQuery("select if(count(*)=0, cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Rows("{}"))

	result = tk.MustQuery("SELECT 79 + + + CASE -87 WHEN -30 THEN COALESCE(COUNT(*), +COALESCE(+15, -33, -12 ) + +72) WHEN +COALESCE(+AVG(DISTINCT(60)), 21) THEN NULL ELSE NULL END AS col0;")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("SELECT -63 + COALESCE ( - 83, - 61 + - + 72 * - CAST( NULL AS SIGNED ) + + 3 );")
	result.Check(testkit.Rows("-146"))
}

func (s *testIntegrationSuite) TestArithmeticBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()

	// for plus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result := tk.MustQuery("SELECT a+b FROM t;")
	result.Check(testkit.Rows("3.089", "-1.200"))
	result = tk.MustQuery("SELECT b+12, b+0.01, b+0.00001, b+12.00001 FROM t;")
	result.Check(testkit.Rows("13.999 2.009 1.99901 13.99901", "11.900 -0.090 -0.09999 11.90001"))
	result = tk.MustQuery("SELECT 1+12, 21+0.01, 89+\"11\", 12+\"a\", 12+NULL, NULL+1, NULL+NULL;")
	result.Check(testkit.Rows("13 21.01 100 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1<<63, 1<<63;")
	rs, err := tk.Exec("SELECT a+b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a + test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-3 as signed) + cast(2 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-3 + 2)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(2 as unsigned) + cast(-3 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(2 + -3)'")
	c.Assert(rs.Close(), IsNil)

	// for minus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result = tk.MustQuery("SELECT a-b FROM t;")
	result.Check(testkit.Rows("-0.909", "-1.000"))
	result = tk.MustQuery("SELECT b-12, b-0.01, b-0.00001, b-12.00001 FROM t;")
	result.Check(testkit.Rows("-10.001 1.989 1.99899 -10.00101", "-12.100 -0.110 -0.10001 -12.10001"))
	result = tk.MustQuery("SELECT 1-12, 21-0.01, 89-\"11\", 12-\"a\", 12-NULL, NULL-1, NULL-NULL;")
	result.Check(testkit.Rows("-11 20.99 78 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1, 4;")
	rs, err = tk.Exec("SELECT a-b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a - test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as signed) - cast(-1 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-1 - 18446744073709551615)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as unsigned) - cast(-1 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551615 - -1)'")
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery(`select cast(-3 as unsigned) - cast(-1 as signed);`).Check(testkit.Rows("18446744073709551614"))
	tk.MustQuery("select 1.11 - 1.11;").Check(testkit.Rows("0.00"))
	tk.MustExec(`create table tb5(a int(10));`)
	tk.MustExec(`insert into tb5 (a) values (10);`)
	e := tk.QueryToErr(`select * from tb5 where a - -9223372036854775808;`)
	c.Assert(e, NotNil)
	c.Assert(e.Error(), Equals, `other error: [types:1690]BIGINT value is out of range in '(Column#0 - -9223372036854775808)'`)
	tk.MustExec(`drop table tb5`)

	// for multiply
	tk.MustQuery("select 1234567890 * 1234567890").Check(testkit.Rows("1524157875019052100"))
	rs, err = tk.Exec("select 1234567890 * 12345671890")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery("select cast(1234567890 as unsigned int) * 12345671890").Check(testkit.Rows("15241570095869612100"))
	tk.MustQuery("select 123344532434234234267890.0 * 1234567118923479823749823749.230").Check(testkit.Rows("152277104042296270209916846800130443726237424001224.7000"))
	rs, err = tk.Exec("select 123344532434234234267890.0 * 12345671189234798237498232384982309489238402830480239849238048239084749.230")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	// FIXME: There is something wrong in showing float number.
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * 1").Check(testkit.Rows("1.7976931348623157e308"))
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * -1").Check(testkit.Rows("-1.7976931348623157e308"))
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * 1.1")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * -1.1")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery("select 0.0 * -1;").Check(testkit.Rows("0.0"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(-1.09, 1.999);")
	result = tk.MustQuery("SELECT a/b, a/12, a/-0.01, b/12, b/-0.01, b/0.000, NULL/b, b/NULL, NULL/NULL FROM t;")
	result.Check(testkit.Rows("-0.545273 -0.090833 109.000000 0.1665833 -199.9000000 <nil> <nil> <nil> <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	rs, err = tk.Exec("select 1e200/1e-200")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	// for intDiv
	result = tk.MustQuery("SELECT 13 DIV 12, 13 DIV 0.01, -13 DIV 2, 13 DIV NULL, NULL DIV 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 1300 -6 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 div 1.1, 2.4 div 1.2, 2.4 div 1.3;")
	result.Check(testkit.Rows("2 2 1"))
	result = tk.MustQuery("SELECT 1.175494351E-37 div 1.7976931348623157E+308, 1.7976931348623157E+308 div -1.7976931348623157E+307, 1 div 1e-82;")
	result.Check(testkit.Rows("0 -1 <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(1.7976931348623157e+308)'",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(1.7976931348623157e+308)'",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(-1.7976931348623158e+307)'",
		"Warning|1365|Division by 0"))
	rs, err = tk.Exec("select 1e300 DIV 1.5")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_int_unsigned int unsigned, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, 5, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar div nonzero, c_time div nonzero, c_time div zero, c_timestamp div nonzero, c_timestamp div zero, c_varchar div zero from t;")
	result.Check(testkit.Rows("0 10000 <nil> 1680900431825 <nil> <nil>"))
	result = tk.MustQuery("select c_enum div nonzero from t;")
	result.Check(testkit.Rows("0"))
	tk.MustQuery("select c_enum div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nonzero div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	result = tk.MustQuery("select c_time div c_enum, c_timestamp div c_time, c_timestamp div c_enum from t;")
	result.Check(testkit.Rows("60000 168090043 10085402590951"))
	result = tk.MustQuery("select c_int_unsigned div nonzero, nonzero div c_int_unsigned, c_int_unsigned div zero from t;")
	result.Check(testkit.Rows("0 2 <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))

	// for mod
	result = tk.MustQuery("SELECT CAST(1 AS UNSIGNED) MOD -9223372036854775808, -9223372036854775808 MOD CAST(1 AS UNSIGNED);")
	result.Check(testkit.Rows("1 0"))
	result = tk.MustQuery("SELECT 13 MOD 12, 13 MOD 0.01, -13 MOD 2, 13 MOD NULL, NULL MOD 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 0.00 -1 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 MOD 1.1, 2.4 MOD 1.2, 2.4 mod 1.30;")
	result.Check(testkit.Rows("0.2 0.0 1.10"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar MOD nonzero, c_time MOD nonzero, c_timestamp MOD nonzero, c_enum MOD nonzero from t;")
	result.Check(testkit.Rows("0 0 3 2"))
	result = tk.MustQuery("select c_time MOD c_enum, c_timestamp MOD c_time, c_timestamp MOD c_enum from t;")
	result.Check(testkit.Rows("0 21903 1"))
	tk.MustQuery("select c_enum MOD zero from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustExec("SET SQL_MODE='ERROR_FOR_DIVISION_BY_ZERO,STRICT_ALL_TABLES';")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (v int);")
	tk.MustExec("INSERT IGNORE INTO t VALUE(12 MOD 0);")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustQuery("select v from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select 0.000 % 0.11234500000000000000;").Check(testkit.Rows("0.00000000000000000000"))

	_, err = tk.Exec("INSERT INTO t VALUE(12 MOD 0);")
	c.Assert(terror.ErrorEqual(err, expression.ErrDivisionByZero), IsTrue)

	tk.MustQuery("select sum(1.2e2) * 0.1").Check(testkit.Rows("12"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustQuery("select sum(a) * 0.1 from t").Check(testkit.Rows("0.12"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	result = tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Rows())
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1105|Division by 0"))

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT, b DECIMAL(6, 2));")
	tk.MustExec("INSERT INTO t VALUES(0, 1.12), (1, 1.21);")
	tk.MustQuery("SELECT a/b FROM t;").Check(testkit.Rows("0.0000", "0.8264"))
}

func (s *testIntegrationSuite) TestAggregationBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a decimal(7, 6))")
	tk.MustExec("insert into t values(1.123456), (1.123456)")
	result := tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("1.1234560000"))

	tk.MustExec("use test")
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`a` int, KEY `idx_a` (`a`))")
	result = tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select max(a), min(a) from t")
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery("select distinct a from t")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select sum(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select count(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitOr(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(4);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select a, bit_or(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "4 4"))
	tk.MustExec("insert into t values(-1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitXor(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select a, bit_xor(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "3 0"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitAnd(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
	tk.MustExec("insert into t values(7);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("7"))
	tk.MustExec("insert into t values(5);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("5"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select a, bit_and(a) from t group by a order by a desc")
	result.Check(testkit.Rows("7 7", "5 5", "3 3", "2 2", "<nil> 18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinGroupConcat(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(100))")
	tk.MustExec("create table d(a varchar(100))")
	tk.MustExec("insert into t values('hello'), ('hello')")
	result := tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,hello"))

	tk.MustExec("set @@group_concat_max_len=7")
	result = tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,h"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))

	_, err := tk.Exec("insert into d select group_concat(a) from t")
	c.Assert(errors.Cause(err).(*terror.Error).Code(), Equals, terror.ErrCode(mysql.ErrCutValueGroupConcat))

	tk.Exec("set sql_mode=''")
	tk.MustExec("insert into d select group_concat(a) from t")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))
	tk.MustQuery("select * from d").Check(testkit.Rows("hello,h"))
}

func (s *testIntegrationSuite) TestLiterals(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	r := tk.MustQuery("SELECT LENGTH(b''), LENGTH(B''), b''+1, b''-1, B''+1;")
	r.Check(testkit.Rows("0 0 1 -1 1"))
}

func (s *testIntegrationSuite) TestColumnInfoModified(c *C) {
	testKit := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, _ := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tab0"))
	col := table.FindCol(tbl.Cols(), "col1")
	c.Assert(col.Tp, Equals, mysql.TypeLong)
}

func (s *testIntegrationSuite) TestIssues(c *C) {
	// for issue #4954
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a CHAR(5) CHARACTER SET latin1);")
	tk.MustExec("INSERT INTO t VALUES ('oe');")
	tk.MustExec("INSERT INTO t VALUES (0xf6);")
	r := tk.MustQuery(`SELECT * FROM t WHERE a= 'oe';`)
	r.Check(testkit.Rows("oe"))
	r = tk.MustQuery(`SELECT HEX(a) FROM t WHERE a= 0xf6;`)
	r.Check(testkit.Rows("F6"))

	// for issue #4006
	tk.MustExec(`drop table if exists tb`)
	tk.MustExec("create table tb(id int auto_increment primary key, v varchar(32));")
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows())
	tk.MustExec(`insert into tb(v) values('hello');`)
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows("1 hello", "2 hello"))

	// for issue #5111
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(c varchar(32));")
	tk.MustExec("insert into t values('1e649'),('-1e649');")
	r = tk.MustQuery(`SELECT * FROM t where c < 1;`)
	r.Check(testkit.Rows("-1e649"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))
	r = tk.MustQuery(`SELECT * FROM t where c > 1;`)
	r.Check(testkit.Rows("1e649"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))

	// for issue #5293
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert t values (1)")
	tk.MustQuery("select * from t where cast(a as binary)").Check(testkit.Rows("1"))
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

func (s *testIntegrationSuite) TestTwoDecimalTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a decimal(10,5), b decimal(10,1))")
	tk.MustExec("insert into t1 values(123.12345, 123.12345)")
	tk.MustExec("update t1 set b = a")
	res := tk.MustQuery("select a, b from t1")
	res.Check(testkit.Rows("123.12345 123.1"))
	res = tk.MustQuery("select 2.00000000000000000000000000000001 * 1.000000000000000000000000000000000000000000002")
	res.Check(testkit.Rows("2.000000000000000000000000000000"))
}

func (s *testIntegrationSuite) TestPrefixIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  			name varchar(12) DEFAULT NULL,
  			KEY pname (name(12))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)

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
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	tk.MustExec("INSERT INTO prefix VALUES(0, 'b', 2), (1, 'bbb', 3), (2, 'bbc', 4), (3, 'bbb', 5), (4, 'abc', 6), (5, 'abc', 7), (6, 'abc', 7), (7, 'ÿÿ', 8), (8, 'ÿÿ0', 9), (9, 'ÿÿÿ', 10);")
	res = tk.MustQuery("select c, b from prefix where b > 'ÿ' and b < 'ÿÿc'")
	res.Check(testkit.Rows("8 ÿÿ", "9 ÿÿ0"))
}

func (s *testIntegrationSuite) TestDecimalMul(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a decimal(38, 17));")
	tk.MustExec("insert into t select 0.5999991229316*0.918755041726043;")
	res := tk.MustQuery("select * from t;")
	res.Check(testkit.Rows("0.55125221922461136"))
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

func (s *testIntegrationSuite) TestUserVarMockWindFunc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a int, b varchar (20), c varchar (20));`)
	tk.MustExec(`insert into t values
					(1,'key1-value1','insert_order1'),
    				(1,'key1-value2','insert_order2'),
    				(1,'key1-value3','insert_order3'),
    				(1,'key1-value4','insert_order4'),
    				(1,'key1-value5','insert_order5'),
    				(1,'key1-value6','insert_order6'),
    				(2,'key2-value1','insert_order1'),
    				(2,'key2-value2','insert_order2'),
    				(2,'key2-value3','insert_order3'),
    				(2,'key2-value4','insert_order4'),
    				(2,'key2-value5','insert_order5'),
    				(2,'key2-value6','insert_order6'),
    				(3,'key3-value1','insert_order1'),
    				(3,'key3-value2','insert_order2'),
    				(3,'key3-value3','insert_order3'),
    				(3,'key3-value4','insert_order4'),
    				(3,'key3-value5','insert_order5'),
    				(3,'key3-value6','insert_order6');
					`)
	tk.MustExec(`SET @LAST_VAL := NULL;`)
	tk.MustExec(`SET @ROW_NUM := 0;`)

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2
				where t2.ROW_NUM < 2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`2 1 2 key2-value1 insert_order1`,
		`3 1 3 key3-value1 insert_order1`,
	))

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`1 2 1 key1-value2 insert_order2`,
		`1 3 1 key1-value3 insert_order3`,
		`1 4 1 key1-value4 insert_order4`,
		`1 5 1 key1-value5 insert_order5`,
		`1 6 1 key1-value6 insert_order6`,
		`2 1 2 key2-value1 insert_order1`,
		`2 2 2 key2-value2 insert_order2`,
		`2 3 2 key2-value3 insert_order3`,
		`2 4 2 key2-value4 insert_order4`,
		`2 5 2 key2-value5 insert_order5`,
		`2 6 2 key2-value6 insert_order6`,
		`3 1 3 key3-value1 insert_order1`,
		`3 2 3 key3-value2 insert_order2`,
		`3 3 3 key3-value3 insert_order3`,
		`3 4 3 key3-value4 insert_order4`,
		`3 5 3 key3-value5 insert_order5`,
		`3 6 3 key3-value6 insert_order6`,
	))
}

func (s *testIntegrationSuite) TestCastAsTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (col1 bigint, col2 double, col3 decimal, col4 varchar(20), col5 json);`)
	tk.MustExec(`insert into t values (1, 1, 1, "1", "1");`)
	tk.MustExec(`insert into t values (null, null, null, null, null);`)
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 = 1;`).Check(testkit.Rows(
		`00:00:01 00:00:01 00:00:01 00:00:01 00:00:01`,
	))
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 is null;`).Check(testkit.Rows(
		`<nil> <nil> <nil> <nil> <nil>`,
	))

	err := tk.ExecToErr(`select cast(col1 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col2 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col3 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col4 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col5 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")
}

func (s *testIntegrationSuite) TestValuesFloat32(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (i int key, j float);`)
	tk.MustExec(`insert into t values (1, 0.01);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.01`))
	tk.MustExec(`insert into t values (1, 0.02) on duplicate key update j = values (j);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.02`))
}

func (s *testIntegrationSuite) TestValuesEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a bigint primary key, b enum('a','b','c'));`)
	tk.MustExec(`insert into t values (1, "a");`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 a`))
	tk.MustExec(`insert into t values (1, "b") on duplicate key update b = values(b);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 b`))
}

func (s *testIntegrationSuite) TestIssue10181(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint unsigned primary key);`)
	tk.MustExec(`insert into t values(9223372036854775807), (18446744073709551615)`)
	tk.MustQuery(`select * from t where a > 9223372036854775807-0.5 order by a`).Check(testkit.Rows(`9223372036854775807`, `18446744073709551615`))
}

func (s *testIntegrationSuite) TestExprPushdownBlacklist(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery(`select * from mysql.expr_pushdown_blacklist`).Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestOptRuleBlacklist(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery(`select * from mysql.opt_rule_blacklist`).Check(testkit.Rows())
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

	// issue 11647
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(b bit(1));`)
	tk.MustExec(`insert into t values(b'1');`)
	tk.MustQuery(`select count(*) from t where b = 1;`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = '1';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = b'1';`).Check(testkit.Rows("1"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(b bit(63));`)
	// Not 64, because the behavior of mysql is amazing. I have no idea to fix it.
	tk.MustExec(`insert into t values(b'111111111111111111111111111111111111111111111111111111111111111');`)
	tk.MustQuery(`select count(*) from t where b = 9223372036854775807;`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = '9223372036854775807';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = b'111111111111111111111111111111111111111111111111111111111111111';`).Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestIssue11594(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1 (v bigint(20) UNSIGNED NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES (1), (2);")
	tk.MustQuery("SELECT SUM(IF(v > 1, v, -v)) FROM t1;").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT sum(IFNULL(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Rows("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Rows("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), v)) FROM t1;").Check(testkit.Rows("3"))
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
