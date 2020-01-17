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

package infoschema_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testTableSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testTableSuite) TestCharacterSetCollations(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// The description column is not important
	tk.MustQuery("SELECT default_collate_name, maxlen FROM information_schema.character_sets ORDER BY character_set_name").Check(
		testkit.Rows("ascii_bin 1", "binary 1", "latin1_bin 1", "utf8_bin 3", "utf8mb4_bin 4"))

	// The is_default column is not important
	// but the id's are used by client libraries and must be stable
	tk.MustQuery("SELECT character_set_name, id, sortlen FROM information_schema.collations ORDER BY collation_name").Check(
		testkit.Rows("ascii 65 1", "binary 63 1", "latin1 47 1", "utf8 83 1", "utf8mb4 46 1"))

	// Test charset/collation in information_schema.COLUMNS table.
	tk.MustExec("DROP DATABASE IF EXISTS charset_collate_test")
	tk.MustExec("CREATE DATABASE charset_collate_test; USE charset_collate_test")

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec(`CREATE TABLE charset_collate_col_test(
		c_int int,
		c_float float,
		c_bit bit,
		c_bool bool,
		c_char char(1) charset ascii collate ascii_bin,
		c_nchar national char(1) charset ascii collate ascii_bin,
		c_binary binary,
		c_varchar varchar(1) charset ascii collate ascii_bin,
		c_nvarchar national varchar(1) charset ascii collate ascii_bin,
		c_varbinary varbinary(1),
		c_year year,
		c_date date,
		c_time time,
		c_datetime datetime,
		c_timestamp timestamp,
		c_blob blob,
		c_tinyblob tinyblob,
		c_mediumblob mediumblob,
		c_longblob longblob,
		c_text text charset ascii collate ascii_bin,
		c_tinytext tinytext charset ascii collate ascii_bin,
		c_mediumtext mediumtext charset ascii collate ascii_bin,
		c_longtext longtext charset ascii collate ascii_bin,
		c_json json,
		c_enum enum('1') charset ascii collate ascii_bin,
		c_set set('1') charset ascii collate ascii_bin
	)`)

	tk.MustQuery(`SELECT column_name, character_set_name, collation_name
					FROM information_schema.COLUMNS
					WHERE table_schema = "charset_collate_test" AND table_name = "charset_collate_col_test"
					ORDER BY column_name`,
	).Check(testkit.Rows(
		"c_binary <nil> <nil>",
		"c_bit <nil> <nil>",
		"c_blob <nil> <nil>",
		"c_bool <nil> <nil>",
		"c_char ascii ascii_bin",
		"c_date <nil> <nil>",
		"c_datetime <nil> <nil>",
		"c_enum ascii ascii_bin",
		"c_float <nil> <nil>",
		"c_int <nil> <nil>",
		"c_json <nil> <nil>",
		"c_longblob <nil> <nil>",
		"c_longtext ascii ascii_bin",
		"c_mediumblob <nil> <nil>",
		"c_mediumtext ascii ascii_bin",
		"c_nchar ascii ascii_bin",
		"c_nvarchar ascii ascii_bin",
		"c_set ascii ascii_bin",
		"c_text ascii ascii_bin",
		"c_time <nil> <nil>",
		"c_timestamp <nil> <nil>",
		"c_tinyblob <nil> <nil>",
		"c_tinytext ascii ascii_bin",
		"c_varbinary <nil> <nil>",
		"c_varchar ascii ascii_bin",
		"c_year <nil> <nil>",
	))
	tk.MustExec("DROP DATABASE charset_collate_test")
}

type mockSessionManager struct {
	processInfoMap map[uint64]*util.ProcessInfo
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo { return sm.processInfoMap }

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) Kill(connectionID uint64, query bool) {}

func (s *testTableSuite) TestSomeTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustQuery("select * from information_schema.COLLATION_CHARACTER_SET_APPLICABILITY where COLLATION_NAME='utf8mb4_bin';").Check(
		testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select * from information_schema.SESSION_VARIABLES where VARIABLE_NAME='tidb_retry_limit';").Check(testkit.Rows("tidb_retry_limit 10"))
	tk.MustQuery("select * from information_schema.ENGINES;").Check(testkit.Rows("InnoDB DEFAULT Supports transactions, row-level locking, and foreign keys YES YES YES"))
	tk.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Check(testkit.Rows("def mysql delete_range_index mysql gc_delete_range UNIQUE"))

	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2)}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		DB:      "information_schema",
		Command: byte(1),
		State:   1,
		Info:    "do something",
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:      2,
		User:    "user-2",
		Host:    "localhost",
		DB:      "test",
		Command: byte(2),
		State:   2,
		Info:    strings.Repeat("x", 101),
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	tk.Se.SetSessionManager(sm)
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s 0 ", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 2 %s 0 ", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 2 %s", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 2 %s", strings.Repeat("x", 101)),
		))

	sm = &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2)}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		DB:      "information_schema",
		Command: byte(1),
		State:   1,
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:            2,
		User:          "user-2",
		Host:          "localhost",
		Command:       byte(2),
		State:         2,
		Info:          strings.Repeat("x", 101),
		StmtCtx:       tk.Se.GetSessionVars().StmtCtx,
		CurTxnStartTS: 410090409861578752,
	}
	tk.Se.SetSessionManager(sm)
	tk.Se.GetSessionVars().TimeZone = time.UTC
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s 0 ", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 2 %s 0 07-29 03:26:05.158(410090409861578752)", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 2 %s", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 2 %s", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where db is null;").Check(
		testkit.Rows(
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 2 %s 0 07-29 03:26:05.158(410090409861578752)", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where Info is null;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 1 %s 0 ", "<nil>"),
		))
}

func (s *testTableSuite) TestSchemataCharacterSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE DATABASE `foo` DEFAULT CHARACTER SET = 'utf8mb4'")
	tk.MustQuery("select default_character_set_name, default_collation_name FROM information_schema.SCHEMATA  WHERE schema_name = 'foo'").Check(
		testkit.Rows("utf8mb4 utf8mb4_bin"))
	tk.MustExec("drop database `foo`")
}

func (s *testTableSuite) TestForServersInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	result := tk.MustQuery("select * from information_schema.TIDB_SERVERS_INFO")
	c.Assert(len(result.Rows()), Equals, 1)

	serversInfo, err := infosync.GetAllServerInfo(context.Background())
	c.Assert(err, IsNil)
	c.Assert(len(serversInfo), Equals, 1)

	for _, info := range serversInfo {
		c.Assert(result.Rows()[0][0], Equals, info.ID)
		c.Assert(result.Rows()[0][1], Equals, info.IP)
		c.Assert(result.Rows()[0][2], Equals, strconv.FormatInt(int64(info.Port), 10))
		c.Assert(result.Rows()[0][3], Equals, strconv.FormatInt(int64(info.StatusPort), 10))
		c.Assert(result.Rows()[0][4], Equals, info.Lease)
		c.Assert(result.Rows()[0][5], Equals, info.Version)
		c.Assert(result.Rows()[0][6], Equals, info.GitHash)
	}
}

func (s *testTableSuite) TestColumnStatistics(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.column_statistics").Check(testkit.Rows())
}

func (s *testTableSuite) TestReloadDropDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_dbs")
	tk.MustExec("use test_dbs")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create table t3 (a int)")
	is := domain.GetDomain(tk.Se).InfoSchema()
	t2, err := is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tk.MustExec("drop database test_dbs")
	is = domain.GetDomain(tk.Se).InfoSchema()
	_, err = is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	c.Assert(terror.ErrorEqual(infoschema.ErrTableNotExists, err), IsTrue)
	_, ok := is.TableByID(t2.Meta().ID)
	c.Assert(ok, IsFalse)
}

func (s *testTableSuite) TestSelectHiddenColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("DROP DATABASE IF EXISTS `test_hidden`;")
	tk.MustExec("CREATE DATABASE `test_hidden`;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE hidden (a int , b int, c int);")
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("3"))
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("hidden"))
	c.Assert(err, IsNil)
	colInfo := tb.Meta().Columns
	// Set column b to hidden
	colInfo[1].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("0"))
	// Set column b to visible
	colInfo[1].Hidden = false
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("1"))
	// Set a, b ,c to hidden
	colInfo[0].Hidden = true
	colInfo[1].Hidden = true
	colInfo[2].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("0"))
}
