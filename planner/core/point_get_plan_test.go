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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPointGetSuite{})

type testPointGetSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPointGetSuite) TestPointGetForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table fu (id int primary key, val int)")
	tk.MustExec("insert into fu values (6, 6)")

	// In autocommit mode, outside a transaction, "for update" doesn't take effect.
	checkUseForUpdate(tk, c, false)

	tk.MustExec("begin")
	checkUseForUpdate(tk, c, true)
	tk.MustExec("rollback")

	tk.MustExec("set @@session.autocommit = 0")
	checkUseForUpdate(tk, c, true)
	tk.MustExec("rollback")
}

func checkUseForUpdate(tk *testkit.TestKit, c *C, expectLock bool) {
	res := tk.MustQuery("explain select * from fu where id = 6 for update")
	// Point_Get_1	1.00	root	table:fu, handle:6
	opInfo := res.Rows()[0][3]
	selectLock := strings.Contains(fmt.Sprintf("%s", opInfo), "lock")
	c.Assert(selectLock, Equals, expectLock)

	tk.MustQuery("select * from fu where id = 6 for update").Check(testkit.Rows("6 6"))
}

func (s *testPointGetSuite) TestWhereIn2BatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key auto_increment not null, b int, c int, unique key idx_abc(a, b, c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 5)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 3 3",
		"4 4 5",
	))
	tk.MustQuery("explain select * from t where a = 1 and b = 1 and c = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, index:a b c",
	))
	tk.MustQuery("explain select * from t where 1 = a and 1 = b and 1 = c").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, index:a b c",
	))
	tk.MustQuery("explain select * from t where 1 = a and b = 1 and 1 = c").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, index:a b c",
	))
	tk.MustQuery("explain select * from t where (a, b, c) in ((1, 1, 1), (2, 2, 2))").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root table:t, index:a b c",
	))

	tk.MustQuery("explain select * from t where a in (1, 2, 3, 4, 5)").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root table:t",
	))

	tk.MustQuery("explain select * from t where a in (1, 2, 3, 1, 2)").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root table:t",
	))

	tk.MustQuery("explain select * from t where (a) in ((1), (2), (3), (1), (2))").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root table:t",
	))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, unique key idx_ab(a, b))")
	tk.MustExec("insert into t values(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"3 4 5",
		"4 5 6",
	))
	tk.MustQuery("explain select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root table:t, index:a b",
	))
	tk.MustQuery("select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((1, 2), (2, 3))").Check(testkit.Rows())
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4), (3, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))
}
