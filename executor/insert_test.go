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
	"strconv"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite8) TestInsertOnDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 1"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 300"))

	tk.MustExec(`insert into t1 values(1, 1) on duplicate key update b1 = 400;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err := tk.Exec(`insert into t1 select * from t2 on duplicate key update c = t2.b;`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a = b;`)
	c.Assert(err.Error(), Equals, `[planner:1052]Column 'b' in field list is ambiguous`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update c = b;`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a1 = values(b2);`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'b2' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));`)
	tk.MustExec(`insert into t (val, k1, k2) values (3, 1, 2);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 4`))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 double, k2 double, v double, primary key(k1, k2));`)
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 2, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(5))
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 1, 2);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(4))
	tk.CheckLastMessage("Records: 3  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 2`, `3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int, c int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 2, 1, 2);`)
	tk.MustExec(`insert into t1 values (3, 3, 2, 2);`)
	tk.MustExec(`insert into t1 values (4, 4, 2, 2);`)
	tk.MustExec(`create table t2(a int primary key, b int, c int, unique(b), unique(c));`)
	tk.MustExec(`insert into t2 select a, b, c from t1 order by id on duplicate key update b=t2.b, c=t2.c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 1 1`, `3 2 2`))

	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(5))
	tk.CheckLastMessage("Records: 5  Duplicates: 0  Warnings: 0")
	tk.MustExec(`insert into t1 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b=b+10`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(7))
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")
}

func (s *testSuite3) TestUpdateDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table c(i int,j int,k int,primary key(i,j,k));`)
	tk.MustExec(`insert into c values(1,2,3);`)
	tk.MustExec(`insert into c values(1,2,4);`)
	_, err := tk.Exec(`update c set i=1,j=2,k=4 where i=1 and j=2 and k=3;`)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-2-4' for key 'PRIMARY'")
}

func (s *testSuite3) TestInsertWrongValueForField(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a bigint);`)
	_, err := tk.Exec(`insert into t1 values("asfasdfsajhlkhlksdaf");`)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue)
}

func (s *testSuite3) TestInsertDateTimeWithTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec(`use test;`)
	tk.MustExec(`set time_zone="+09:00";`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (id int, c1 datetime not null default CURRENT_TIMESTAMP);`)
	tk.MustExec(`set TIMESTAMP = 1234;`)
	tk.MustExec(`insert t (id) values (1);`)

	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 1970-01-01 09:20:34`,
	))
}

func (s *testSuite3) TestInsertZeroYear(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a year(4));`)
	tk.MustExec(`insert into t1 values(0000),(00),("0000"),("000"), ("00"), ("0"), (79), ("79");`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows(
		`0`,
		`0`,
		`0`,
		`2000`,
		`2000`,
		`2000`,
		`1979`,
		`1979`,
	))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(f_year year NOT NULL DEFAULT '0000')ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`insert into t values();`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`0`,
	))
	tk.MustExec(`insert into t values('0000');`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`0`,
		`0`,
	))
}

func (s *testSuite3) TestPartitionInsertOnDuplicate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int,primary key(a,b)) partition by range(a) (partition p0 values less than (100),partition p1 values less than (1000))`)
	tk.MustExec(`insert into t1 set a=1, b=1`)
	tk.MustExec(`insert into t1 set a=1,b=1 on duplicate key update a=1,b=1`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 1"))

	tk.MustExec(`create table t2 (a int,b int,primary key(a,b)) partition by hash(a) partitions 4`)
	tk.MustExec(`insert into t2 set a=1,b=1;`)
	tk.MustExec(`insert into t2 set a=1,b=1 on duplicate key update a=1,b=1`)
	tk.MustQuery(`select * from t2`).Check(testkit.Rows("1 1"))

	tk.MustExec(`CREATE TABLE t3 (a int, b int, c int, d int, e int,
  PRIMARY KEY (a,b),
  UNIQUE KEY (b,c,d)
) PARTITION BY RANGE ( b ) (
  PARTITION p0 VALUES LESS THAN (4),
  PARTITION p1 VALUES LESS THAN (7),
  PARTITION p2 VALUES LESS THAN (11)
)`)
	tk.MustExec("insert into t3 values (1,2,3,4,5)")
	tk.MustExec("insert into t3 values (1,2,3,4,5),(6,2,3,4,6) on duplicate key update e = e + values(e)")
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 2 3 4 16"))
}

func (s *testSuite3) TestBit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a bit(3))`)
	_, err := tk.Exec("insert into t1 values(-1)")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Matches, ".*Data too long for column 'a' at.*")
	_, err = tk.Exec("insert into t1 values(9)")
	c.Assert(err.Error(), Matches, ".*Data too long for column 'a' at.*")

	tk.MustExec(`create table t64 (a bit(64))`)
	tk.MustExec("insert into t64 values(-1)")
	tk.MustExec("insert into t64 values(18446744073709551615)")      // 2^64 - 1
	_, err = tk.Exec("insert into t64 values(18446744073709551616)") // z^64
	c.Assert(err.Error(), Matches, ".*Out of range value for column 'a' at.*")

}

func (s *testSuite3) TestAllocateContinuousRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int, key I_a(a));`)
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tk := testkit.NewTestKitWithInit(c, s.store)
			for j := 0; j < 10; j++ {
				k := strconv.Itoa(idx*100 + j)
				sql := "insert into t1(a,b) values (" + k + ", 2)"
				for t := 0; t < 20; t++ {
					sql += ",(" + k + ",2)"
				}
				tk.MustExec(sql)
				q := "select _tidb_rowid from t1 where a=" + k
				fmt.Printf("query: %v\n", q)
				rows := tk.MustQuery(q).Rows()
				c.Assert(len(rows), Equals, 21)
				last := 0
				for _, r := range rows {
					c.Assert(len(r), Equals, 1)
					v, err := strconv.Atoi(r[0].(string))
					c.Assert(err, Equals, nil)
					if last > 0 {
						c.Assert(last+1, Equals, v)
					}
					last = v
				}
			}
		}(i)
	}
	wg.Wait()
}
