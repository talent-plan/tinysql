// Copyright 2019 PingCAP, Inc.
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
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestExplainCartesianJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")

	cases := []struct {
		sql             string
		isCartesianJoin bool
	}{
		{"explain select * from t t1, t t2", true},
		{"explain select * from t t1 where exists (select 1 from t t2 where t2.v > t1.v)", true},
		{"explain select * from t t1 where exists (select 1 from t t2 where t2.v in (t1.v+1, t1.v+2))", true},
		{"explain select * from t t1, t t2 where t1.v = t2.v", false},
	}
	for _, ca := range cases {
		rows := tk.MustQuery(ca.sql).Rows()
		ok := false
		for _, row := range rows {
			str := fmt.Sprintf("%v", row)
			if strings.Contains(str, "CARTESIAN") {
				ok = true
			}
		}

		c.Assert(ok, Equals, ca.isCartesianJoin)
	}
}
