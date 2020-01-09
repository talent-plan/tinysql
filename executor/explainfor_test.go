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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager1 struct {
	PS []*util.ProcessInfo
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager1) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager1) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager1) Kill(cid uint64, query bool) {

}

func (s *testSuite) TestIssue11124(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk2 := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table kankan1(id int, name text);")
	tk.MustExec("create table kankan2(id int, h1 text);")
	tk.MustExec("insert into kankan1 values(1, 'a'), (2, 'a');")
	tk.MustExec("insert into kankan2 values(2, 'z');")
	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'")
	tkRootProcess := tk.Se.ShowProcess()
	ps := []*util.ProcessInfo{tkRootProcess}
	tk.Se.SetSessionManager(&mockSessionManager1{PS: ps})
	tk2.Se.SetSessionManager(&mockSessionManager1{PS: ps})

	rs := tk.MustQuery("explain select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'").Rows()
	rs2 := tk2.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Rows()
	for i := range rs {
		c.Assert(rs[i], DeepEquals, rs2[i])
	}
}
