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

package ast_test

import (
	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb/parser/ast"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	// test non-SelectStmt
	var stmt Node = &DeleteStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &InsertStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &UpdateStmt{}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &ExplainStmt{}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Stmt: &InsertStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &InsertStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsFalse)

	stmt = &ExplainStmt{
		Stmt: &SelectStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

	stmt = &ExplainStmt{
		Analyze: true,
		Stmt:    &SelectStmt{},
	}
	c.Assert(IsReadOnly(stmt), IsTrue)

}
