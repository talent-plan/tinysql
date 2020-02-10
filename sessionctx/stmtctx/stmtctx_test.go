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

package stmtctx_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"testing"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type stmtctxSuit struct{}

var _ = Suite(&stmtctxSuit{})

func (s *stmtctxSuit) TestStatementContextPushDownFLags(c *C) {
	testCases := []struct {
		in  *stmtctx.StatementContext
		out uint64
	}{
		{&stmtctx.StatementContext{InInsertStmt: true}, 8},
		{&stmtctx.StatementContext{InDeleteStmt: true}, 16},
		{&stmtctx.StatementContext{InSelectStmt: true}, 32},
		{&stmtctx.StatementContext{IgnoreTruncate: true}, 1},
		{&stmtctx.StatementContext{TruncateAsWarning: true}, 2},
		{&stmtctx.StatementContext{OverflowAsWarning: true}, 64},
		{&stmtctx.StatementContext{IgnoreZeroInDate: true}, 128},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true}, 256},
		{&stmtctx.StatementContext{PadCharToFullLength: true}, 4},
		{&stmtctx.StatementContext{InSelectStmt: true, TruncateAsWarning: true}, 34},
		{&stmtctx.StatementContext{DividedByZeroAsWarning: true, IgnoreTruncate: true}, 257},
	}
	for _, tt := range testCases {
		got := tt.in.PushDownFlags()
		c.Assert(got, Equals, tt.out, Commentf("get %v, want %v", got, tt.out))
	}
}
