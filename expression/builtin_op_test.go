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

package expression

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestUnary(c *C) {
	cases := []struct {
		args     interface{}
		expected interface{}
		overflow bool
		getErr   bool
	}{
		{uint64(9223372036854775809), "-9223372036854775809", true, false},
		{uint64(9223372036854775810), "-9223372036854775810", true, false},
		{uint64(9223372036854775808), int64(-9223372036854775808), false, false},
		{int64(math.MinInt64), "9223372036854775808", true, false}, // --9223372036854775808
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = origin
	}()

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryMinus, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if !t.getErr {
			c.Assert(err, IsNil)
			if !t.overflow {
				c.Assert(d.GetValue(), Equals, t.expected)
			} else {
				c.Assert(d.GetMysqlDecimal().String(), Equals, t.expected)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}

	_, err := funcs[ast.UnaryMinus].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestIsTrueOrFalse(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	testCases := []struct {
		args    []interface{}
		isTrue  interface{}
		isFalse interface{}
	}{
		{
			args:    []interface{}{-12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{0},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{float64(0)},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{"aaa"},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{""},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{"0.3"},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{float64(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{types.NewDecFromFloatForTest(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{nil},
			isTrue:  0,
			isFalse: 0,
		},
	}

	for _, tc := range testCases {
		isTrueSig, err := funcs[ast.IsTruth].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isTrueSig, NotNil)

		isTrue, err := evalBuiltinFunc(isTrueSig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(isTrue, testutil.DatumEquals, types.NewDatum(tc.isTrue))
	}

	for _, tc := range testCases {
		isFalseSig, err := funcs[ast.IsFalsity].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isFalseSig, NotNil)

		isFalse, err := evalBuiltinFunc(isFalseSig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(isFalse, testutil.DatumEquals, types.NewDatum(tc.isFalse))
	}
}
