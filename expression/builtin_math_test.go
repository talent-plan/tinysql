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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-tipb"
	"math"
	"math/rand"
)

func (s *testEvaluatorSuite) TestAbs(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{int64(-1), int64(1)},
		{float64(3.14), float64(3.14)},
		{float64(-3.14), float64(3.14)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Abs]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCeil(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	type testCase struct {
		arg    interface{}
		expect interface{}
		isNil  bool
		getErr bool
	}

	cases := []testCase{
		{nil, nil, true, false},
		{int64(1), int64(1), false, false},
		{float64(1.23), float64(2), false, false},
		{float64(-1.23), float64(-1), false, false},
		{"1.23", float64(2), false, false},
		{"-1.23", float64(-1), false, false},
		{"tidb", float64(0), false, false},
		{"1tidb", float64(1), false, false}}

	expressions := []Expression{
		&Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		},
		&Constant{
			Value:   types.NewFloat64Datum(float64(12.34)),
			RetType: types.NewFieldType(mysql.TypeFloat),
		},
	}

	runCasesOn := func(funcName string, cases []testCase, exps []Expression) {
		for _, test := range cases {
			f, err := newFunctionForTest(s.ctx, funcName, s.primitiveValsToConstants([]interface{}{test.arg})...)
			c.Assert(err, IsNil)

			result, err := f.Eval(chunk.Row{})
			if test.getErr {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
				if test.isNil {
					c.Assert(result.Kind(), Equals, types.KindNull)
				} else {
					c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
				}
			}
		}

		for _, exp := range exps {
			_, err := funcs[funcName].getFunction(s.ctx, []Expression{exp})
			c.Assert(err, IsNil)
		}
	}

	runCasesOn(ast.Ceil, cases, expressions)
	runCasesOn(ast.Ceiling, cases, expressions)
}

func (s *testEvaluatorSuite) TestRand(c *C) {
	fc := funcs[ast.Rand]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Less, float64(1))
	c.Assert(v.GetFloat64(), GreaterEqual, float64(0))

	// issue 3211
	f2, err := fc.getFunction(s.ctx, []Expression{&Constant{Value: types.NewIntDatum(20160101), RetType: types.NewFieldType(mysql.TypeLonglong)}})
	c.Assert(err, IsNil)
	randGen := rand.New(rand.NewSource(20160101))
	for i := 0; i < 3; i++ {
		v, err = evalBuiltinFunc(f2, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetFloat64(), Equals, randGen.Float64())
	}
}

func (s *testEvaluatorSuite) TestRound(c *C) {
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{-1.23}, -1},
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{-1.58}, -2},
		{[]interface{}{1.58}, 2},
		{[]interface{}{1.298, 1}, 1.3},
		{[]interface{}{1.298}, 1},
		{[]interface{}{1.298, 0}, 1},
		{[]interface{}{23.298, -1}, 20},
		{[]interface{}{newDec("-1.23")}, newDec("-1")},
		{[]interface{}{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]interface{}{newDec("-1.58")}, newDec("-2")},
		{[]interface{}{newDec("1.58")}, newDec("2")},
		{[]interface{}{newDec("1.58"), 1}, newDec("1.6")},
		{[]interface{}{newDec("23.298"), -1}, newDec("20")},
		{[]interface{}{nil, 2}, nil},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Round]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		switch f.(type) {
		case *builtinRoundWithFracIntSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracInt)
		case *builtinRoundWithFracDecSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracDec)
		case *builtinRoundWithFracRealSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracReal)
		case *builtinRoundIntSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundInt)
		case *builtinRoundDecSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundDec)
		case *builtinRoundRealSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundReal)
		}
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestTruncate(c *C) {
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{1.58, 0}, 1},
		{[]interface{}{1.298, 1}, 1.2},
		{[]interface{}{123.2, -1}, 120},
		{[]interface{}{123.2, 100}, 123.2},
		{[]interface{}{123.2, -100}, 0},
		{[]interface{}{123.2, -100}, 0},
		{[]interface{}{1.797693134862315708145274237317043567981e+308, 2},
			1.797693134862315708145274237317043567981e+308},
		{[]interface{}{newDec("-1.23"), 0}, newDec("-1")},
		{[]interface{}{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]interface{}{newDec("-11.23"), -1}, newDec("-10")},
		{[]interface{}{newDec("1.58"), 0}, newDec("1")},
		{[]interface{}{newDec("1.58"), 1}, newDec("1.5")},
		{[]interface{}{newDec("11.58"), -1}, newDec("10")},
		{[]interface{}{newDec("23.298"), -1}, newDec("20")},
		{[]interface{}{newDec("23.298"), -100}, newDec("0")},
		{[]interface{}{newDec("23.298"), 100}, newDec("23.298")},
		{[]interface{}{nil, 2}, nil},
		{[]interface{}{uint64(9223372036854775808), -10}, 9223372030000000000},
		{[]interface{}{9223372036854775807, -7}, 9223372036850000000},
		{[]interface{}{uint64(18446744073709551615), -10}, uint64(18446744070000000000)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Truncate]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCRC32(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{nil}, nil},
		{[]interface{}{""}, 0},
		{[]interface{}{-1}, 808273962},
		{[]interface{}{"-1"}, 808273962},
		{[]interface{}{"mysql"}, 2501908538},
		{[]interface{}{"MySQL"}, 3259397556},
		{[]interface{}{"hello"}, 907060870},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.CRC32]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestConv(c *C) {
	cases := []struct {
		args     []interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{"a", 16, 2}, "1010", false, false},
		{[]interface{}{"6E", 18, 8}, "172", false, false},
		{[]interface{}{"-17", 10, -18}, "-H", false, false},
		{[]interface{}{"-17", 10, 18}, "2D3FGB0B9CG4BD1H", false, false},
		{[]interface{}{nil, 10, 10}, "0", true, false},
		{[]interface{}{"+18aZ", 7, 36}, "1", false, false},
		{[]interface{}{"18446744073709551615", -10, 16}, "7FFFFFFFFFFFFFFF", false, false},
		{[]interface{}{"12F", -10, 16}, "C", false, false},
		{[]interface{}{"  FF ", 16, 10}, "255", false, false},
		{[]interface{}{"TIDB", 10, 8}, "0", false, false},
		{[]interface{}{"aa", 10, 2}, "0", false, false},
		{[]interface{}{" A", -10, 16}, "0", false, false},
		{[]interface{}{"a6a", 10, 8}, "0", false, false},
		{[]interface{}{"a6a", 1, 8}, "0", true, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Conv, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		tp := f.GetType()
		c.Assert(tp.Tp, Equals, mysql.TypeVarString)
		c.Assert(tp.Charset, Equals, charset.CharsetUTF8MB4)
		c.Assert(tp.Collate, Equals, charset.CollationUTF8MB4)
		c.Assert(tp.Flag, Equals, uint(0))

		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.expected)
			}
		}
	}

	v := []struct {
		s    string
		base int64
		ret  string
	}{
		{"-123456D1f", 5, "-1234"},
		{"+12azD", 16, "12a"},
		{"+", 12, ""},
	}
	for _, t := range v {
		r := getValidPrefix(t.s, t.base)
		c.Assert(r, Equals, t.ret)
	}

	_, err := funcs[ast.Conv].getFunction(s.ctx, []Expression{Zero, Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestPi(c *C) {
	f, err := funcs[ast.PI].getFunction(s.ctx, nil)
	c.Assert(err, IsNil)

	pi, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(pi, testutil.DatumEquals, types.NewDatum(math.Pi))
}
