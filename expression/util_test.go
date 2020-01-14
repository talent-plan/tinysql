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
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestBaseBuiltin(c *check.C) {
	ctx := mock.NewContext()
	bf := newBaseBuiltinFuncWithTp(ctx, nil, types.ETTimestamp)
	_, _, err := bf.evalInt(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalReal(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalString(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalDecimal(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalTime(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalDuration(chunk.Row{})
	c.Assert(err, check.NotNil)
	_, _, err = bf.evalJSON(chunk.Row{})
	c.Assert(err, check.NotNil)
}

func (s *testUtilSuite) TestGetUint64FromConstant(c *check.C) {
	con := &Constant{
		Value: types.NewDatum(nil),
	}
	_, isNull, ok := GetUint64FromConstant(con)
	c.Assert(ok, check.IsTrue)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{
		Value: types.NewIntDatum(-1),
	}
	_, _, ok = GetUint64FromConstant(con)
	c.Assert(ok, check.IsFalse)

	con.Value = types.NewIntDatum(1)
	num, isNull, ok := GetUint64FromConstant(con)
	c.Assert(ok, check.IsTrue)
	c.Assert(isNull, check.IsFalse)
	c.Assert(num, check.Equals, uint64(1))

	con.Value = types.NewUintDatum(1)
	num, _, _ = GetUint64FromConstant(con)
	c.Assert(num, check.Equals, uint64(1))
}

func (s *testUtilSuite) TestSetExprColumnInOperand(c *check.C) {
	col := &Column{RetType: newIntFieldType()}
	c.Assert(setExprColumnInOperand(col).(*Column).InOperand, check.IsTrue)

	f, err := funcs[ast.Abs].getFunction(mock.NewContext(), []Expression{col})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f}
	setExprColumnInOperand(fun)
	c.Assert(f.getArgs()[0].(*Column).InOperand, check.IsTrue)
}

func (s testUtilSuite) TestPopRowFirstArg(c *check.C) {
	c1, c2, c3 := &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}, &Column{RetType: newIntFieldType()}
	f, err := funcs[ast.RowFunc].getFunction(mock.NewContext(), []Expression{c1, c2, c3})
	c.Assert(err, check.IsNil)
	fun := &ScalarFunction{Function: f, FuncName: model.NewCIStr(ast.RowFunc), RetType: newIntFieldType()}
	fun2, err := PopRowFirstArg(mock.NewContext(), fun)
	c.Assert(err, check.IsNil)
	c.Assert(len(fun2.(*ScalarFunction).GetArgs()), check.Equals, 2)
}

func (s testUtilSuite) TestGetStrIntFromConstant(c *check.C) {
	col := &Column{}
	_, _, err := GetStringFromConstant(mock.NewContext(), col)
	c.Assert(err, check.NotNil)

	con := &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, err := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(err, check.IsNil)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newIntFieldType(), Value: types.NewIntDatum(1)}
	ret, _, _ := GetStringFromConstant(mock.NewContext(), con)
	c.Assert(ret, check.Equals, "1")

	con = &Constant{RetType: &types.FieldType{Tp: mysql.TypeNull}}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("abc")}
	_, isNull, _ = GetIntFromConstant(mock.NewContext(), con)
	c.Assert(isNull, check.IsTrue)

	con = &Constant{RetType: newStringFieldType(), Value: types.NewStringDatum("123")}
	num, _, _ := GetIntFromConstant(mock.NewContext(), con)
	c.Assert(num, check.Equals, 123)
}

func (s *testUtilSuite) TestPushDownNot(c *check.C) {
	ctx := mock.NewContext()
	col := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	// !((a=1||a=1)&&a=1)
	eqFunc := newFunction(ast.EQ, col, One)
	orFunc := newFunction(ast.LogicOr, eqFunc, eqFunc)
	andFunc := newFunction(ast.LogicAnd, orFunc, eqFunc)
	notFunc := newFunction(ast.UnaryNot, andFunc)
	// (a!=1&&a!=1)||a=1
	neFunc := newFunction(ast.NE, col, One)
	andFunc2 := newFunction(ast.LogicAnd, neFunc, neFunc)
	orFunc2 := newFunction(ast.LogicOr, andFunc2, neFunc)
	notFuncCopy := notFunc.Clone()
	ret := PushDownNot(ctx, notFunc)
	c.Assert(ret.Equal(ctx, orFunc2), check.IsTrue)
	c.Assert(notFunc.Equal(ctx, notFuncCopy), check.IsTrue)
}

func (s *testUtilSuite) TestFilter(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	result := make([]Expression, 0, 5)
	result = Filter(result, conditions, isLogicOrFunction)
	c.Assert(result, check.HasLen, 1)
}

func (s *testUtilSuite) TestFilterOutInPlace(c *check.C) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	remained, filtered := FilterOutInPlace(conditions, isLogicOrFunction)
	c.Assert(len(remained), check.Equals, 2)
	c.Assert(remained[0].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(remained[1].(*ScalarFunction).FuncName.L, check.Equals, "eq")
	c.Assert(len(filtered), check.Equals, 1)
	c.Assert(filtered[0].(*ScalarFunction).FuncName.L, check.Equals, "or")
}

func (s *testUtilSuite) TestHashGroupKey(c *check.C) {
	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for i := 0; i < len(tNames); i++ {
		ft := eType2FieldType(eTypes[i])
		if eTypes[i] == types.ETDecimal {
			ft.Flen = 0
		}
		colExpr := &Column{Index: 0, RetType: ft}
		input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
		fillColumnWithGener(eTypes[i], input, 0, nil)
		colBuf := chunk.NewColumn(ft, 1024)
		bufs := make([][]byte, 1024)
		for j := 0; j < 1024; j++ {
			bufs[j] = bufs[j][:0]
		}
		var err error
		err = VecEval(ctx, colExpr, input, colBuf)
		if err != nil {
			c.Fatal(err)
		}
		if bufs, err = codec.HashGroupKey(sc, 1024, colBuf, bufs, ft); err != nil {
			c.Fatal(err)
		}

		var buf []byte
		for j := 0; j < input.NumRows(); j++ {
			d, err := colExpr.Eval(input.GetRow(j))
			if err != nil {
				c.Fatal(err)
			}
			buf, err = codec.EncodeValue(sc, buf[:0], d)
			if err != nil {
				c.Fatal(err)
			}
			c.Assert(string(bufs[j]), check.Equals, string(buf))
		}
	}
}

func isLogicOrFunction(e Expression) bool {
	if f, ok := e.(*ScalarFunction); ok {
		return f.FuncName.L == ast.LogicOr
	}
	return false
}

func (s *testUtilSuite) TestDisableParseJSONFlag4Expr(c *check.C) {
	var expr Expression
	expr = &Column{RetType: newIntFieldType()}
	ft := expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &CorrelatedColumn{Column: Column{RetType: newIntFieldType()}}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsTrue)

	expr = &ScalarFunction{RetType: newIntFieldType()}
	ft = expr.GetType()
	ft.Flag |= mysql.ParseToJSONFlag
	DisableParseJSONFlag4Expr(expr)
	c.Assert(mysql.HasParseToJSONFlag(ft.Flag), check.IsFalse)
}

func BenchmarkExtractColumns(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractColumns(expr)
	}
	b.ReportAllocs()
}

func BenchmarkExprFromSchema(b *testing.B) {
	conditions := []Expression{
		newFunction(ast.EQ, newColumn(0), newColumn(1)),
		newFunction(ast.EQ, newColumn(1), newColumn(2)),
		newFunction(ast.EQ, newColumn(2), newColumn(3)),
		newFunction(ast.EQ, newColumn(3), newLonglong(1)),
		newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
	}
	expr := ComposeCNFCondition(mock.NewContext(), conditions...)
	schema := &Schema{Columns: ExtractColumns(expr)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExprFromSchema(expr, schema)
	}
	b.ReportAllocs()
}
