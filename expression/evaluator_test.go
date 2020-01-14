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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = SerialSuites(&testEvaluatorSerialSuites{})
var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	testleak.BeforeTest()
	defer testleak.AfterTestT(t)

	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	TestingT(t)
}

type testEvaluatorSuite struct {
	*parser.Parser
	ctx sessionctx.Context
}

type testEvaluatorSerialSuites struct {
	*parser.Parser
}

func (s *testEvaluatorSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
	s.ctx.GetSessionVars().StmtCtx.TimeZone = time.Local
	s.ctx.GetSessionVars().SetSystemVar("max_allowed_packet", "67108864")
}

func (s *testEvaluatorSuite) TearDownSuite(c *C) {
}

func (s *testEvaluatorSuite) SetUpTest(c *C) {
	s.ctx.GetSessionVars().PlanColumnID = 0
}

func (s *testEvaluatorSuite) TearDownTest(c *C) {
	s.ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
}

func (s *testEvaluatorSuite) kindToFieldType(kind byte) types.FieldType {
	ft := types.FieldType{}
	switch kind {
	case types.KindNull:
		ft.Tp = mysql.TypeNull
	case types.KindInt64:
		ft.Tp = mysql.TypeLonglong
	case types.KindUint64:
		ft.Tp = mysql.TypeLonglong
		ft.Flag |= mysql.UnsignedFlag
	case types.KindMinNotNull:
		ft.Tp = mysql.TypeLonglong
	case types.KindMaxValue:
		ft.Tp = mysql.TypeLonglong
	case types.KindFloat32:
		ft.Tp = mysql.TypeDouble
	case types.KindFloat64:
		ft.Tp = mysql.TypeDouble
	case types.KindString:
		ft.Tp = mysql.TypeVarString
	case types.KindBytes:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlEnum:
		ft.Tp = mysql.TypeEnum
	case types.KindMysqlSet:
		ft.Tp = mysql.TypeSet
	case types.KindInterface:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlDecimal:
		ft.Tp = mysql.TypeNewDecimal
	case types.KindMysqlDuration:
		ft.Tp = mysql.TypeDuration
	case types.KindMysqlTime:
		ft.Tp = mysql.TypeDatetime
	case types.KindBinaryLiteral:
		ft.Tp = mysql.TypeVarString
		ft.Charset = charset.CharsetBin
		ft.Collate = charset.CollationBin
	case types.KindMysqlBit:
		ft.Tp = mysql.TypeBit
	case types.KindMysqlJSON:
		ft.Tp = mysql.TypeJSON
	}
	return ft
}

func (s *testEvaluatorSuite) datumsToConstants(datums []types.Datum) []Expression {
	constants := make([]Expression, 0, len(datums))
	for _, d := range datums {
		ft := s.kindToFieldType(d.Kind())
		ft.Flen, ft.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
		constants = append(constants, &Constant{Value: d, RetType: &ft})
	}
	return constants
}

func (s *testEvaluatorSuite) primitiveValsToConstants(args []interface{}) []Expression {
	cons := s.datumsToConstants(types.MakeDatums(args...))
	for i, arg := range args {
		types.DefaultTypeForValue(arg, cons[i].GetType())
	}
	return cons
}

func (s *testEvaluatorSuite) TestBinopLogic(c *C) {
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{nil, ast.LogicAnd, 1, nil},
		{nil, ast.LogicAnd, 0, 0},
		{nil, ast.LogicOr, 1, 1},
		{nil, ast.LogicOr, 0, nil},
		{nil, ast.LogicXor, 1, nil},
		{nil, ast.LogicXor, 0, nil},
		{1, ast.LogicAnd, 0, 0},
		{1, ast.LogicAnd, 1, 1},
		{1, ast.LogicOr, 0, 1},
		{1, ast.LogicOr, 1, 1},
		{0, ast.LogicOr, 0, 0},
		{1, ast.LogicXor, 0, 1},
		{1, ast.LogicXor, 1, 0},
		{0, ast.LogicXor, 0, 0},
		{0, ast.LogicXor, 1, 1},
	}
	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		switch x := t.ret.(type) {
		case nil:
			c.Assert(v.Kind(), Equals, types.KindNull)
		case int:
			c.Assert(v, testutil.DatumEquals, types.NewDatum(int64(x)))
		}
	}
}

func (s *testEvaluatorSuite) TestBinopBitop(c *C) {
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{1, ast.And, 1, 1},
		{1, ast.Or, 1, 1},
		{1, ast.Xor, 1, 0},
		{1, ast.LeftShift, 1, 2},
		{2, ast.RightShift, 1, 1},
		{nil, ast.And, 1, nil},
		{1, ast.And, nil, nil},
		{nil, ast.Or, 1, nil},
		{nil, ast.Xor, 1, nil},
		{nil, ast.LeftShift, 1, nil},
		{nil, ast.RightShift, 1, nil},
	}

	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		switch x := t.ret.(type) {
		case nil:
			c.Assert(v.Kind(), Equals, types.KindNull)
		case int:
			c.Assert(v, testutil.DatumEquals, types.NewDatum(uint64(x)))
		}
	}
}

func (s *testEvaluatorSuite) TestUnaryOp(c *C) {
	tbl := []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		// test Minus.
		{nil, ast.UnaryMinus, nil},
		{float64(1.0), ast.UnaryMinus, float64(-1.0)},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{uint64(1), ast.UnaryMinus, -int64(1)},
	}
	for i, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(t.result), Commentf("%d", i))
	}

	tbl = []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		{types.NewDecFromInt(1), ast.UnaryMinus, types.NewDecFromInt(-1)},
		{types.ZeroDuration, ast.UnaryMinus, new(types.MyDecimal)},
		{types.Time{Time: types.FromGoTime(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)), Type: mysql.TypeDatetime, Fsp: 0}, ast.UnaryMinus, types.NewDecFromInt(-20091110230000)},
	}

	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		expect := types.NewDatum(t.result)
		ret, err := result.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &expect)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, 0, Commentf("%v %s", t.arg, t.op))
	}
}
