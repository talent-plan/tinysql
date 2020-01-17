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
	case types.KindInterface:
		ft.Tp = mysql.TypeVarString
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
}
