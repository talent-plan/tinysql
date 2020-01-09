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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = Suite(&testEvalSuite{})

type testEvalSuite struct {
	colID int64
}

func (s *testEvalSuite) SetUpSuite(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) allocColID() int64 {
	s.colID++
	return s.colID
}

func (s *testEvalSuite) TearDownTest(c *C) {
	s.colID = 0
}

func (s *testEvalSuite) TestPBToExpr(c *C) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)
	ds := []types.Datum{types.NewIntDatum(1), types.NewUintDatum(1), types.NewFloat64Datum(1),
		types.NewDecimalDatum(newMyDecimal(c, "1")), types.NewDurationDatum(newDuration(time.Second))}

	for _, d := range ds {
		expr := datumExpr(c, d)
		expr.Val = expr.Val[:len(expr.Val)/2]
		_, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, NotNil)
	}

	expr := &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp: tipb.ExprType_ValueList,
			},
		},
	}
	_, err := PBToExpr(expr, fieldTps, sc)
	c.Assert(err, IsNil)

	val := make([]byte, 0, 32)
	val = codec.EncodeInt(val, 1)
	expr = &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp:  tipb.ExprType_ValueList,
				Val: val[:len(val)/2],
			},
		},
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	c.Assert(err, NotNil)

	expr = &tipb.Expr{
		Tp: tipb.ExprType_ScalarFunc,
		Children: []*tipb.Expr{
			{
				Tp:  tipb.ExprType_ValueList,
				Val: val,
			},
		},
		Sig:       tipb.ScalarFuncSig_AbsInt,
		FieldType: ToPBFieldType(newIntFieldType()),
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	c.Assert(err, NotNil)
}

func datumExpr(c *C, d types.Datum) *tipb.Expr {
	expr := new(tipb.Expr)
	switch d.Kind() {
	case types.KindInt64:
		expr.Tp = tipb.ExprType_Int64
		expr.Val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		expr.Tp = tipb.ExprType_Uint64
		expr.Val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		expr.Tp = tipb.ExprType_String
		expr.Val = d.GetBytes()
	case types.KindBytes:
		expr.Tp = tipb.ExprType_Bytes
		expr.Val = d.GetBytes()
	case types.KindFloat32:
		expr.Tp = tipb.ExprType_Float32
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		expr.Tp = tipb.ExprType_Float64
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		expr.Tp = tipb.ExprType_MysqlDuration
		expr.Val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		expr.Tp = tipb.ExprType_MysqlDecimal
		var err error
		expr.Val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		c.Assert(err, IsNil)
	case types.KindMysqlJSON:
		expr.Tp = tipb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		c.Assert(err, IsNil)
	case types.KindMysqlTime:
		expr.Tp = tipb.ExprType_MysqlTime
		var err error
		expr.Val, err = codec.EncodeMySQLTime(nil, d.GetMysqlTime(), mysql.TypeUnspecified, nil)
		c.Assert(err, IsNil)
		expr.FieldType = ToPBFieldType(newDateFieldType())
	default:
		expr.Tp = tipb.ExprType_Null
	}
	return expr
}

func newMyDecimal(c *C, s string) *types.MyDecimal {
	d := new(types.MyDecimal)
	c.Assert(d.FromString([]byte(s)), IsNil)
	return d
}

func newDuration(dur time.Duration) types.Duration {
	return types.Duration{
		Duration: dur,
		Fsp:      types.DefaultFsp,
	}
}

func newDateFieldType() *types.FieldType {
	return &types.FieldType{
		Tp: mysql.TypeDate,
	}
}

func newIntFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flen:    mysql.MaxIntWidth,
		Decimal: 0,
		Flag:    mysql.BinaryFlag,
	}
}

func newStringFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   mysql.TypeVarString,
		Flen: types.UnspecifiedLength,
	}
}
