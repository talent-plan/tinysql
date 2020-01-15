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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func evalBuiltinFunc(f builtinFunc, row chunk.Row) (d types.Datum, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch f.getRetTp().EvalType() {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = f.evalInt(row)
		if mysql.HasUnsignedFlag(f.getRetTp().Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = f.evalReal(row)
	case types.ETDecimal:
		res, isNull, err = f.evalDecimal(row)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = f.evalTime(row)
	case types.ETDuration:
		res, isNull, err = f.evalDuration(row)
	case types.ETJson:
		res, isNull, err = f.evalJSON(row)
	case types.ETString:
		res, isNull, err = f.evalString(row)
	}

	if isNull || err != nil {
		d.SetValue(nil)
		return d, err
	}
	d.SetValue(res)
	return
}

func (s *testEvaluatorSuite) TestIsNullFunc(c *C) {
	fc := funcs[ast.IsNull]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(1)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx sessionctx.Context, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", funcName)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(funcName),
		RetType:  f.getRetTp(),
		Function: f,
	}, nil
}
