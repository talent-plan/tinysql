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

package expression

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinCompareCases = map[string][]vecExprBenchCase{
	ast.NE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong},
			},
		},
	},
	ast.IsNull: {},
	ast.LE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong},
			},
		},
	},
	ast.LT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong},
			},
		},
	},
	ast.GT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong},
			},
		},
	},
	ast.EQ: {},
	ast.GE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong},
			},
		},
	},
	ast.Date: {},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCompareEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinCompareCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCompareFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinCompareCases)
}

func BenchmarkVectorizedBuiltinCompareEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinCompareCases)
}

func BenchmarkVectorizedBuiltinCompareFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinCompareCases)
}
