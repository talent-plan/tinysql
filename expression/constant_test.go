// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"sort"
	"strings"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newColumn(id int) *Column {
	return newColumnWithType(id, types.NewFieldType(mysql.TypeLonglong))
}

func newColumnWithType(id int, t *types.FieldType) *Column {
	return &Column{
		UniqueID: int64(id),
		RetType:  t,
	}
}

func newLonglong(value int64) *Constant {
	return &Constant{
		Value:   types.NewIntDatum(value),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func newFunction(funcName string, args ...Expression) Expression {
	typeLong := types.NewFieldType(mysql.TypeLonglong)
	return NewFunctionInternal(mock.NewContext(), funcName, typeLong, args...)
}

func (*testExpressionSuite) TestConstantPropagation(c *C) {
	tests := []struct {
		solver     []PropagateConstantSolver
		conditions []Expression
		result     string
	}{
		{
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newColumn(2)),
				newFunction(ast.EQ, newColumn(2), newColumn(3)),
				newFunction(ast.EQ, newColumn(3), newLonglong(1)),
				newFunction(ast.LogicOr, newLonglong(1), newColumn(0)),
			},
			result: "1, eq(Column#0, 1), eq(Column#1, 1), eq(Column#2, 1), eq(Column#3, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.NE, newColumn(2), newLonglong(2)),
			},
			result: "eq(Column#0, 1), eq(Column#1, 1), ne(Column#2, 2)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(1), newLonglong(1)),
				newFunction(ast.EQ, newColumn(2), newColumn(3)),
				newFunction(ast.GE, newColumn(2), newLonglong(2)),
				newFunction(ast.NE, newColumn(2), newLonglong(4)),
				newFunction(ast.NE, newColumn(3), newLonglong(5)),
			},
			result: "eq(Column#0, 1), eq(Column#1, 1), eq(Column#2, Column#3), ge(Column#2, 2), ge(Column#3, 2), ne(Column#2, 4), ne(Column#2, 5), ne(Column#3, 4), ne(Column#3, 5)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.EQ, newColumn(0), newColumn(2)),
				newFunction(ast.GE, newColumn(1), newLonglong(0)),
			},
			result: "eq(Column#0, Column#1), eq(Column#0, Column#2), ge(Column#0, 0), ge(Column#1, 0), ge(Column#2, 0)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.GT, newColumn(0), newLonglong(2)),
				newFunction(ast.GT, newColumn(1), newLonglong(3)),
				newFunction(ast.LT, newColumn(0), newLonglong(1)),
				newFunction(ast.GT, newLonglong(2), newColumn(1)),
			},
			result: "eq(Column#0, Column#1), gt(2, Column#0), gt(2, Column#1), gt(Column#0, 2), gt(Column#0, 3), gt(Column#1, 2), gt(Column#1, 3), lt(Column#0, 1), lt(Column#1, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver(), pgSolver2{}},
			conditions: []Expression{
				newFunction(ast.EQ, newLonglong(1), newColumn(0)),
				newLonglong(0),
			},
			result: "0",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.In, newColumn(0), newLonglong(1), newLonglong(2)),
				newFunction(ast.In, newColumn(1), newLonglong(3), newLonglong(4)),
			},
			result: "eq(Column#0, Column#1), in(Column#0, 1, 2), in(Column#0, 3, 4), in(Column#1, 1, 2), in(Column#1, 3, 4)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newFunction(ast.Mul, newColumn(0), newColumn(0)), newLonglong(50)),
			},
			result: "eq(Column#0, Column#1), le(mul(Column#0, Column#0), 50), le(mul(Column#1, Column#1), 50)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newColumn(0), newColumn(1)),
				newFunction(ast.LE, newColumn(0), newFunction(ast.Plus, newColumn(1), newLonglong(1))),
			},
			result: "eq(Column#0, Column#1), le(Column#0, plus(Column#0, 1)), le(Column#0, plus(Column#1, 1)), le(Column#1, plus(Column#1, 1))",
		},
	}
	for _, tt := range tests {
		for _, solver := range tt.solver {
			ctx := mock.NewContext()
			conds := make([]Expression, 0, len(tt.conditions))
			for _, cd := range tt.conditions {
				conds = append(conds, FoldConstant(cd))
			}
			newConds := solver.PropagateConstant(ctx, conds)
			var result []string
			for _, v := range newConds {
				result = append(result, v.String())
			}
			sort.Strings(result)
			c.Assert(strings.Join(result, ", "), Equals, tt.result, Commentf("different for expr %s", tt.conditions))
		}
	}
}

func (*testExpressionSuite) TestConstantFolding(c *C) {
	tests := []struct {
		condition Expression
		result    string
	}{
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newLonglong(1), newLonglong(2))),
			result:    "lt(Column#0, 3)",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newColumn(0), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(Column#0, 0)",
		},
		{
			condition: newFunction(ast.LT, newColumn(0), newFunction(ast.Plus, newColumn(1), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(Column#0, plus(Column#1, 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}
