// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testAggFuncSuit{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testAggFuncSuit struct {
	ctx     sessionctx.Context
	rows    []chunk.Row
	nullRow chunk.Row
}

func generateRowData() []chunk.Row {
	rows := make([]chunk.Row, 0, 5050)
	for i := 1; i <= 100; i++ {
		for j := 0; j < i; j++ {
			rows = append(rows, chunk.MutRowFromDatums(types.MakeDatums(i)).ToRow())
		}
	}
	return rows
}

func (s *testAggFuncSuit) SetUpSuite(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.GetSessionVars().GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	s.rows = generateRowData()
	s.nullRow = chunk.MutRowFromDatums([]types.Datum{{}}).ToRow()
}

func (s *testAggFuncSuit) TestAvg(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{col})
	c.Assert(err, IsNil)
	avgFunc := desc.GetAggFunc(ctx)
	evalCtx := avgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := avgFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, row := range s.rows {
		err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = avgFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(67))
	err = avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = avgFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(67))
}

func (s *testAggFuncSuit) TestAvgFinalMode(c *C) {
	rows := make([][]types.Datum, 0, 100)
	for i := 1; i <= 100; i++ {
		rows = append(rows, types.MakeDatums(i, int64(i*i)))
	}
	ctx := mock.NewContext()
	cntCol := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	sumCol := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	aggFunc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{cntCol, sumCol})
	c.Assert(err, IsNil)
	aggFunc.Mode = FinalMode
	avgFunc := aggFunc.GetAggFunc(ctx)
	evalCtx := avgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	for _, row := range rows {
		err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, chunk.MutRowFromDatums(row).ToRow())
		c.Assert(err, IsNil)
	}
	result := avgFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(67))
}

func (s *testAggFuncSuit) TestSum(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{col})
	c.Assert(err, IsNil)
	sumFunc := desc.GetAggFunc(ctx)
	evalCtx := sumFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := sumFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, row := range s.rows {
		err := sumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = sumFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(338350))
	err = sumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = sumFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(338350))
	partialResult := sumFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(338350))
}

func (s *testAggFuncSuit) TestCount(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{col})
	c.Assert(err, IsNil)
	countFunc := desc.GetAggFunc(ctx)
	evalCtx := countFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(0))

	for _, row := range s.rows {
		err := countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	err = countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	partialResult := countFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(5050))
}

func (s *testAggFuncSuit) TestFirstRow(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	c.Assert(err, IsNil)
	firstRowFunc := desc.GetAggFunc(ctx)
	evalCtx := firstRowFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	row := chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result := firstRowFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(2)).ToRow()
	err = firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = firstRowFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := firstRowFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))
}

func (s *testAggFuncSuit) TestMaxMin(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{col})
	c.Assert(err, IsNil)
	maxFunc := desc.GetAggFunc(ctx)
	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncMin, []expression.Expression{col})
	c.Assert(err, IsNil)
	minFunc := desc.GetAggFunc(ctx)
	maxEvalCtx := maxFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)
	minEvalCtx := minFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.IsNull(), IsTrue)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.IsNull(), IsTrue)

	row := chunk.MutRowFromDatums(types.MakeDatums(2))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row.SetDatum(0, types.NewIntDatum(3))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row.SetDatum(0, types.NewIntDatum(1))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))

	row.SetDatum(0, types.NewDatum(nil))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))
	partialResult := minFunc.GetPartialResult(minEvalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(1))
}
