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
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"math/rand"
	"sync"
	"testing"
)

type mockVecPlusIntBuiltinFunc struct {
	baseBuiltinFunc

	buf         *chunk.Column
	enableAlloc bool
}

func (p *mockVecPlusIntBuiltinFunc) allocBuf(n int) (*chunk.Column, error) {
	if p.enableAlloc {
		return p.bufAllocator.get(types.ETInt, n)
	}
	if p.buf == nil {
		p.buf = chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), n)
	}
	return p.buf, nil
}

func (p *mockVecPlusIntBuiltinFunc) releaseBuf(buf *chunk.Column) {
	if p.enableAlloc {
		p.bufAllocator.put(buf)
	}
}

func (p *mockVecPlusIntBuiltinFunc) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := p.allocBuf(n)
	if err != nil {
		return err
	}
	defer p.releaseBuf(buf)
	if err := p.args[0].VecEvalInt(p.ctx, input, result); err != nil {
		return err
	}
	if err := p.args[1].VecEvalInt(p.ctx, input, buf); err != nil {
		return err
	}
	dst64s := result.Int64s()
	src64s := buf.Int64s()
	for i := range dst64s {
		dst64s[i] += src64s[i]
	}
	for i := 0; i < n; i++ {
		if buf.IsNull(i) && !result.IsNull(i) {
			result.SetNull(i, true)
		}
	}
	return nil
}

func genMockVecPlusIntBuiltinFunc() (*mockVecPlusIntBuiltinFunc, *chunk.Chunk, *chunk.Column) {
	tp := types.NewFieldType(mysql.TypeLonglong)
	col1 := newColumn(0)
	col1.Index, col1.RetType = 0, tp
	col2 := newColumn(1)
	col2.Index, col2.RetType = 1, tp
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1, col2}, types.ETInt, types.ETInt, types.ETInt)
	plus := &mockVecPlusIntBuiltinFunc{bf, nil, false}
	input := chunk.New([]*types.FieldType{tp, tp}, 1024, 1024)
	buf := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, int64(i))
		input.AppendInt64(1, int64(i))
	}
	return plus, input, buf
}

func (s *testEvaluatorSuite) TestMockVecPlusInt(c *C) {
	plus, input, buf := genMockVecPlusIntBuiltinFunc()
	plus.enableAlloc = false
	c.Assert(plus.vecEvalInt(input, buf), IsNil)
	for i := 0; i < 1024; i++ {
		c.Assert(buf.IsNull(i), IsFalse)
		c.Assert(buf.GetInt64(i), Equals, int64(i*2))
	}

	plus.enableAlloc = true
	c.Assert(plus.vecEvalInt(input, buf), IsNil)
	for i := 0; i < 1024; i++ {
		c.Assert(buf.IsNull(i), IsFalse)
		c.Assert(buf.GetInt64(i), Equals, int64(i*2))
	}
}

func (s *testEvaluatorSuite) TestMockVecPlusIntParallel(c *C) {
	plus, input, buf := genMockVecPlusIntBuiltinFunc()
	plus.enableAlloc = true // it's concurrency-safe if enableAlloc is true
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			result := buf.CopyConstruct(nil)
			for i := 0; i < 200; i++ {
				c.Assert(plus.vecEvalInt(input, result), IsNil)
				for i := 0; i < 1024; i++ {
					c.Assert(result.IsNull(i), IsFalse)
					c.Assert(result.GetInt64(i), Equals, int64(i*2))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkColumnBufferAllocate(b *testing.B) {
	allocator := newLocalSliceBuffer(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ := allocator.get(types.ETInt, 1024)
		allocator.put(buf)
	}
}

func BenchmarkColumnBufferAllocateParallel(b *testing.B) {
	allocator := newLocalSliceBuffer(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, _ := allocator.get(types.ETInt, 1024)
			allocator.put(buf)
		}
	})
}

func BenchmarkPlusIntBufAllocator(b *testing.B) {
	plus, input, buf := genMockVecPlusIntBuiltinFunc()
	names := []string{"enable", "disable"}
	enable := []bool{true, false}
	for i := range enable {
		b.Run(names[i], func(b *testing.B) {
			plus.enableAlloc = enable[i]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := plus.vecEvalInt(input, buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type mockBuiltinDouble struct {
	baseBuiltinFunc

	evalType  types.EvalType
	enableVec bool
}

func (p *mockBuiltinDouble) vectorized() bool {
	return p.enableVec
}

func (p *mockBuiltinDouble) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := p.args[0].VecEvalInt(p.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := range i64s {
		i64s[i] <<= 1
	}
	return nil
}

func (p *mockBuiltinDouble) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := p.args[0].VecEvalReal(p.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		f64s[i] *= 2
	}
	return nil
}

func (p *mockBuiltinDouble) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	var buf *chunk.Column
	var err error
	if buf, err = p.baseBuiltinFunc.bufAllocator.get(p.evalType, input.NumRows()); err != nil {
		return err
	}
	if err := p.args[0].VecEvalString(p.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(input.NumRows())
	for i := 0; i < input.NumRows(); i++ {
		str := buf.GetString(i)
		result.AppendString(str + str)
	}
	p.baseBuiltinFunc.bufAllocator.put(buf)
	return nil
}

func (p *mockBuiltinDouble) evalInt(row chunk.Row) (int64, bool, error) {
	v, isNull, err := p.args[0].EvalInt(p.ctx, row)
	if err != nil {
		return 0, false, err
	}
	return v * 2, isNull, nil
}

func (p *mockBuiltinDouble) evalReal(row chunk.Row) (float64, bool, error) {
	v, isNull, err := p.args[0].EvalReal(p.ctx, row)
	if err != nil {
		return 0, false, err
	}
	return v * 2, isNull, nil
}

func (p *mockBuiltinDouble) evalString(row chunk.Row) (string, bool, error) {
	v, isNull, err := p.args[0].EvalString(p.ctx, row)
	if err != nil {
		return "", false, err
	}
	return v + v, isNull, nil
}

func convertETType(eType types.EvalType) (mysqlType byte) {
	switch eType {
	case types.ETInt:
		mysqlType = mysql.TypeLonglong
	case types.ETReal:
		mysqlType = mysql.TypeDouble
	case types.ETString:
		mysqlType = mysql.TypeVarString
	}
	return
}

func genMockRowDouble(eType types.EvalType, enableVec bool) (builtinFunc, *chunk.Chunk, *chunk.Column, error) {
	mysqlType := convertETType(eType)
	tp := types.NewFieldType(mysqlType)
	col1 := newColumn(1)
	col1.Index = 0
	col1.RetType = tp
	bf := newBaseBuiltinFuncWithTp(mock.NewContext(), []Expression{col1}, eType, eType)
	rowDouble := &mockBuiltinDouble{bf, eType, enableVec}
	input := chunk.New([]*types.FieldType{tp}, 1024, 1024)
	buf := chunk.NewColumn(types.NewFieldType(convertETType(eType)), 1024)
	for i := 0; i < 1024; i++ {
		switch eType {
		case types.ETInt:
			input.AppendInt64(0, int64(i))
		case types.ETReal:
			input.AppendFloat64(0, float64(i))
		case types.ETString:
			input.AppendString(0, fmt.Sprintf("%v", i))
		}
	}
	return rowDouble, input, buf, nil
}

func (s *testEvaluatorSuite) checkVecEval(c *C, eType types.EvalType, sel []int, result *chunk.Column) {
	if sel == nil {
		for i := 0; i < 1024; i++ {
			sel = append(sel, i)
		}
	}
	switch eType {
	case types.ETInt:
		i64s := result.Int64s()
		c.Assert(len(i64s), Equals, len(sel))
		for i, j := range sel {
			c.Assert(i64s[i], Equals, int64(j*2))
		}
	case types.ETReal:
		f64s := result.Float64s()
		c.Assert(len(f64s), Equals, len(sel))
		for i, j := range sel {
			c.Assert(f64s[i], Equals, float64(j*2))
		}
	case types.ETString:
		for i, j := range sel {
			c.Assert(result.GetString(i), Equals, fmt.Sprintf("%v%v", j, j))
		}
	}
}

func vecEvalType(f builtinFunc, eType types.EvalType, input *chunk.Chunk, result *chunk.Column) error {
	switch eType {
	case types.ETInt:
		return f.vecEvalInt(input, result)
	case types.ETReal:
		return f.vecEvalReal(input, result)
	case types.ETString:
		return f.vecEvalString(input, result)
	}
	panic("not implement")
}

func (s *testEvaluatorSuite) TestDoubleRow2Vec(c *C) {
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETString}
	for _, eType := range eTypes {
		rowDouble, input, result, err := genMockRowDouble(eType, false)
		c.Assert(err, IsNil)
		c.Assert(vecEvalType(rowDouble, eType, input, result), IsNil)
		s.checkVecEval(c, eType, nil, result)

		sel := []int{0}
		for {
			end := sel[len(sel)-1]
			gap := 1024 - end
			if gap < 10 {
				break
			}
			sel = append(sel, end+rand.Intn(gap-1)+1)
		}
		input.SetSel(sel)
		c.Assert(vecEvalType(rowDouble, eType, input, result), IsNil)

		s.checkVecEval(c, eType, sel, result)
	}
}

func (s *testEvaluatorSuite) TestDoubleVec2Row(c *C) {
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETString}
	for _, eType := range eTypes {
		rowDouble, input, result, err := genMockRowDouble(eType, true)
		result.Reset(eType)
		c.Assert(err, IsNil)
		it := chunk.NewIterator4Chunk(input)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			switch eType {
			case types.ETInt:
				v, _, err := rowDouble.evalInt(row)
				c.Assert(err, IsNil)
				result.AppendInt64(v)
			case types.ETReal:
				v, _, err := rowDouble.evalReal(row)
				c.Assert(err, IsNil)
				result.AppendFloat64(v)
			case types.ETString:
				v, _, err := rowDouble.evalString(row)
				c.Assert(err, IsNil)
				result.AppendString(v)
			}
		}
		s.checkVecEval(c, eType, nil, result)
	}
}

func evalRows(b *testing.B, it *chunk.Iterator4Chunk, eType types.EvalType, result *chunk.Column, rowDouble builtinFunc) {
	switch eType {
	case types.ETInt:
		for i := 0; i < b.N; i++ {
			result.Reset(eType)
			for r := it.Begin(); r != it.End(); r = it.Next() {
				v, isNull, err := rowDouble.evalInt(r)
				if err != nil {
					b.Fatal(err)
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendInt64(v)
				}
			}
		}
	case types.ETReal:
		for i := 0; i < b.N; i++ {
			result.Reset(eType)
			for r := it.Begin(); r != it.End(); r = it.Next() {
				v, isNull, err := rowDouble.evalReal(r)
				if err != nil {
					b.Fatal(err)
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendFloat64(v)
				}
			}
		}
	case types.ETString:
		for i := 0; i < b.N; i++ {
			result.Reset(eType)
			for r := it.Begin(); r != it.End(); r = it.Next() {
				v, isNull, err := rowDouble.evalString(r)
				if err != nil {
					b.Fatal(err)
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendString(v)
				}
			}
		}
	}
}

func BenchmarkMockDoubleRow(b *testing.B) {
	typeNames := []string{"Int", "Real", "String"}
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETString}
	for i, eType := range eTypes {
		b.Run(typeNames[i], func(b *testing.B) {
			rowDouble, input, result, _ := genMockRowDouble(eType, false)
			it := chunk.NewIterator4Chunk(input)
			b.ResetTimer()
			evalRows(b, it, eType, result, rowDouble)
		})
	}
}

func BenchmarkMockDoubleVec(b *testing.B) {
	typeNames := []string{"Int", "Real", "String"}
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETString}
	for i, eType := range eTypes {
		b.Run(typeNames[i], func(b *testing.B) {
			rowDouble, input, result, _ := genMockRowDouble(eType, true)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := vecEvalType(rowDouble, eType, input, result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func (s *testEvaluatorSuite) TestVectorizedCheck(c *C) {
	con := &Constant{}
	c.Assert(con.Vectorized(), IsTrue)
	col := &Column{}
	c.Assert(col.Vectorized(), IsTrue)
	cor := CorrelatedColumn{Column: *col}
	c.Assert(cor.Vectorized(), IsTrue)

	vecF, _, _, _ := genMockRowDouble(types.ETInt, true)
	sf := &ScalarFunction{Function: vecF}
	c.Assert(sf.Vectorized(), IsTrue)

	rowF, _, _, _ := genMockRowDouble(types.ETInt, false)
	sf = &ScalarFunction{Function: rowF}
	c.Assert(sf.Vectorized(), IsFalse)
}

func genFloat32Col() (*Column, *chunk.Chunk, *chunk.Column) {
	typeFloat := types.NewFieldType(mysql.TypeFloat)
	col := &Column{Index: 0, RetType: typeFloat}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{typeFloat}, 1024)
	for i := 0; i < 1024; i++ {
		chk.AppendFloat32(0, rand.Float32())
	}
	result := chunk.NewColumn(typeFloat, 1024)
	return col, chk, result
}

func (s *testEvaluatorSuite) TestFloat32ColVec(c *C) {
	col, chk, result := genFloat32Col()
	ctx := mock.NewContext()
	c.Assert(col.VecEvalReal(ctx, chk, result), IsNil)
	it := chunk.NewIterator4Chunk(chk)
	i := 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		v, _, err := col.EvalReal(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetFloat64(i))
		i++
	}

	// set Sel
	n := chk.NumRows()
	sel := make([]int, n/2)
	for i := 0; i < n; i += 2 {
		sel = append(sel, i)
	}
	chk.SetSel(sel)
	c.Assert(col.VecEvalReal(ctx, chk, result), IsNil)
	i = 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		v, _, err := col.EvalReal(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetFloat64(i))
		i++
	}

	// set an empty Sel
	sel = sel[:0]
	c.Assert(col.VecEvalReal(ctx, chk, result), IsNil)
}

func BenchmarkFloat32ColRow(b *testing.B) {
	col, chk, _ := genFloat32Col()
	ctx := mock.NewContext()
	it := chunk.NewIterator4Chunk(chk)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			if _, _, err := col.EvalReal(ctx, row); err != nil {
				b.Fatal(err)
			}

		}
	}
}

func BenchmarkFloat32ColVec(b *testing.B) {
	col, chk, result := genFloat32Col()
	ctx := mock.NewContext()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.VecEvalReal(ctx, chk, result); err != nil {
			b.Fatal(err)
		}
	}
}
