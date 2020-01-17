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

package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
)

// All the following avg function implementations return the decimal result,
// which store the partial results in "partialResult4AvgInt64".
//
// "baseAvgDecimal" is wrapped by:
// - "avgOriginal4Int64"
// - "avgPartial4Int64"
type baseAvgInt64 struct {
	baseAggFunc
}

type partialResult4AvgInt64 struct {
	sum   int64
	count int64
}

func (e *baseAvgInt64) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4AvgInt64{})
}

func (e *baseAvgInt64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgInt64)(pr)
	p.sum = 0
	p.count = int64(0)
}

func (e *baseAvgInt64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgInt64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.sum/p.count)
	return nil
}

type avgOriginal4Int64 struct {
	baseAvgInt64
}

func (e *avgOriginal4Int64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgInt64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		newSum, err := types.AddInt64(p.sum, input)
		if err != nil {
			return err
		}
		p.sum = newSum
		p.count++
	}
	return nil
}

type avgPartial4Int64 struct {
	baseAvgInt64
}

func (e *avgPartial4Int64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgInt64)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		newSum, err := types.AddInt64(p.sum, inputSum)
		if err != nil {
			return err
		}
		p.sum = newSum
		p.count += inputCount
	}
	return nil
}

func (e *avgPartial4Int64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4AvgInt64)(src), (*partialResult4AvgInt64)(dst)
	if p1.count == 0 {
		return nil
	}
	newSum, err := types.AddInt64(p1.sum, p2.sum)
	if err != nil {
		return err
	}
	p2.sum = newSum
	p2.count += p1.count
	return nil
}

type partialResult4AvgDistinctInt64 struct {
	partialResult4AvgInt64
	valSet set.Int64Set
}

type avgOriginal4DistinctInt64 struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctInt64) AllocPartialResult() PartialResult {
	p := &partialResult4AvgDistinctInt64{
		valSet: set.NewInt64Set(),
	}
	return PartialResult(p)
}

func (e *avgOriginal4DistinctInt64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctInt64)(pr)
	p.sum = 0
	p.count = int64(0)
	p.valSet = set.NewInt64Set()
}

func (e *avgOriginal4DistinctInt64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgDistinctInt64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		newSum, err := types.AddInt64(p.sum, input)
		if err != nil {
			return err
		}
		p.sum = newSum
		p.count++
	}
	return nil
}

func (e *avgOriginal4DistinctInt64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctInt64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.sum/p.count)
	return nil
}

// All the following avg function implementations return the float64 result,
// which store the partial results in "partialResult4AvgFloat64".
//
// "baseAvgFloat64" is wrapped by:
// - "avgOriginal4Float64"
// - "avgPartial4Float64"
type baseAvgFloat64 struct {
	baseAggFunc
}

type partialResult4AvgFloat64 struct {
	sum   float64
	count int64
}

func (e *baseAvgFloat64) AllocPartialResult() PartialResult {
	return (PartialResult)(&partialResult4AvgFloat64{})
}

func (e *baseAvgFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgFloat64)(pr)
	p.sum = 0
	p.count = 0
}

func (e *baseAvgFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	}
	return nil
}

type avgOriginal4Float64 struct {
	baseAvgFloat64
}

func (e *avgOriginal4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		p.sum += input
		p.count++
	}
	return nil
}

type avgPartial4Float64 struct {
	baseAvgFloat64
}

func (e *avgPartial4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgFloat64)(pr)
	for _, row := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.sum += inputSum
		p.count += inputCount
	}
	return nil
}

func (e *avgPartial4Float64) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4AvgFloat64)(src), (*partialResult4AvgFloat64)(dst)
	p2.sum += p1.sum
	p2.count += p1.count
	return nil
}

type partialResult4AvgDistinctFloat64 struct {
	partialResult4AvgFloat64
	valSet set.Float64Set
}

type avgOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctFloat64) AllocPartialResult() PartialResult {
	p := &partialResult4AvgDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p)
}

func (e *avgOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	p.sum = float64(0)
	p.count = int64(0)
	p.valSet = set.NewFloat64Set()
}

func (e *avgOriginal4DistinctFloat64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		p.sum += input
		p.count++
		p.valSet.Insert(input)
	}
	return nil
}

func (e *avgOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	return nil
}
