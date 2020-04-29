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
)

type partialResult4SumFloat64 struct {
	val    float64
	isNull bool
}

type partialResult4Int64 struct {
	val    int64
	isNull bool
}

type baseSumAggFunc struct {
	baseAggFunc
}

type sum4Float64 struct {
	baseSumAggFunc
}

func (e *sum4Float64) AllocPartialResult() PartialResult {
	p := new(partialResult4SumFloat64)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *sum4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *sum4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4SumFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		p.val += input
	}
	return nil
}

func (e *sum4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4SumFloat64)(src), (*partialResult4SumFloat64)(dst)
	if p1.isNull {
		return nil
	}
	p2.val += p1.val
	p2.isNull = false
	return nil
}

type sum4Int64 struct {
	baseSumAggFunc
}

func (e *sum4Int64) AllocPartialResult() PartialResult {
	p := new(partialResult4Int64)
	p.isNull = true
	return PartialResult(p)
}

func (e *sum4Int64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Int64)(pr)
	p.isNull = true
}

func (e *sum4Int64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Int64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *sum4Int64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Int64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}

		newSum, err := types.AddInt64(p.val, input)
		if err != nil {
			return err
		}
		p.val = newSum
	}
	return nil
}

func (e *sum4Int64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Int64)(src), (*partialResult4Int64)(dst)
	if p1.isNull {
		return nil
	}
	newSum, err := types.AddInt64(p1.val, p2.val)
	if err != nil {
		return err
	}
	p2.val = newSum
	p2.isNull = false
	return nil
}
