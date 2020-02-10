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

package mocktikv

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

var _ executor = &hashAggExec{}

type hashAggExec struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxsMap        aggCtxsMapper
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	groups            map[string]struct{}
	groupKeys         [][]byte
	groupKeyRows      [][][]byte
	executed          bool
	currGroupIdx      int
	count             int64

	src executor
}

func (e *hashAggExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *hashAggExec) GetSrcExec() executor {
	return e.src
}

func (e *hashAggExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *hashAggExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *hashAggExec) innerNext(ctx context.Context) (bool, error) {
	values, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if values == nil {
		return false, nil
	}
	err = e.aggregate(values)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *hashAggExec) Next(ctx context.Context) (value [][]byte, err error) {
	e.count++
	if e.aggCtxsMap == nil {
		e.aggCtxsMap = make(aggCtxsMapper)
	}
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}

	if e.currGroupIdx >= len(e.groups) {
		return nil, nil
	}
	gk := e.groupKeys[e.currGroupIdx]
	value = make([][]byte, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		partialResults := agg.GetPartialResult(aggCtxs[i])
		for _, result := range partialResults {
			data, err := codec.EncodeValue(e.evalCtx.sc, nil, result)
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = append(value, data)
		}
	}
	value = append(value, e.groupKeyRows[e.currGroupIdx]...)
	e.currGroupIdx++

	return value, nil
}

func (e *hashAggExec) getGroupKey() ([]byte, [][]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil, nil
	}
	bufLen := 0
	row := make([][]byte, 0, length)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.evalCtx.sc, nil, v)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		bufLen += len(b)
		row = append(row, b)
	}
	buf := make([]byte, 0, bufLen)
	for _, col := range row {
		buf = append(buf, col...)
	}
	return buf, row, nil
}

// aggregate updates aggregate functions with row.
func (e *hashAggExec) aggregate(value [][]byte) error {
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	// Get group key.
	gk, gbyKeyRow, err := e.getGroupKey()
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
		e.groupKeyRows = append(e.groupKeyRows, gbyKeyRow)
	}
	// Update aggregate expressions.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.Update(aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *hashAggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.evalCtx.sc))
		}
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}
