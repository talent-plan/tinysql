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
	"bytes"
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ executor = &tableScanExec{}
	_ executor = &indexScanExec{}
	_ executor = &selectionExec{}
	_ executor = &limitExec{}
	_ executor = &topNExec{}
)

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	ResetCounts()
	Counts() []int64
	Next(ctx context.Context) ([][]byte, error)
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs    map[int64]int
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore MVCCStore
	cursor    int
	seekKey   []byte
	start     int
	counts    []int64

	src executor

	rd *rowcodec.BytesDecoder
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *tableScanExec) GetSrcExec() executor {
	return e.src
}

func (e *tableScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *tableScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *tableScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
			return value, nil
		}
		value, err = e.getRowFromRange(ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			e.seekKey = nil
			e.cursor++
			continue
		}
		if e.counts != nil {
			e.counts[e.cursor]++
		}
		return value, nil
	}

	return nil, nil
}

func (e *tableScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	handle, err := tablecodec.DecodeRowKey(ran.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, val, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = []byte(tablecodec.TruncateToRowKeyLen(kv.Key(pair.Key)))
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}

	handle, err := tablecodec.DecodeRowKey(pair.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, pair.Value, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

type indexScanExec struct {
	*tipb.IndexScan
	colsLen   int
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore MVCCStore
	cursor    int
	seekKey   []byte
	pkStatus  tablecodec.PrimaryKeyStatus
	start     int
	counts    []int64

	src executor
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) GetSrcExec() executor {
	return e.src
}

func (e *indexScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *indexScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *indexScanExec) isUnique() bool {
	return e.Unique != nil && *e.Unique
}

func (e *indexScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
		} else {
			value, err = e.getRowFromRange(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if value == nil {
				e.cursor++
				e.seekKey = nil
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor]++
			}
		}
		return value, nil
	}

	return nil, nil
}

// getRowFromPoint is only used for unique key.
func (e *indexScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	return tablecodec.DecodeIndexKV(ran.StartKey, val, e.colsLen, e.pkStatus)
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = pair.Key
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}

	return tablecodec.DecodeIndexKV(pair.Key, pair.Value, e.colsLen, e.pkStatus)
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	evalCtx           *evalContext
	src               executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	return e.src
}

func (e *selectionExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *selectionExec) Counts() []int64 {
	return e.src.Counts()
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, row []types.Datum, ctx *stmtctx.StatementContext) (bool, error) {
	for _, expr := range exprs {
		data, err := expr.Eval(chunk.MutRowFromDatums(row).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) Next(ctx context.Context) (value [][]byte, err error) {
	for {
		value, err = e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			return nil, nil
		}

		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := evalBool(e.conditions, e.row, e.evalCtx.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return value, nil
		}
	}
}

type topNExec struct {
	heap              *topNHeap
	evalCtx           *evalContext
	relatedColOffsets []int
	orderByExprs      []expression.Expression
	row               []types.Datum
	cursor            int
	executed          bool

	src executor
}

func (e *topNExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *topNExec) GetSrcExec() executor {
	return e.src
}

func (e *topNExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *topNExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *topNExec) innerNext(ctx context.Context) (bool, error) {
	value, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = e.evalTopN(value)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *topNExec) Next(ctx context.Context) (value [][]byte, err error) {
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
	if e.cursor >= len(e.heap.rows) {
		return nil, nil
	}
	sort.Sort(&e.heap.topNSorter)
	row := e.heap.rows[e.cursor]
	e.cursor++

	return row.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(value [][]byte) error {
	newRow := &sortRow{
		key: make([]types.Datum, len(value)),
	}
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range e.orderByExprs {
		newRow.key[i], err = expr.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.heap.tryToAddRow(newRow) {
		newRow.data = append(newRow.data, value...)
	}
	return errors.Trace(e.heap.err)
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	return e.src
}

func (e *limitExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *limitExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *limitExec) Next(ctx context.Context) (value [][]byte, err error) {
	if e.cursor >= e.limit {
		return nil, nil
	}

	value, err = e.src.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}
	e.cursor++
	return value, nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

// getRowData decodes raw byte slice to row data.
func getRowData(
	columns []*tipb.ColumnInfo,
	colIDs map[int64]int,
	handle int64,
	value []byte,
	rd *rowcodec.BytesDecoder,
) ([][]byte, error) {
	return rd.DecodeToBytes(colIDs, handle, value, nil)
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}
