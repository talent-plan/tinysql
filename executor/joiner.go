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

package executor

import (
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ joiner = &leftOuterJoiner{}
	_ joiner = &rightOuterJoiner{}
	_ joiner = &innerJoiner{}
)

// joiner is used to generate join results according to the join type.
// A typical instruction flow is:
//
//     hasMatch, hasNull := false, false
//     for innerIter.Current() != innerIter.End() {
//         matched, isNull, err := j.tryToMatchInners(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//         hasNull = hasNull || isNull
//     }
//     if !hasMatch {
//         j.onMissMatch(hasNull, outer, chk)
//     }
//
// NOTE: This interface is **not** thread-safe.
type joiner interface {
	// tryToMatchInners tries to join an outer row with a batch of inner rows. When
	// 'inners.Len != 0' but all the joined rows are filtered, the outer row is
	// considered unmatched. Otherwise, the outer row is matched and some joined
	// rows are appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	// Note that when the outer row is considered unmatched, we need to differentiate
	// whether the join conditions return null or false, because that matters for
	// AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, and the result is reflected
	// by the second return value; for other join types, we always return false.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer row, and decide whether the outer row can be
	// matched with at lease one inner row.
	tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, isNull bool, err error)

	// tryToMatchOuters tries to join a batch of outer rows with one inner row.
	// It's used when the join is an outer join and the hash table is built
	// using the outer side.
	tryToMatchOuters(outer chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error)

	// onMissMatch operates on the unmatched outer row according to the join
	// type. An outer row can be considered miss matched if:
	//   1. it can not pass the filter on the outer table side.
	//   2. there is no inner row with the same join key.
	//   3. all the joined rows can not pass the filter on the join result.
	//
	// On these conditions, the caller calls this function to handle the
	// unmatched outer rows according to the current join type:
	//   1. 'LeftOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   2. 'RightOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   3. 'InnerJoin': ignores the unmatched outer row.
	onMissMatch(outer chunk.Row, chk *chunk.Chunk)

	// Clone deep copies a joiner.
	Clone() joiner
}

func newJoiner(ctx sessionctx.Context, joinType plannercore.JoinType,
	outerIsRight bool, defaultInner []types.Datum, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) joiner {
	base := baseJoiner{
		ctx:          ctx,
		conditions:   filter,
		outerIsRight: outerIsRight,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	colTypes := make([]*types.FieldType, 0, len(lhsColTypes)+len(rhsColTypes))
	colTypes = append(colTypes, lhsColTypes...)
	colTypes = append(colTypes, rhsColTypes...)
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	base.isNull = make([]bool, 0, chunk.InitialCapacity)
	if joinType == plannercore.LeftOuterJoin || joinType == plannercore.RightOuterJoin {
		innerColTypes := lhsColTypes
		if !outerIsRight {
			innerColTypes = rhsColTypes
		}
		base.initDefaultInner(innerColTypes, defaultInner)
	}
	switch joinType {
	case plannercore.LeftOuterJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &leftOuterJoiner{base}
	case plannercore.RightOuterJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &rightOuterJoiner{base}
	case plannercore.InnerJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &innerJoiner{base}
	}
	panic("unsupported join type in func newJoiner()")
}

type outerRowStatusFlag byte

const (
	outerRowUnmatched outerRowStatusFlag = iota
	outerRowMatched
	outerRowHasNull
)

type baseJoiner struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	shallowRow   chunk.MutRow
	selected     []bool
	isNull       []bool
	maxChunkSize int
}

func (j *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	j.defaultInner = mutableRow.ToRow()
}

func (j *baseJoiner) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

// filter is used to filter the result constructed by tryToMatchInners, the result is
// built by one outer row and multiple inner rows. The returned bool value
// indicates whether the outer row matches any inner rows.
func (j *baseJoiner) filter(input, output *chunk.Chunk, outerColsLen int) (bool, error) {
	var err error
	j.selected, err = expression.VectorizedFilter(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		return false, err
	}
	// Batch copies selected rows to output chunk.
	innerColOffset, outerColOffset := 0, input.NumCols()-outerColsLen
	if !j.outerIsRight {
		innerColOffset, outerColOffset = outerColsLen, 0
	}
	return chunk.CopySelectedJoinRows(input, innerColOffset, outerColOffset, j.selected, output)
}

// filterAndCheckOuterRowStatus is used to filter the result constructed by
// tryToMatchOuters, the result is built by multiple outer rows and one inner
// row. The returned outerRowStatusFlag slice value indicates the status of
// each outer row (matched/unmatched/hasNull).
func (j *baseJoiner) filterAndCheckOuterRowStatus(input, output *chunk.Chunk, innerColsLen int, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, _ error) {
	var err error
	j.selected, j.isNull, err = expression.VectorizedFilterConsiderNull(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected, j.isNull)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(j.selected); i++ {
		if j.isNull[i] {
			outerRowStatus[i] = outerRowHasNull
		} else if !j.selected[i] {
			outerRowStatus[i] = outerRowUnmatched
		}
	}

	// Batch copies selected rows to output chunk.
	innerColOffset, outerColOffset := 0, innerColsLen
	if !j.outerIsRight {
		innerColOffset, outerColOffset = input.NumCols()-innerColsLen, 0
	}

	_, err = chunk.CopySelectedJoinRows(input, innerColOffset, outerColOffset, j.selected, output)
	return outerRowStatus, err
}

func (j *baseJoiner) Clone() baseJoiner {
	base := baseJoiner{
		ctx:          j.ctx,
		conditions:   make([]expression.Expression, 0, len(j.conditions)),
		outerIsRight: j.outerIsRight,
		maxChunkSize: j.maxChunkSize,
		selected:     make([]bool, 0, len(j.selected)),
		isNull:       make([]bool, 0, len(j.isNull)),
	}
	for _, con := range j.conditions {
		base.conditions = append(base.conditions, con.Clone())
	}
	if j.chk != nil {
		base.chk = j.chk.CopyConstruct()
	} else {
		base.shallowRow = chunk.MutRow(j.shallowRow.ToRow())
	}
	if !j.defaultInner.IsEmpty() {
		base.defaultInner = j.defaultInner.CopyConstruct()
	}
	return base
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *leftOuterJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *leftOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, outer, inner)
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *leftOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), j.defaultInner)
}

func (j *leftOuterJoiner) Clone() joiner {
	return &leftOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *rightOuterJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *rightOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, inner, outer)
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *rightOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, j.defaultInner)
	chk.AppendPartialRow(j.defaultInner.Len(), outer)
}

func (j *rightOuterJoiner) Clone() joiner {
	return &rightOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *innerJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), chk.RequiredRows()-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *innerJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}
	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *innerJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
}

func (j *innerJoiner) Clone() joiner {
	return &innerJoiner{baseJoiner: j.baseJoiner.Clone()}
}
