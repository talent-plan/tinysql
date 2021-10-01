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

package executor

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

var _ Executor = &HashJoinExec{}

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerSideExec     Executor
	innerSideExec     Executor
	innerSideEstCount float64
	outerSideFilter   expression.CNFExprs
	outerKeys         []*expression.Column
	innerKeys         []*expression.Column

	// concurrency is the number of partition, build and join workers.
	concurrency  uint
	rowContainer *hashRowContainer
	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	// closeCh add a lock for closing executor.
	closeCh  chan struct{}
	joinType plannercore.JoinType

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	outerChkResourceCh chan *outerChkResource
	outerResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult

	prepared bool
}

// outerChkResource stores the result of the join outer side fetch worker,
// `dest` is for Chunk reuse: after join workers process the outer side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the outer side fetch worker will put new data into `chk` and write `chk` into dest.
type outerChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	close(e.closeCh)
	if e.prepared {
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.outerChkResourceCh != nil {
			close(e.outerChkResourceCh)
			for range e.outerChkResourceCh {
			}
		}
		for i := range e.outerResultChs {
			for range e.outerResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.outerChkResourceCh = nil
		e.joinChkResourceCh = nil
	}
	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.closeCh = make(chan struct{})
	e.joinWorkerWaitGroup = sync.WaitGroup{}
	return nil
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and outer the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		err := e.fetchAndBuildHashTable(ctx)
		if err != nil {
			return err
		}
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinExec) fetchAndBuildHashTable(ctx context.Context) error {
	// TODO: Implementing the building hash table stage.

	// In this stage, you'll read the data from the inner side executor of the join operator and
	// then use its data to build hash table.

	// You'll need to store the hash table in `e.rowContainer`
	// and you can call `newHashRowContainer` in `executor/hash_table.go` to build it.
	// In this stage you can only assign value for `e.rowContainer` without changing any value of the `HashJoinExec`.
	return nil
}

func (e *HashJoinExec) initializeForOuter() {
	// e.outerResultChs is for transmitting the chunks which store the data of
	// outerSideExec, it'll be written by outer side worker goroutine, and read by join
	// workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerSideExec chunks from
	// join workers to outerSideExec worker.
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerChkResourceCh <- &outerChkResource{
			chk:  newFirstChunk(e.outerSideExec),
			dest: e.outerResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
}

// fetchOuterSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterSideChunks(ctx context.Context) {
	for {
		var outerSideResource *outerChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case outerSideResource, ok = <-e.outerChkResourceCh:
			if !ok {
				return
			}
		}
		outerSideResult := outerSideResource.chk
		err := Next(ctx, e.outerSideExec, outerSideResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}

		if outerSideResult.NumRows() == 0 {
			return
		}

		outerSideResource.dest <- outerSideResult
	}
}

func (e *HashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForOuter()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchOuterSideChunks(ctx) }, e.handleOuterSideFetcherPanic)

	outerKeyColIdx := make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		outerKeyColIdx[i] = e.outerKeys[i].Index
	}

	// Start e.concurrency join workers to outer hash table and join build side and
	// outer side rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinWorker(workID, outerKeyColIdx) }, e.handleJoinWorkerPanic)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinExec) runJoinWorker(workerID uint, outerKeyColIdx []int) {
	// TODO: Implement the worker of probing stage.

	// In this method, you read the data from the channel e.outerResultChs[workerID].
	// Then use `e.join2Chunk` method get the joined result `joinResult`,
	// and put the `joinResult` into the channel `e.joinResultCh`.

	// You may pay attention to:
	//
	// - e.closeCh, this is a channel tells that the join can be terminated as soon as possible.
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	close(e.joinResultCh)
}

func (e *HashJoinExec) handleOuterSideFetcherPanic(r interface{}) {
	for i := range e.outerResultChs {
		close(e.outerResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) joinMatchedOuterSideRow2Chunk(workerID uint, outerKey uint64, outerSideRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	buildSideRows, err := e.rowContainer.GetMatchedRows(outerKey, outerSideRow, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		e.joiners[workerID].onMissMatch(outerSideRow, joinResult.chk)
		return true, joinResult
	}
	iter := chunk.NewIterator4Slice(buildSideRows)
	hasMatch := false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, _, err := e.joiners[workerID].tryToMatchInners(outerSideRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(outerSideRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinExec) join2Chunk(workerID uint, outerSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerSideFilter, chunk.NewIterator4Chunk(outerSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	hCtx.initHash(outerSideChk.NumRows())
	for _, i := range hCtx.keyColIdx {
		err = codec.HashChunkSelected(e.rowContainer.sc, hCtx.hashVals, outerSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull, selected)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}

	for i := range selected {
		if !selected[i] || hCtx.hasNull[i] { // process unmatched outer side rows
			e.joiners[workerID].onMissMatch(outerSideChk.GetRow(i), joinResult.chk)
		} else { // process matched outer side rows
			outerKey, outerRow := hCtx.hashVals[i].Sum64(), outerSideChk.GetRow(i)
			ok, joinResult = e.joinMatchedOuterSideRow2Chunk(workerID, outerKey, outerRow, hCtx, joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
