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

package executor

import (
	"context"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	tblID2Table map[int64]table.Table

	// tblColPosInfos stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see plannercore.TblColPosInfos
	tblColPosInfos plannercore.TblColPosInfoSlice
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	return e.deleteSingleTableByChunk(ctx)
}

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleIndex int, isExtraHandle bool, row []types.Datum) error {
	end := len(row)
	if isExtraHandle {
		end--
	}
	handle := row[handleIndex].GetInt64()
	err := e.removeRow(e.ctx, tbl, handle, row[:end])
	if err != nil {
		return err
	}

	return nil
}

func (e *DeleteExec) deleteSingleTableByChunk(ctx context.Context) error {
	var (
		tbl           table.Table
		isExtrahandle bool
		handleIndex   int
		rowCount      int
	)
	for _, info := range e.tblColPosInfos {
		tbl = e.tblID2Table[info.TblID]
		handleIndex = info.HandleOrdinal
		isExtrahandle = handleIsExtra(e.children[0].Schema().Columns[info.HandleOrdinal])
	}

	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	for {
		iter := chunk.NewIterator4Chunk(chk)

		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}

		for chunkRow := iter.Begin(); chunkRow != iter.End(); chunkRow = iter.Next() {
			datumRow := chunkRow.GetDatumRow(fields)
			err = e.deleteOneRow(tbl, handleIndex, isExtrahandle, datumRow)
			if err != nil {
				return err
			}
			rowCount++
		}
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	return nil
}

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h int64, data []types.Datum) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return err
	}
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	return e.children[0].Open(ctx)
}
