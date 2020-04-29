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

package decoder

import (
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
)

// Column contains the info and generated expr of column.
type Column struct {
	Col *table.Column
}

// RowDecoder decodes a byte slice into datums and eval the generated column value.
type RowDecoder struct {
	colTypes map[int64]*types.FieldType
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl table.Table, decodeColMap map[int64]Column) *RowDecoder {
	colFieldMap := make(map[int64]*types.FieldType, len(decodeColMap))
	for id, col := range decodeColMap {
		colFieldMap[id] = &col.Col.ColumnInfo.FieldType
	}
	return &RowDecoder{
		colTypes: colFieldMap,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated column value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx sessionctx.Context, handle int64, b []byte, decodeLoc, sysLoc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	var err error
	if !rowcodec.IsNewFormat(b) {
		row, err = tablecodec.DecodeRowWithMap(b, rd.colTypes, decodeLoc, row)
	} else {
		row, err = tablecodec.DecodeRowWithMapNew(b, rd.colTypes, decodeLoc, row)
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

// BuildFullDecodeColMap build a map that contains [columnID -> struct{*table.Column, expression.Expression}] from
// indexed columns and all of its depending columns.
func BuildFullDecodeColMap(indexedCols []*table.Column) (map[int64]Column, error) {
	pendingCols := make([]*table.Column, len(indexedCols))
	copy(pendingCols, indexedCols)
	decodeColMap := make(map[int64]Column, len(pendingCols))

	for i := 0; i < len(pendingCols); i++ {
		col := pendingCols[i]
		if _, ok := decodeColMap[col.ID]; ok {
			continue // already discovered
		}

		decodeColMap[col.ID] = Column{
			Col: col,
		}
	}
	return decodeColMap, nil
}
