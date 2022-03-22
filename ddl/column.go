// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"sync/atomic"
)

// adjustColumnInfoInAddColumn is used to set the correct position of column info when adding column.
// 1. The added column was append at the end of tblInfo.Columns, due to ddl state was not public then.
//    It should be moved to the correct position when the ddl state to be changed to public.
// 2. The offset of column should also to be set to the right value.
func adjustColumnInfoInAddColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[len(oldCols)-1])
	newCols = append(newCols, oldCols[offset:len(oldCols)-1]...)
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(newCols); i++ {
		offsetChanged[newCols[i].Offset] = i
		newCols[i].Offset = i
	}
	newCols[offset].Offset = offset
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	tblInfo.Columns = newCols
}

// adjustColumnInfoInDropColumn is used to set the correct position of column info when dropping column.
// 1. The offset of column should to be set to the last of the columns.
// 2. The dropped column is moved to the end of tblInfo.Columns, due to it was not public any more.
func adjustColumnInfoInDropColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(oldCols); i++ {
		offsetChanged[oldCols[i].Offset] = i - 1
		oldCols[i].Offset = i - 1
	}
	oldCols[offset].Offset = len(oldCols) - 1
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[offset+1:]...)
	newCols = append(newCols, oldCols[offset])
	tblInfo.Columns = newCols
}

func createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right position in "Columns" when it state change to public.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols...)
	newCols = append(newCols, colInfo)

	tblInfo.Columns = newCols
	return colInfo, position, nil
}

func checkAddColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, *model.ColumnInfo, int, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}
	col := &model.ColumnInfo{}
	offset := 0
	err = job.DecodeArgs(col, &offset)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, 0, errors.Trace(err)
	}

	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			return nil, nil, nil, 0, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
		}
	}
	return tblInfo, columnInfo, col, offset, nil
}

/* onAddColumn handle add column job
 *  parameters:
 *      d *ddlCtx:      context of the ddl, but you won't use it here;
 *      t *meta.Meta:   handling meta information in a transaction;
 *      job *model.Job: a ddl job, here is an add column job;
 *  return value:
 *     ver :  current version;
 *     error: error information, but err will be tracked by other tool and no need to return;
 *  onAddColumn may need to follow the steps:
 *       1. Judge whether the job is rolling back to call `onDropColumn`;
 *       2. Get table and changed column information from job;
 *       3. Creat a column to add if column info is null;
 *       4. Determine the stage of the job.(stages order: none -> delete only -> write only -> reorg -> public);
 *       5. Handle the job in the stage and move to next stage;
 *       6. Update information and return version.
 *  Some hints that might be useful:
 *       - You need to complete the codes of each case.
 *       - You'll need to find the right place where to put the function `adjustColumnInfoInAddColumn`.
 *       - Pay attention to the state of schema and columnInfo.
 *       - Remember use `FinishTableJob` to finish the job.
 */
func onAddColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropColumn(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, columnInfo, col, offset, err := checkAddColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if columnInfo == nil {
		columnInfo, offset, err = createColumnInfo(tblInfo, col)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] run add column job", zap.String("job", job.String()), zap.Reflect("columnInfo", *columnInfo), zap.Int("offset", offset))
		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{columnInfo, offset}
		}
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	// TODO fill the codes of the each case.
	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		// To be filled
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateDeleteOnly:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteOnly:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteReorganization:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("column", columnInfo.State)
	}

	return ver, errors.Trace(err)
}

/*  onDropColumn handle drop column job
 *  parameters:
 *      t *meta.Meta:   handling meta information in a transaction;
 *      job *model.Job: a ddl job, here is a drop column job;
 *  return value:
 *     ver :  current version;
 *     error: error information, but err will be tracked by other tool and no need to return;
 *  onDropColumn may need to follow the steps:
 *       1. Get table and changed column information from job;
 *       2. Determine the stage of the job.(stages order: public -> write only -> delete only -> reorg );
 *       3. Handle the job in the stage and move to next stage;
 *       4. Update information and return version.
 *  Some hints that might be useful:
 *       - You need to complete the codes of each case.
 *       - You'll need to find the right place where to put the function `adjustColumnInfoInDropColumn`.
 *       - Also you'll need to take a corner case about the default value.
 *       - Think about how the not null property and default value will influence the `Drop Column` operation.
 *       - You can get a default value by `generateOriginDefaultValue`.
 *       - Remember use `FinishTableJob` to finish the job.
 *       - Don't forget to do something, if this is a rolling back add column job.
 */
func onDropColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, colInfo, err := checkDropColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := colInfo.State

	// TODO fill the codes of the each case.
	switch colInfo.State {
	case model.StatePublic:
		// To be filled
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfo.State)
	case model.StateWriteOnly:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteOnly:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteReorganization:
		// To be filled
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	default:
		err = errInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}
	return tblInfo, colInfo, nil
}

func onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumnDefaultValue(t, job, newCol, &newCol.Name)
}

func (w *worker) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	var modifyColumnTp byte
	err := job.DecodeArgs(newCol, oldColName, &modifyColumnTp)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return w.doModifyColumn(t, job, newCol, oldColName, modifyColumnTp)
}

// doModifyColumn updates the column information and reorders all columns.
func (w *worker) doModifyColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr, modifyColumnTp byte) (ver int64, _ error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, oldName.L)
	if job.IsRollingback() {
		ver, err = rollbackModifyColumnJob(t, tblInfo, job, oldCol, modifyColumnTp)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		return ver, nil
	}

	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
	}
	// If we want to rename the column name, we need to check whether it already exists.
	if newCol.Name.L != oldName.L {
		c := model.FindColumnInfo(tblInfo.Columns, newCol.Name.L)
		if c != nil {
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnExists.GenWithStackByArgs(newCol.Name)
		}
	}

	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		if val.(bool) {
			if newCol.State != model.StatePublic {
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	// Column from null to not null.
	if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		noPreventNullFlag := !mysql.HasPreventNullInsertFlag(oldCol.Flag)
		// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		err = modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, newCol.Name, oldCol.Tp != newCol.Tp)
		if err != nil {
			if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
				job.State = model.JobStateRollingback
			}
			return ver, err
		}
		// The column should get into prevent null status first.
		if noPreventNullFlag {
			return updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
		}
	}

	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset

	columnChanged := make(map[string]*model.ColumnInfo)
	columnChanged[oldName.L] = newCol

	if newPos == oldPos {
		tblInfo.Columns[newPos] = newCol
	} else {
		cols := tblInfo.Columns

		// Reorder columns in place.
		if newPos < oldPos {
			copy(cols[newPos+1:], cols[newPos:oldPos])
		} else {
			copy(cols[oldPos:], cols[oldPos+1:newPos+1])
		}
		cols[newPos] = newCol

		for i, col := range tblInfo.Columns {
			if col.Offset != i {
				columnChanged[col.Name.L] = col
				col.Offset = i
			}
		}
	}

	// Change offset and name in indices.
	for _, idx := range tblInfo.Indices {
		for _, c := range idx.Columns {
			if newCol, ok := columnChanged[c.Name.L]; ok {
				c.Name = newCol.Name
				c.Offset = newCol.Offset
			}
		}
	}

	ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

// checkForNullValue ensure there are no null values of the column of this table.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with mysql.
func checkForNullValue(ctx sessionctx.Context, isDataTruncated bool, schema, table, newCol model.CIStr, oldCols ...*model.ColumnInfo) error {
	colsStr := ""
	for i, col := range oldCols {
		if i == 0 {
			colsStr += "`" + col.Name.L + "` is null"
		} else {
			colsStr += " or `" + col.Name.L + "` is null"
		}
	}
	sql := fmt.Sprintf("select 1 from `%s`.`%s` where %s limit 1;", schema.L, table.L, colsStr)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		if isDataTruncated {
			return ErrWarnDataTruncated.GenWithStackByArgs(newCol.L, rowCount)
		}
		return errInvalidUseOfNull
	}
	return nil
}

func updateColumnDefaultValue(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldCol := model.FindColumnInfo(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newCol.Name, tblInfo.Name)
	}
	// The newCol's offset may be the value of the old schema version, so we can't use newCol directly.
	oldCol.DefaultValue = newCol.DefaultValue
	oldCol.Flag = newCol.Flag

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func isColumnWithIndex(colName string, indices []*model.IndexInfo) bool {
	for _, indexInfo := range indices {
		for _, col := range indexInfo.Columns {
			if col.Name.L == colName {
				return true
			}
		}
	}
	return false
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

func checkAddColumnTooManyColumns(colNum int) error {
	if uint32(colNum) > atomic.LoadUint32(&TableColumnCountLimit) {
		return errTooManyFields
	}
	return nil
}

// rollbackModifyColumnJob rollbacks the job when an error occurs.
func rollbackModifyColumnJob(t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, modifyColumnTp byte) (ver int64, _ error) {
	var err error
	if modifyColumnTp == mysql.TypeNull {
		// field NotNullFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.NotNullFlag
		// field PreventNullInsertFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.PreventNullInsertFlag
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	return ver, nil
}

// modifyColsFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
func modifyColsFromNull2NotNull(w *worker, dbInfo *model.DBInfo, tblInfo *model.TableInfo, cols []*model.ColumnInfo,
	newColName model.CIStr, isModifiedType bool) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	// If there is a null value inserted, it cannot be modified and needs to be rollback.
	err = checkForNullValue(ctx, isModifiedType, dbInfo.Name, tblInfo.Name, newColName, cols...)
	if err != nil {
		return errors.Trace(err)
	}

	// Prevent this field from inserting null values.
	for _, col := range cols {
		col.Flag |= mysql.PreventNullInsertFlag
	}
	return nil
}

func generateOriginDefaultValue(col *model.ColumnInfo) (interface{}, error) {
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.Flag) {
		zeroVal := table.GetZeroValue(col)
		odValue, err = zeroVal.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return odValue, nil
}

func getColumnInfoByName(tbInfo *model.TableInfo, column string) *model.ColumnInfo {
	for _, colInfo := range tbInfo.Cols() {
		if colInfo.Name.L == column {
			return colInfo
		}
	}
	return nil
}
