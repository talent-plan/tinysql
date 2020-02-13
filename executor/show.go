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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
)

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    model.CIStr
	Table     *ast.TableName  // Used for showing columns.
	Column    *ast.ColumnName // Used for `desc table column`.
	IndexName model.CIStr     // Used for show table regions.
	Flag      int             // Some flag parsed from sql, such as FULL.

	is infoschema.InfoSchema

	result *chunk.Chunk
	cursor int

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`
	GlobalScope bool // GlobalScope is used by show variables
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		e.result = newFirstChunk(e)
		err := e.fetchAll(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := 0; colIdx < e.Schema().Len(); colIdx++ {
			retType := e.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				continue
			}
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				if valLen := len(row.GetString(colIdx)); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if e.cursor >= e.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), e.result.NumRows()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) fetchAll(ctx context.Context) error {
	switch e.Tp {
	case ast.ShowCreateTable:
		return e.fetchShowCreateTable()
	case ast.ShowCreateDatabase:
		return e.fetchShowCreateDatabase()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
	}
	return nil
}

// moveInfoSchemaToFront moves information_schema to the first, and the others are sorted in the origin ascending order.
func moveInfoSchemaToFront(dbs []string) {
	if len(dbs) > 0 && strings.EqualFold(dbs[0], "INFORMATION_SCHEMA") {
		return
	}

	i := sort.SearchStrings(dbs, "INFORMATION_SCHEMA")
	if i < len(dbs) && strings.EqualFold(dbs[i], "INFORMATION_SCHEMA") {
		copy(dbs[1:i+1], dbs[0:i])
		dbs[0] = "INFORMATION_SCHEMA"
	}
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := e.is.AllSchemaNames()
	sort.Strings(dbs)
	// let information_schema be the first database
	moveInfoSchemaToFront(dbs)
	for _, d := range dbs {
		e.appendRow([]interface{}{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowTables() error {
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	// sort for tables
	tableNames := make([]string, 0, len(e.is.SchemaTables(e.DBName)))
	var tableTypes = make(map[string]string)
	for _, v := range e.is.SchemaTables(e.DBName) {
		tableNames = append(tableNames, v.Meta().Name.O)
		tableTypes[v.Meta().Name.O] = "BASE TABLE"
	}
	sort.Strings(tableNames)
	for _, v := range tableNames {
		if e.Full {
			e.appendRow([]interface{}{v, tableTypes[v]})
		} else {
			e.appendRow([]interface{}{v})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowVariables() (err error) {
	var (
		value         string
		ok            bool
		sessionVars   = e.ctx.GetSessionVars()
		unreachedVars = make([]string, 0, len(variable.SysVars))
	)
	for _, v := range variable.SysVars {
		if !e.GlobalScope {
			// For a session scope variable,
			// 1. try to fetch value from SessionVars.Systems;
			// 2. if this variable is session-only, fetch value from SysVars
			//		otherwise, fetch the value from table `mysql.Global_Variables`.
			value, ok, err = variable.GetSessionOnlySysVars(sessionVars, v.Name)
		} else {
			// If the scope of a system variable is ScopeNone,
			// it's a read-only variable, so we return the default value of it.
			// Otherwise, we have to fetch the values from table `mysql.Global_Variables` for global variable names.
			value, ok, err = variable.GetScopeNoneSystemVar(v.Name)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			unreachedVars = append(unreachedVars, v.Name)
			continue
		}
		e.appendRow([]interface{}{v.Name, value})
	}
	if len(unreachedVars) != 0 {
		systemVars, err := sessionVars.GlobalVarsAccessor.GetAllSysVars()
		if err != nil {
			return errors.Trace(err)
		}
		for _, varName := range unreachedVars {
			varValue, ok := systemVars[varName]
			if !ok {
				varValue = variable.SysVars[varName].Value
			}
			e.appendRow([]interface{}{varName, varValue})
		}
	}
	return nil
}

func getDefaultCollate(charsetName string) string {
	for _, c := range charset.GetSupportedCharsets() {
		if strings.EqualFold(c.Name, charsetName) {
			return c.DefaultCollation
		}
	}
	return ""
}

// escape the identifier for pretty-printing.
// For instance, the identifier "foo `bar`" will become "`foo ``bar```".
// The sqlMode controls whether to escape with backquotes (`) or double quotes
// (`"`) depending on whether mysql.ModeANSIQuotes is enabled.
func escape(cis model.CIStr, sqlMode mysql.SQLMode) string {
	var quote string
	if sqlMode&mysql.ModeANSIQuotes != 0 {
		quote = `"`
	} else {
		quote = "`"
	}
	return quote + strings.Replace(cis.O, quote, quote+quote, -1) + quote
}

// ConstructResultOfShowCreateTable constructs the result for show create table.
func ConstructResultOfShowCreateTable(ctx sessionctx.Context, tableInfo *model.TableInfo, allocator autoid.Allocator, buf *bytes.Buffer) (err error) {
	tblCharset := tableInfo.Charset
	if len(tblCharset) == 0 {
		tblCharset = mysql.DefaultCharset
	}
	tblCollate := tableInfo.Collate
	// Set default collate if collate is not specified.
	if len(tblCollate) == 0 {
		tblCollate = getDefaultCollate(tblCharset)
	}

	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE TABLE %s (\n", escape(tableInfo.Name, sqlMode))
	var pkCol *model.ColumnInfo
	var hasAutoIncID bool
	for i, col := range tableInfo.Cols() {
		fmt.Fprintf(buf, "  %s %s", escape(col.Name, sqlMode), col.GetTypeDesc())
		if col.Charset != "binary" {
			if col.Charset != tblCharset {
				fmt.Fprintf(buf, " CHARACTER SET %s", col.Charset)
			}
			if col.Collate != tblCollate {
				fmt.Fprintf(buf, " COLLATE %s", col.Collate)
			} else {
				defcol, err := charset.GetDefaultCollation(col.Charset)
				if err == nil && defcol != col.Collate {
					fmt.Fprintf(buf, " COLLATE %s", col.Collate)
				}
			}
		}
		if mysql.HasAutoIncrementFlag(col.Flag) {
			hasAutoIncID = true
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.Flag) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated columns in MySQL
			if !mysql.HasNoDefaultValueFlag(col.Flag) {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.Flag) {
						buf.WriteString(" DEFAULT NULL")
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)

					fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
				}
			}
			if mysql.HasOnUpdateNowFlag(col.Flag) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				buf.WriteString(table.OptionalFsp(&col.FieldType))
			}
		}
		if len(col.Comment) > 0 {
			fmt.Fprintf(buf, " COMMENT '%s'", format.OutputFormat(col.Comment))
		}
		if i != len(tableInfo.Cols())-1 {
			buf.WriteString(",\n")
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHanle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", escape(pkCol.Name, sqlMode))
	}

	publicIndices := make([]*model.IndexInfo, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		if idx.State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", escape(idxInfo.Name, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", escape(idxInfo.Name, sqlMode))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		for _, c := range idxInfo.Columns {
			colInfo := escape(c.Name, sqlMode)
			if c.Length != types.UnspecifiedLength {
				colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
			}
			cols = append(cols, colInfo)
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(cols, ","))
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// Because we only support case sensitive utf8_bin collate, we need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 {
		// If we can not find default collate for the given charset,
		// do not show the collate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tableInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", tableInfo.Compression)
	}

	if hasAutoIncID {
		autoIncID, err := allocator.NextGlobalAutoID(tableInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}
		// It's compatible with MySQL.
		if autoIncID > 1 {
			fmt.Fprintf(buf, " AUTO_INCREMENT=%d", autoIncID)
		}
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(tableInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(tableInfo.Comment))
	}
	return nil
}

func (e *ShowExec) fetchShowCreateTable() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	allocator := tb.Allocator(e.ctx)
	var buf bytes.Buffer
	// TODO: let the result more like MySQL.
	if err = ConstructResultOfShowCreateTable(e.ctx, tb.Meta(), allocator, &buf); err != nil {
		return err
	}

	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String()})
	return nil
}

// ConstructResultOfShowCreateDatabase constructs the result for show create database.
func ConstructResultOfShowCreateDatabase(ctx sessionctx.Context, dbInfo *model.DBInfo, ifNotExists bool, buf *bytes.Buffer) (err error) {
	sqlMode := ctx.GetSessionVars().SQLMode
	var ifNotExistsStr string
	if ifNotExists {
		ifNotExistsStr = "/*!32312 IF NOT EXISTS*/ "
	}
	fmt.Fprintf(buf, "CREATE DATABASE %s%s", ifNotExistsStr, escape(dbInfo.Name, sqlMode))
	if s := dbInfo.Charset; len(s) > 0 {
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s */", s)
	}
	return nil
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	dbInfo, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	var buf bytes.Buffer
	err := ConstructResultOfShowCreateDatabase(e.ctx, dbInfo, e.IfNotExists, &buf)
	if err != nil {
		return err
	}
	e.appendRow([]interface{}{dbInfo.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, w := range warns {
		if errOnly && w.Level != stmtctx.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := x.ToSQLError()
			e.appendRow([]interface{}{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			e.appendRow([]interface{}{w.Level, int64(mysql.ErrUnknown), warn.Error()})
		}
	}
	return nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, ok := e.is.TableByID(e.Table.TableInfo.ID)
	if !ok {
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	return tb, nil
}

func (e *ShowExec) appendRow(row []interface{}) {
	for i, col := range row {
		if col == nil {
			e.result.AppendNull(i)
			continue
		}
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}
