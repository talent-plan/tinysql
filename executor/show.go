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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/sqlexec"
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
	case ast.ShowCharset:
		return e.fetchShowCharset()
	case ast.ShowCollation:
		return e.fetchShowCollation()
	case ast.ShowColumns:
		return e.fetchShowColumns(ctx)
	case ast.ShowCreateTable:
		return e.fetchShowCreateTable()
	case ast.ShowCreateDatabase:
		return e.fetchShowCreateDatabase()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowEngines:
		return e.fetchShowEngines()
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowOpenTables:
		return e.fetchShowOpenTables()
	case ast.ShowTableStatus:
		return e.fetchShowTableStatus()
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
	case ast.ShowProcessList:
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		// empty result
	case ast.ShowProfiles:
		// empty result
	case ast.ShowMasterStatus:
		return e.fetchShowMasterStatus()
	case ast.ShowBuiltins:
		return e.fetchShowBuiltins()
	}
	return nil
}

func (e *ShowExec) fetchShowEngines() error {
	sql := `SELECT * FROM information_schema.engines`
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)

	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		e.result.AppendRow(row)
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

func (e *ShowExec) fetchShowProcessList() error {
	sm := e.ctx.GetSessionManager()
	if sm == nil {
		return nil
	}

	pl := sm.ShowProcessList()
	for _, pi := range pl {
		row := pi.ToRowForShow(e.Full)
		e.appendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowOpenTables() error {
	// TiDB has no concept like mysql's "table cache" and "open table"
	// For simplicity, we just return an empty result with the same structure as MySQL's SHOW OPEN TABLES
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

func (e *ShowExec) fetchShowTableStatus() error {
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	sql := fmt.Sprintf(`SELECT
               table_name, engine, version, row_format, table_rows,
               avg_row_length, data_length, max_data_length, index_length,
               data_free, auto_increment, create_time, update_time, check_time,
               table_collation, IFNULL(checksum,''), create_options, table_comment
               FROM information_schema.tables
	       WHERE table_schema='%s' ORDER BY table_name`, e.DBName)

	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(sql)

	if err != nil {
		return errors.Trace(err)
	}

	for _, row := range rows {
		e.result.AppendRow(row)

	}
	return nil
}

func (e *ShowExec) fetchShowColumns(ctx context.Context) error {
	tb, err := e.getTable()

	if err != nil {
		return errors.Trace(err)
	}

	cols := tb.Cols()
	for _, col := range cols {
		if e.Column != nil && e.Column.Name.L != col.Name.L {
			continue
		}

		desc := table.NewColDesc(col)
		var columnDefault interface{}
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			if col.Tp == mysql.TypeBit {
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				columnDefault = defaultValStr
			}
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		if e.Full {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	if tb.Meta().PKIsHandle {
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkCol = col
				break
			}
		}
		e.appendRow([]interface{}{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
		})
	}
	for _, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		for i, col := range idxInfo.Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}
			var subPart interface{}
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}
			nullVal := "YES"
			if idx.Meta().Name.O == mysql.PrimaryKeyName {
				nullVal = ""
			}
			e.appendRow([]interface{}{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				col.Name.O,             // Column_name
				"A",                    // Collation
				0,                      // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				nullVal,                // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	for _, desc := range descs {
		e.appendRow([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.ctx.GetSessionVars().TxnCtx.StartTS
	e.appendRow([]interface{}{"tidb-binlog", tso, "", "", ""})
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

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return errors.Trace(err)
	}
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == variable.ScopeSession {
			continue
		}
		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{status, value})
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
		if col.IsGenerated() {
			// It's a generated column.
			fmt.Fprintf(buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
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
			if !mysql.HasNoDefaultValueFlag(col.Flag) && !col.IsGenerated() {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.Flag) {
						if col.Tp == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					if col.Decimal > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", col.Decimal))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
					if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
						timeValue, err := table.GetColDefaultValue(ctx, col)
						if err != nil {
							return errors.Trace(err)
						}
						defaultValStr = timeValue.GetMysqlTime().String()
					}

					if col.Tp == mysql.TypeBit {
						defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
						fmt.Fprintf(buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
					} else {
						fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
					}
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
	// add partition info here.
	appendPartitionInfo(tableInfo.Partition, buf)
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

func appendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer) {
	if partitionInfo == nil {
		return
	}
	if partitionInfo.Type == model.PartitionTypeHash {
		fmt.Fprintf(buf, "\nPARTITION BY HASH( %s )", partitionInfo.Expr)
		fmt.Fprintf(buf, "\nPARTITIONS %d", partitionInfo.Num)
		return
	}
	// this if statement takes care of range columns case
	if partitionInfo.Columns != nil && partitionInfo.Type == model.PartitionTypeRange {
		buf.WriteString("\nPARTITION BY RANGE COLUMNS(")
		for i, col := range partitionInfo.Columns {
			buf.WriteString(col.L)
			if i < len(partitionInfo.Columns)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString(") (\n")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s ( %s ) (\n", partitionInfo.Type.String(), partitionInfo.Expr)
	}
	for i, def := range partitionInfo.Definitions {
		lessThans := strings.Join(def.LessThan, ",")
		fmt.Fprintf(buf, "  PARTITION %s VALUES LESS THAN (%s)", def.Name, lessThans)
		if i < len(partitionInfo.Definitions)-1 {
			buf.WriteString(",\n")
		} else {
			buf.WriteString("\n")
		}
	}
	buf.WriteString(")")
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

func (e *ShowExec) fetchShowCollation() error {
	collations := charset.GetSupportedCollations()
	for _, v := range collations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		e.appendRow([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
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
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}

func (e *ShowExec) fetchShowBuiltins() error {
	for _, f := range expression.GetBuiltinList() {
		e.appendRow([]interface{}{f})
	}
	return nil
}
