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

package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type tableHintInfo struct {
	sortMergeJoinTables []hintTableInfo
	hashJoinTables      []hintTableInfo
	indexHintList       []indexHintInfo
}

type hintTableInfo struct {
	dbName  model.CIStr
	tblName model.CIStr
	matched bool
}

type indexHintInfo struct {
	dbName    model.CIStr
	tblName   model.CIStr
	indexHint *ast.IndexHint
}

func tableNames2HintTableInfo(ctx sessionctx.Context, hintTables []ast.HintTable) []hintTableInfo {
	if len(hintTables) == 0 {
		return nil
	}
	hintTableInfos := make([]hintTableInfo, len(hintTables))
	defaultDBName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	for i, hintTable := range hintTables {
		tableInfo := hintTableInfo{tblName: hintTable.TableName}
		if tableInfo.dbName.L == "" {
			tableInfo.dbName = defaultDBName
		}
		hintTableInfos[i] = tableInfo
	}
	return hintTableInfos
}

func (info *tableHintInfo) ifPreferMergeJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.sortMergeJoinTables)
}

func (info *tableHintInfo) ifPreferHashJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.hashJoinTables)
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (info *tableHintInfo) matchTableName(tables []*hintTableInfo, hintTables []hintTableInfo) bool {
	hintMatched := false
	for _, table := range tables {
		for i, curEntry := range hintTables {
			if table == nil {
				continue
			}
			if curEntry.dbName.L == table.dbName.L && curEntry.tblName.L == table.tblName.L {
				hintTables[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}

func restore2JoinHint(hintType string, hintTables []hintTableInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	for i, table := range hintTables {
		buffer.WriteString(table.tblName.L)
		if i < len(hintTables)-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func extractUnmatchedTables(hintTables []hintTableInfo) []string {
	var tableNames []string
	for _, table := range hintTables {
		if !table.matched {
			tableNames = append(tableNames, table.tblName.O)
		}
	}
	return tableNames
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
}

type capFlagType = uint64

const (
	_ capFlagType = iota
	// canExpandAST indicates whether the origin AST can be expanded during plan
	// building. ONLY used for `CreateViewStmt` now.
	canExpandAST
)

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx sessionctx.Context
	is  infoschema.InfoSchema
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int

	tableHintInfo []tableHintInfo
	// optFlag indicates the flags of the optimizer rules.
	optFlag uint64
	// capFlag indicates the capability flags.
	capFlag capFlagType

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	// handleHelper records the handle column position for tables. Delete/Update/SelectLock/UnionScan may need this information.
	// It collects the information by the following procedure:
	//   Since we build the plan tree from bottom to top, we maintain a stack to record the current handle information.
	//   If it's a dataSource/tableDual node, we create a new map.
	//   If it's a aggregation, we pop the map and push a nil map since no handle information left.
	//   If it's a union, we pop all children's and push a nil map.
	//   If it's a join, we pop its children's out then merge them and push the new map to stack.
	//   If we meet a subquery, it's clearly that it's a independent problem so we just pop one map out when we finish building the subquery.
	handleHelper *handleColHelper
}

type handleColHelper struct {
	id2HandleMapStack []map[int64][]*expression.Column
	stackTail         int
}

func (hch *handleColHelper) popMap() map[int64][]*expression.Column {
	ret := hch.id2HandleMapStack[hch.stackTail-1]
	hch.stackTail--
	hch.id2HandleMapStack = hch.id2HandleMapStack[:hch.stackTail]
	return ret
}

func (hch *handleColHelper) pushMap(m map[int64][]*expression.Column) {
	hch.id2HandleMapStack = append(hch.id2HandleMapStack, m)
	hch.stackTail++
}

func (hch *handleColHelper) mergeAndPush(m1, m2 map[int64][]*expression.Column) {
	newMap := make(map[int64][]*expression.Column)
	for k, v := range m1 {
		newMap[k] = make([]*expression.Column, len(v))
		copy(newMap[k], v)
	}
	for k, v := range m2 {
		if _, ok := newMap[k]; ok {
			newMap[k] = append(newMap[k], v...)
		} else {
			newMap[k] = make([]*expression.Column, len(v))
			copy(newMap[k], v)
		}
	}
	hch.pushMap(newMap)
}

func (hch *handleColHelper) tailMap() map[int64][]*expression.Column {
	return hch.id2HandleMapStack[hch.stackTail-1]
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	return &PlanBuilder{
		ctx:          sctx,
		is:           is,
		colMapper:    make(map[*ast.ColumnNameExpr]int),
		handleHelper: &handleColHelper{id2HandleMapStack: make([]map[int64][]*expression.Column, 0)},
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	b.optFlag = flagPrunColumns
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(ctx, x)
	case *ast.DeleteStmt:
		return b.buildDelete(ctx, x)
	case *ast.ExplainStmt:
		return b.buildExplain(ctx, x)
	case *ast.InsertStmt:
		return b.buildInsert(ctx, x)
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.ShowStmt:
		return b.buildShow(ctx, x)
	case *ast.SetStmt:
		return b.buildSet(ctx, x)
	case *ast.AnalyzeTableStmt:
		return b.buildAnalyze(x)
	case *ast.UseStmt, *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DDLNode:
		return b.buildDDL(ctx, x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Convert column name expression to string value expression.
				vars.Value = ast.NewValueExpr(cn.Name.Name.O)
			}
			mockTablePlan := LogicalTableDual{}.Init(b.ctx)
			var err error
			assign.Expr, _, err = b.rewrite(ctx, vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.(*driver.ValueExpr).Datum,
				RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func getPathByIndexName(paths []*util.AccessPath, idxName model.CIStr, tblInfo *model.TableInfo) *util.AccessPath {
	var tablePath *util.AccessPath
	for _, path := range paths {
		if path.IsTablePath {
			tablePath = path
			continue
		}
		if path.Index.Name.L == idxName.L {
			return path
		}
	}
	if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
		return tablePath
	}
	return nil
}

func isPrimaryIndex(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func (b *PlanBuilder) getPossibleAccessPaths(indexHints []*ast.IndexHint, tbl table.Table, dbName, tblName model.CIStr) ([]*util.AccessPath, error) {
	tblInfo := tbl.Meta()
	publicPaths := make([]*util.AccessPath, 0, len(tblInfo.Indices)+2)
	publicPaths = append(publicPaths, &util.AccessPath{IsTablePath: true})
	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			publicPaths = append(publicPaths, &util.AccessPath{Index: index})
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*util.AccessPath, 0, len(publicPaths))
	ignored := make([]*util.AccessPath, 0, len(publicPaths))

	// Extract comment-style index hint like /*+ INDEX(t, idx1, idx2) */.
	indexHintsLen := len(indexHints)
	if hints := b.TableHints(); hints != nil {
		for _, hint := range hints.indexHintList {
			if hint.dbName.L == dbName.L && hint.tblName.L == tblName.L {
				indexHints = append(indexHints, hint.indexHint)
			}
		}
	}

	for i, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		hasScanHint = true

		// It is syntactically valid to omit index_list for USE INDEX, which means “use no indexes”.
		// Omitting index_list for FORCE INDEX or IGNORE INDEX is a syntax error.
		// See https://dev.mysql.com/doc/refman/8.0/en/index-hints.html.
		if hint.IndexNames == nil && hint.HintType != ast.HintIgnore {
			if path := getTablePath(publicPaths); path != nil {
				hasUseOrForce = true
				path.Forced = true
				available = append(available, path)
			}
		}
		for _, idxName := range hint.IndexNames {
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				err := ErrKeyDoesNotExist.GenWithStackByArgs(idxName, tblInfo.Name)
				// if hint is from comment-style sql hints, we should throw a warning instead of error.
				if i < indexHintsLen {
					return nil, err
				}
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				continue
			}
			if hint.HintType == ast.HintIgnore {
				// Collect all the ignored index hints.
				ignored = append(ignored, path)
				continue
			}
			// Currently we don't distinguish between "FORCE" and "USE" because
			// our cost estimation is not reliable.
			hasUseOrForce = true
			path.Forced = true
			available = append(available, path)
		}
	}

	if !hasScanHint || !hasUseOrForce {
		available = publicPaths
	}

	available = removeIgnoredPaths(available, ignored, tblInfo)

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use table scan.
	if len(available) == 0 {
		available = append(available, &util.AccessPath{IsTablePath: true})
	}
	return available, nil
}

func removeIgnoredPaths(paths, ignoredPaths []*util.AccessPath, tblInfo *model.TableInfo) []*util.AccessPath {
	if len(ignoredPaths) == 0 {
		return paths
	}
	remainedPaths := make([]*util.AccessPath, 0, len(paths))
	for _, path := range paths {
		if path.IsTablePath || getPathByIndexName(ignoredPaths, path.Index.Name, tblInfo) == nil {
			remainedPaths = append(remainedPaths, path)
		}
	}
	return remainedPaths
}

func (b *PlanBuilder) buildAdmin(ctx context.Context, as *ast.AdminStmt) (Plan, error) {
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminShowNextRowID:
		p := &ShowNextRowID{TableName: as.Tables[0]}
		p.setSchemaAndNames(buildShowNextRowID())
		ret = p
	case ast.AdminShowDDL:
		p := &ShowDDL{}
		p.setSchemaAndNames(buildShowDDLFields())
		ret = p
	case ast.AdminShowDDLJobs:
		p := LogicalShowDDLJobs{JobNumber: as.JobNumber}.Init(b.ctx)
		p.setSchemaAndNames(buildShowDDLJobsFields())
		for _, col := range p.schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		ret = p
		if as.Where != nil {
			ret, err = b.buildSelection(ctx, p, as.Where, nil)
			if err != nil {
				return nil, err
			}
		}
	case ast.AdminCancelDDLJobs:
		p := &CancelDDLJobs{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildCancelDDLJobsFields())
		ret = p
	case ast.AdminShowDDLJobQueries:
		p := &ShowDDLJobQueries{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildShowDDLJobQueriesFields())
		ret = p
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.

	return ret, nil
}

// getColsInfo returns the info of index columns, normal columns and primary key.
func getColsInfo(tn *ast.TableName) (indicesInfo []*model.IndexInfo, colsInfo []*model.ColumnInfo, pkCol *model.ColumnInfo) {
	tbl := tn.TableInfo
	for _, col := range tbl.Columns {
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		} else {
			colsInfo = append(colsInfo, col)
		}
	}
	for _, idx := range tn.TableInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	return
}

func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	for _, tbl := range as.TableNames {
		idxInfo, colInfo, pkInfo := getColsInfo(tbl)
		for _, idx := range idxInfo {
			info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PhysicalTableID: tbl.TableInfo.ID, Incremental: as.Incremental}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{
				IndexInfo:   idx,
				analyzeInfo: info,
				TblInfo:     tbl.TableInfo,
			})
		}
		if len(colInfo) > 0 || pkInfo != nil {
			info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PhysicalTableID: tbl.TableInfo.ID, Incremental: as.Incremental}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
				PKInfo:      pkInfo,
				ColsInfo:    colInfo,
				analyzeInfo: info,
				TblInfo:     tbl.TableInfo,
			})
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.TableNames[0].TableInfo
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
			pkCol := tblInfo.GetPkColInfo()
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PhysicalTableID: tblInfo.ID, Incremental: as.Incremental}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info})
			continue
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PhysicalTableID: tblInfo.ID, Incremental: as.Incremental}
		p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.TableNames[0].TableInfo
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PhysicalTableID: tblInfo.ID, Incremental: as.Incremental}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
		}
	}
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PhysicalTableID: tblInfo.ID, Incremental: as.Incremental}
		p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info})
	}
	return p, nil
}

var cmSketchSizeLimit = kv.TxnEntrySizeLimit / binary.MaxVarintLen32

var analyzeOptionLimit = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    1024,
	ast.AnalyzeOptCMSketchWidth: uint64(cmSketchSizeLimit),
	ast.AnalyzeOptCMSketchDepth: uint64(cmSketchSizeLimit),
	ast.AnalyzeOptNumSamples:    100000,
}

var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    10000,
}

func handleAnalyzeOptions(opts []ast.AnalyzeOpt) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, val := range analyzeOptionDefault {
		optMap[key] = val
	}
	for _, opt := range opts {
		if opt.Value == 0 || opt.Value > analyzeOptionLimit[opt.Type] {
			return nil, errors.Errorf("value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
		}
		optMap[opt.Type] = opt.Value
	}
	if optMap[ast.AnalyzeOptCMSketchWidth]*optMap[ast.AnalyzeOptCMSketchDepth] > uint64(cmSketchSizeLimit) {
		return nil, errors.Errorf("cm sketch size(depth * width) should not larger than %d", cmSketchSizeLimit)
	}
	return optMap, nil
}

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	opts, err := handleAnalyzeOptions(as.AnalyzeOpts)
	if err != nil {
		return nil, err
	}
	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as, opts)
		}
		return b.buildAnalyzeIndex(as, opts)
	}
	return b.buildAnalyzeTable(as, opts)
}

func buildShowNextRowID() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(4)
	schema.Append(buildColumnWithName("", "DB_NAME", mysql.TypeVarchar, mysql.MaxDatabaseNameLength))
	schema.Append(buildColumnWithName("", "TABLE_NAME", mysql.TypeVarchar, mysql.MaxTableNameLength))
	schema.Append(buildColumnWithName("", "COLUMN_NAME", mysql.TypeVarchar, mysql.MaxColumnNameLength))
	schema.Append(buildColumnWithName("", "NEXT_GLOBAL_ROW_ID", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildShowDDLFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(6)
	schema.Append(buildColumnWithName("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "OWNER_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "OWNER_ADDRESS", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "RUNNING_JOBS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "SELF_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "QUERY", mysql.TypeVarchar, 256))

	return schema.col2Schema(), schema.names
}

func buildShowDDLJobsFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "DB_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "TABLE_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "JOB_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCHEMA_STATE", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCHEMA_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "TABLE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "ROW_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "START_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "END_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "STATE", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

func buildTableRegionsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName("", "REGION_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "START_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "END_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "LEADER_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "LEADER_STORE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "PEERS", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCATTERING", mysql.TypeTiny, 1))
	schema.Append(buildColumnWithName("", "WRITTEN_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "READ_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "APPROXIMATE_SIZE(MB)", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "APPROXIMATE_KEYS", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildShowDDLJobQueriesFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "QUERY", mysql.TypeVarchar, 256))
	return schema.col2Schema(), schema.names
}

func buildCancelDDLJobsFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "RESULT", mysql.TypeVarchar, 128))

	return schema.col2Schema(), schema.names
}

func buildColumnWithName(tableName, name string, tp byte, size int) (*expression.Column, *types.FieldName) {
	cs, cl := types.DefaultCharsetForType(tp)
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = charset.CharsetUTF8MB4
		cl = charset.CollationUTF8MB4
		flag = 0
	}

	fieldType := &types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    flag,
	}
	return &expression.Column{
		RetType: fieldType,
	}, &types.FieldName{DBName: model.NewCIStr(infoschema.Name), TblName: model.NewCIStr(tableName), ColName: model.NewCIStr(name)}
}

type columnsWithNames struct {
	cols  []*expression.Column
	names types.NameSlice
}

func newColumnsWithNames(cap int) *columnsWithNames {
	return &columnsWithNames{
		cols:  make([]*expression.Column, 0, 2),
		names: make(types.NameSlice, 0, 2),
	}
}

func (cwn *columnsWithNames) Append(col *expression.Column, name *types.FieldName) {
	cwn.cols = append(cwn.cols, col)
	cwn.names = append(cwn.names, name)
}

func (cwn *columnsWithNames) col2Schema() *expression.Schema {
	return expression.NewSchema(cwn.cols...)
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (Plan, error) {
	p := LogicalShow{
		ShowContents: ShowContents{
			Tp:          show.Tp,
			DBName:      show.DBName,
			Table:       show.Table,
			Column:      show.Column,
			IndexName:   show.IndexName,
			Flag:        show.Flag,
			Full:        show.Full,
			IfNotExists: show.IfNotExists,
			GlobalScope: show.GlobalScope,
		},
	}.Init(b.ctx)
	switch show.Tp {
	case ast.ShowTables, ast.ShowTableStatus:
		if p.DBName == "" {
			return nil, ErrNoDB
		}
	}
	schema, names := buildShowSchema(show)
	p.SetSchema(schema)
	p.names = names
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	var err error
	var np LogicalPlan
	np = p
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.OutputNames()[0].ColName},
		}
		np, err = b.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Where != nil {
		np, err = b.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if np != p {
		b.optFlag |= flagEliminateProjection
		fieldsLen := len(p.schema.Columns)
		proj := LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx)
		schema := expression.NewSchema(make([]*expression.Column, 0, fieldsLen)...)
		for _, col := range p.schema.Columns {
			proj.Exprs = append(proj.Exprs, col)
			newCol := col.Clone().(*expression.Column)
			newCol.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
			schema.Append(newCol)
		}
		proj.SetSchema(schema)
		proj.SetChildren(np)
		proj.SetOutputNames(np.OutputNames())
		return proj, nil
	}
	return p, nil
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.UseStmt:
		if raw.DBName == "" {
			return nil, ErrNoDB
		}
	}
	return p, nil
}

func (b *PlanBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	value, err := table.GetColDefaultValue(b.ctx, col.ToInfo())
	if err != nil {
		return nil, err
	}
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

func (b *PlanBuilder) findDefaultValue(cols []*table.Column, name *ast.ColumnName) (*expression.Constant, error) {
	for _, col := range cols {
		if col.Name.L == name.Name.L {
			return b.getDefaultValue(col)
		}
	}
	return nil, ErrUnknownColumn.GenWithStackByArgs(name.Name.O, "field_list")
}

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	tableInfo := tn.TableInfo
	// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	schema, names := expression.TableInfo2SchemaAndNames(b.ctx, tn.Schema, tableInfo)
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		return nil, errors.Errorf("Can't get table %s.", tableInfo.Name.O)
	}

	insertPlan := Insert{
		Table:         tableInPlan,
		Columns:       insert.Columns,
		tableSchema:   schema,
		tableColNames: names,
		IsReplace:     insert.IsReplace,
	}.Init(b.ctx)

	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(insertPlan.tableSchema)
	mockTablePlan.names = insertPlan.tableColNames

	checkRefColumn := func(n ast.Node) ast.Node {
		if insertPlan.NeedFillDefaultValue {
			return n
		}
		switch n.(type) {
		case *ast.ColumnName, *ast.ColumnNameExpr:
			insertPlan.NeedFillDefaultValue = true
		}
		return n
	}

	if len(insert.Setlist) > 0 {
		// Branch for `INSERT ... SET ...`.
		err := b.buildSetValuesOfInsert(ctx, insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, err
		}
	} else if len(insert.Lists) > 0 {
		// Branch for `INSERT ... VALUES ...`.
		err := b.buildValuesListOfInsert(ctx, insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, err
		}
	} else {
		// Branch for `INSERT ... SELECT ...`.
		err := b.buildSelectPlanOfInsert(ctx, insert, insertPlan)
		if err != nil {
			return nil, err
		}
	}

	err := insertPlan.ResolveIndices()
	return insertPlan, err
}

func (b *PlanBuilder) getAffectCols(insertStmt *ast.InsertStmt, insertPlan *Insert) (affectedValuesCols []*table.Column, err error) {
	if len(insertStmt.Columns) > 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name (col_name [, col_name] ...) {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name (col_name [, col_name] ...) SELECT ...`.
		colName := make([]string, 0, len(insertStmt.Columns))
		for _, col := range insertStmt.Columns {
			colName = append(colName, col.Name.O)
		}
		affectedValuesCols, err = table.FindCols(insertPlan.Table.Cols(), colName, insertPlan.Table.Meta().PKIsHandle)
		if err != nil {
			return nil, err
		}

	} else if len(insertStmt.Setlist) == 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesCols = insertPlan.Table.Cols()
	}
	return affectedValuesCols, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	colNames := make([]string, 0, len(insert.Setlist))
	exprCols := make([]*expression.Column, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		idx, err := expression.FindFieldName(insertPlan.tableColNames, assign.Column)
		if err != nil {
			return err
		}
		if idx < 0 {
			return errors.Errorf("Can't find column %s", assign.Column)
		}
		colNames = append(colNames, assign.Column.Name.L)
		exprCols = append(exprCols, insertPlan.tableSchema.Columns[idx])
	}

	insertPlan.AllAssignmentsAreConstant = true
	for i, assign := range insert.Setlist {
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		expr, _, err := b.rewriteWithPreprocess(ctx, assign.Expr, mockTablePlan, nil, true, checkRefColumn)
		if err != nil {
			return err
		}
		if insertPlan.AllAssignmentsAreConstant {
			_, isConstant := expr.(*expression.Constant)
			insertPlan.AllAssignmentsAreConstant = isConstant
		}

		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			Col:     exprCols[i],
			ColName: model.NewCIStr(colNames[i]),
			Expr:    expr,
		})
	}
	return nil
}

func (b *PlanBuilder) buildValuesListOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			fmt.Printf("%v %v\n", len(insert.Lists[0]), affectedValuesCols)
			return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
	}

	insertPlan.AllAssignmentsAreConstant = true
	totalTableCols := insertPlan.Table.Cols()
	for i, valuesItem := range insert.Lists {
		// The length of all the value_list should be the same.
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		if i > 0 && len(insert.Lists[i-1]) != len(insert.Lists[i]) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
		}
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			var expr expression.Expression
			var err error
			switch x := valueItem.(type) {
			case *ast.DefaultExpr:
				if x.Name != nil {
					expr, err = b.findDefaultValue(totalTableCols, x.Name)
				} else {
					expr, err = b.getDefaultValue(affectedValuesCols[j])
				}
			case *driver.ValueExpr:
				expr = &expression.Constant{
					Value:   x.Datum,
					RetType: &x.Type,
				}
			default:
				expr, _, err = b.rewriteWithPreprocess(ctx, valueItem, mockTablePlan, nil, true, checkRefColumn)
			}
			if err != nil {
				return err
			}
			if insertPlan.AllAssignmentsAreConstant {
				_, isConstant := expr.(*expression.Constant)
				insertPlan.AllAssignmentsAreConstant = isConstant
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	return nil
}

func (b *PlanBuilder) buildSelectPlanOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}
	selectPlan, err := b.Build(ctx, insert.Select)
	if err != nil {
		return err
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	if selectPlan.Schema().Len() != len(affectedValuesCols) {
		return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
	}

	names := selectPlan.OutputNames()
	insertPlan.SelectPlan, err = DoOptimize(ctx, b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return err
	}

	// schema4NewRow is the schema for the newly created data record based on
	// the result of the select statement.
	schema4NewRow := expression.NewSchema(make([]*expression.Column, len(insertPlan.Table.Cols()))...)
	names4NewRow := make(types.NameSlice, len(insertPlan.Table.Cols()))
	// TODO: don't clone it.
	for i, selCol := range insertPlan.SelectPlan.Schema().Columns {
		ordinal := affectedValuesCols[i].Offset
		schema4NewRow.Columns[ordinal] = &expression.Column{}
		*schema4NewRow.Columns[ordinal] = *selCol

		schema4NewRow.Columns[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.Columns[ordinal].RetType = affectedValuesCols[i].FieldType

		names4NewRow[ordinal] = names[i]
	}
	for i := range schema4NewRow.Columns {
		if schema4NewRow.Columns[i] == nil {
			schema4NewRow.Columns[i] = &expression.Column{UniqueID: insertPlan.ctx.GetSessionVars().AllocPlanColumnID()}
			names4NewRow[i] = types.EmptyName
		}
	}
	return nil
}

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (Plan, error) {
	p := &DDL{Statement: node}
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan Plan, format string, execStmt ast.StmtNode) (Plan, error) {
	p := &Explain{
		TargetPlan: targetPlan,
		Format:     format,
		ExecStmt:   execStmt,
	}
	p.ctx = b.ctx
	return p, p.prepareSchema()
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}
	targetPlan, _, err := OptimizeAstNode(ctx, b.ctx, explain.Stmt, b.is)
	if err != nil {
		return nil, err
	}

	return b.buildExplainPlan(targetPlan, explain.Format, explain.Stmt)
}

func buildShowProcedureSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "ROUTINES"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowTriggerSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "TRIGGERS"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowEventsSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "EVENTS"
	schema := newColumnsWithNames(15)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowWarningsSchema() (*expression.Schema, types.NameSlice) {
	tblName := "WARNINGS"
	schema := newColumnsWithNames(3)
	schema.Append(buildColumnWithName(tblName, "Level", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName(tblName, "Code", mysql.TypeLong, 19))
	schema.Append(buildColumnWithName(tblName, "Message", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt) (schema *expression.Schema, outputNames []*types.FieldName) {
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowProcedureStatus:
		return buildShowProcedureSchema()
	case ast.ShowTriggers:
		return buildShowTriggerSchema()
	case ast.ShowEvents:
		return buildShowEventsSchema()
	case ast.ShowWarnings, ast.ShowErrors:
		return buildShowWarningsSchema()
	case ast.ShowRegions:
		return buildTableRegionsSchema()
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowDatabases:
		names = []string{"Database"}
	case ast.ShowOpenTables:
		names = []string{"Database", "Table", "In_use", "Name_locked"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeLong}
	case ast.ShowTables:
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Table_type")
		}
	case ast.ShowTableStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCreateTable:
		names = []string{"Table", "Create Table"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowDrainerStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowIndex:
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPumpStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Table_name", "Partition_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowProfiles: // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{mysql.TypeLong, mysql.TypeDouble, mysql.TypeVarchar}
	case ast.ShowMasterStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPrivileges:
		names = []string{"Privilege", "Context", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindings:
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time", "State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeDatetime, mysql.TypeVarchar}
	case ast.ShowBuiltins:
		names = []string{"Supported_builtin_functions"}
		ftypes = []byte{mysql.TypeVarchar}
	}

	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: model.NewCIStr(names[i])})
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schema.Append(col)
	}
	return
}
