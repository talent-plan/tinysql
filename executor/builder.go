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

package executor

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// executorBuilder builds an Executor from a Plan.
// The InfoSchema must not change during execution.
type executorBuilder struct {
	ctx     sessionctx.Context
	is      infoschema.InfoSchema
	startTS uint64 // cached when the first time getStartTS() is called
	// err is set when there is error happened during Executor building process.
	err error
}

func newExecutorBuilder(ctx sessionctx.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

// MockPhysicalPlan is used to return a specified executor in when build.
// It is mainly used for testing.
type MockPhysicalPlan interface {
	plannercore.PhysicalPlan
	GetExecutor() Executor
}

func (b *executorBuilder) build(p plannercore.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plannercore.DDL:
		return b.buildDDL(v)
	case *plannercore.Delete:
		return b.buildDelete(v)
	case *plannercore.Explain:
		return b.buildExplain(v)
	case *plannercore.Insert:
		return b.buildInsert(v)
	case *plannercore.PhysicalLimit:
		return b.buildLimit(v)
	case *plannercore.ShowDDL:
		return b.buildShowDDL(v)
	case *plannercore.PhysicalShowDDLJobs:
		return b.buildShowDDLJobs(v)
	case *plannercore.PhysicalShow:
		return b.buildShow(v)
	case *plannercore.Simple:
		return b.buildSimple(v)
	case *plannercore.Set:
		return b.buildSet(v)
	case *plannercore.PhysicalSort:
		return b.buildSort(v)
	case *plannercore.PhysicalTopN:
		return b.buildTopN(v)
	case *plannercore.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plannercore.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plannercore.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plannercore.PhysicalSelection:
		return b.buildSelection(v)
	case *plannercore.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *plannercore.PhysicalProjection:
		return b.buildProjection(v)
	case *plannercore.PhysicalMemTable:
		return b.buildMemTable(v)
	case *plannercore.PhysicalTableDual:
		return b.buildTableDual(v)
	case *plannercore.Analyze:
		return b.buildAnalyze(v)
	case *plannercore.PhysicalTableReader:
		return b.buildTableReader(v)
	case *plannercore.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *plannercore.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	default:
		if mp, ok := p.(MockPhysicalPlan); ok {
			return mp.GetExecutor()
		}

		b.err = ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildShowDDL(v *plannercore.ShowDDL) Executor {
	// We get DDLInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DDLInfo.
	e := &ShowDDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}

	var err error
	ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.ddlOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		b.err = err
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	ddlInfo, err := admin.GetDDLInfo(txn)
	if err != nil {
		b.err = err
		return nil
	}
	e.ddlInfo = ddlInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDDLJobs(v *plannercore.PhysicalShowDDLJobs) Executor {
	e := &ShowDDLJobsExec{
		jobNumber:    v.JobNumber,
		is:           b.is,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(mathutil.MinUint64(v.Count, uint64(b.ctx.GetSessionVars().MaxChunkSize)))
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *executorBuilder) buildShow(v *plannercore.PhysicalShow) Executor {
	e := &ShowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		Tp:           v.Tp,
		DBName:       model.NewCIStr(v.DBName),
		Table:        v.Table,
		Column:       v.Column,
		IndexName:    v.IndexName,
		IfNotExists:  v.IfNotExists,
		Flag:         v.Flag,
		Full:         v.Full,
		GlobalScope:  v.GlobalScope,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) Executor {
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	var baseExec baseExecutor
	if selectExec != nil {
		baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID(), selectExec)
	} else {
		baseExec = newBaseExecutor(b.ctx, nil, v.ExplainID())
	}
	baseExec.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseExecutor:              baseExec,
		Table:                     v.Table,
		Columns:                   v.Columns,
		Lists:                     v.Lists,
		SetList:                   v.SetList,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		hasRefCols:                v.NeedFillDefaultValue,
		SelectExec:                selectExec,
	}
	err := ivs.initInsertColumns()
	if err != nil {
		b.err = err
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
	}
	return insert
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildDDL(v *plannercore.DDL) Executor {
	e := &DDLExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildExplain builds a explain executor. `e.rows` collects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) Executor {
	explainExec := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		explain:      v,
	}
	return explainExec
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) Executor {
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.buildUnionScanFromReader(reader, v)
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader Executor, v *plannercore.PhysicalUnionScan) Executor {
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), reader)}
	// Get the handle column index of the below Plan.
	us.belowHandleIndex = v.HandleCol.Index
	us.mutableRow = chunk.MutRowFromTypes(retTypes(us))
	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.desc = x.desc
		// Union scan can only be in a write transaction, so DirtyDB should has non-nil value now, thus
		// GetDirtyDB() is safe here. If this table has been modified in the transaction, non-nil DirtyTable
		// can be found in DirtyDB now, so GetDirtyTable is safe; if this table has not been modified in the
		// transaction, empty DirtyTable would be inserted into DirtyDB, it does not matter when multiple
		// goroutines write empty DirtyTable to DirtyDB for this table concurrently. Although the DirtyDB looks
		// safe for data race in all the cases, the map of golang will throw panic when it's accessed in parallel.
		// So we lock it when getting dirty table.
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		us.table = x.table
	case *IndexReaderExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.columns {
				if col.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		us.table = x.table
	case *IndexLookUpExecutor:
		us.desc = x.desc
		for _, ic := range x.index.Columns {
			for i, col := range x.columns {
				if col.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		physicalTableID := getPhysicalTableID(x.table)
		us.dirty = GetDirtyDB(b.ctx).GetDirtyTable(physicalTableID)
		us.conditions = v.Conditions
		us.columns = x.columns
		us.table = x.table
	default:
		// The mem table will not be written by sql directly, so we can omit the union scan to avoid err reporting.
		return reader
	}
	return us
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == plannercore.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == plannercore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			retTypes(leftExec),
			retTypes(rightExec),
		),
		isOuterJoin: v.JoinType.IsOuterJoin(),
	}

	leftKeys := v.LeftJoinKeys
	rightKeys := v.RightJoinKeys

	e.outerIdx = 0
	innerFilter := v.RightConditions

	e.innerTable = &mergeJoinInnerTable{
		reader:   rightExec,
		joinKeys: rightKeys,
	}

	e.outerTable = &mergeJoinOuterTable{
		reader: leftExec,
		filter: v.LeftConditions,
		keys:   leftKeys,
	}

	if v.JoinType == plannercore.RightOuterJoin {
		e.outerIdx = 1
		e.outerTable.reader = rightExec
		e.outerTable.filter = v.RightConditions
		e.outerTable.keys = rightKeys

		innerFilter = v.LeftConditions
		e.innerTable.reader = leftExec
		e.innerTable.joinKeys = leftKeys
	}

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	if len(innerFilter) != 0 {
		b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}
	return e
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &HashJoinExec{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), leftExec, rightExec),
		concurrency:       v.Concurrency,
		joinType:          v.JoinType,
		buildSideEstCount: v.Children()[v.InnerChildIdx].StatsCount(),
	}

	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := retTypes(leftExec), retTypes(rightExec)
	if v.InnerChildIdx == 0 {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
		e.buildSideExec = leftExec
		e.probeSideExec = rightExec
		e.probeSideFilter = v.RightConditions
		e.buildKeys = v.LeftJoinKeys
		e.probeKeys = v.RightJoinKeys
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.buildSideExec.Schema().Len())
		}
	} else {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
		e.buildSideExec = rightExec
		e.probeSideExec = leftExec
		e.probeSideFilter = v.LeftConditions
		e.buildKeys = v.RightJoinKeys
		e.probeKeys = v.LeftJoinKeys
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.buildSideExec.Schema().Len())
		}
	}
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes)
	}
	return e
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sessionVars := b.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), src),
		sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create table t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// mysql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// mysql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// mysql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 row in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {

		ordinal := []int{partialOrdinal}
		partialOrdinal++
		if aggDesc.Name == ast.AggFuncAvg {
			ordinal = append(ordinal, partialOrdinal+1)
			partialOrdinal++
		}
		partialAggDesc, finalDesc := aggDesc.Split(ordinal)
		partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
		finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
		e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
		e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		filters:      v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:  newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		numWorkers:    b.ctx.GetSessionVars().ProjectionConcurrency,
		evaluatorSuit: expression.NewEvaluatorSuite(v.Exprs),
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildTableDual(v *plannercore.PhysicalTableDual) Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID())
	base.initCap = v.RowCount
	e := &TableDualExec{
		baseExecutor: base,
		numDualRows:  v.RowCount,
	}
	return e
}

func (b *executorBuilder) getStartTS() (uint64, error) {
	if b.startTS != 0 {
		// Return the cached value.
		return b.startTS, nil
	}

	txn, err := b.ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	b.startTS = txn.StartTS()
	if b.startTS == 0 {
		return 0, errors.Trace(ErrGetStartTS)
	}
	return b.startTS, nil
}

func (b *executorBuilder) buildMemTable(v *plannercore.PhysicalMemTable) Executor {
	var e Executor
	tb, _ := b.is.TableByID(v.Table.ID)
	e = &TableScanExec{
		baseExecutor:   newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		t:              tb,
		columns:        v.Columns,
		seekHandle:     math.MinInt64,
		isVirtualTable: !tb.Type().IsNormalTable(),
	}
	return e
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildDelete(v *plannercore.Delete) Executor {
	tblID2table := make(map[int64]table.Table)
	for _, info := range v.TblColPosInfos {
		tblID2table[info.TblID], _ = b.is.TableByID(info.TblID)
	}
	b.startTS = b.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ExplainID(), selExec)
	base.initCap = chunk.ZeroCapacity
	deleteExec := &DeleteExec{
		baseExecutor:   base,
		tblID2Table:    tblID2table,
		tblColPosInfos: v.TblColPosInfos,
	}
	return deleteExec
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask) *analyzeTask {
	sc := b.ctx.GetSessionVars().StmtCtx
	e := &AnalyzeIndexExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		idxInfo:         task.IndexInfo,
		concurrency:     b.ctx.GetSessionVars().IndexSerialScanConcurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:    tipb.AnalyzeType_TypeIndex,
			Flags: sc.PushDownFlags(),
		},
	}
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: int64(defaultNumBuckets),
		NumColumns: int32(len(task.IndexInfo.Columns)),
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	return &analyzeTask{taskType: idxTask, idxExec: e}
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(task plannercore.AnalyzeColumnsTask) *analyzeTask {
	cols := task.ColsInfo
	if task.PKInfo != nil {
		cols = append([]*model.ColumnInfo{task.PKInfo}, cols...)
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	e := &AnalyzeColumnsExec{
		ctx:             b.ctx,
		physicalTableID: task.PhysicalTableID,
		colsInfo:        task.ColsInfo,
		pkInfo:          task.PKInfo,
		concurrency:     b.ctx.GetSessionVars().DistSQLScanConcurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:    tipb.AnalyzeType_TypeColumn,
			Flags: sc.PushDownFlags(),
		},
	}
	depth := int32(defaultCMSketchDepth)
	width := int32(defaultCMSketchWidth)
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    int64(defaultNumBuckets),
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		ColumnsInfo:   model.ColumnsToProto(cols, task.PKInfo != nil),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	b.err = plannercore.SetPBColumnsDefaultValue(b.ctx, e.analyzePB.ColReq.ColumnsInfo, cols)
	return &analyzeTask{taskType: colTask, colExec: e}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
		wg:           &sync.WaitGroup{},
	}
	for _, task := range v.ColTasks {
		e.tasks = append(e.tasks, b.buildAnalyzeColumnsPushdown(task))
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task))
		if b.err != nil {
			return nil
		}
	}
	return e
}

func constructDistExec(sctx sessionctx.Context, plans []plannercore.PhysicalPlan) ([]*tipb.Executor, error) {
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx)
		if err != nil {
			return nil, err
		}
		executors = append(executors, execPB)
	}
	return executors, nil
}

func (b *executorBuilder) constructDAGReq(plans []plannercore.PhysicalPlan) (dagReq *tipb.DAGRequest, err error) {
	dagReq = &tipb.DAGRequest{}
	sc := b.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = sc.PushDownFlags()
	dagReq.Executors, err = constructDistExec(b.ctx, plans)
	return dagReq, err
}

func buildNoRangeTableReader(b *executorBuilder, v *plannercore.PhysicalTableReader) (*TableReaderExecutor, error) {
	dagReq, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, err
	}
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	tbl, _ := b.is.TableByID(ts.Table.ID)
	startTS, err := b.getStartTS()
	if err != nil {
		return nil, err
	}
	e := &TableReaderExecutor{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:        dagReq,
		startTS:      startTS,
		table:        tbl,
		keepOrder:    ts.KeepOrder,
		desc:         ts.Desc,
		columns:      ts.Columns,
		plans:        v.TablePlans,
	}

	for i := range v.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

// buildTableReader builds a table reader executor. It first build a no range table reader,
// and then update it ranges from table scan plan.
func (b *executorBuilder) buildTableReader(v *plannercore.PhysicalTableReader) *TableReaderExecutor {
	ret, err := buildNoRangeTableReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	dagReq, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	tbl, _ := b.is.TableByID(is.Table.ID)
	startTS, err := b.getStartTS()
	if err != nil {
		return nil, err
	}
	e := &IndexReaderExecutor{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:           dagReq,
		startTS:         startTS,
		physicalTableID: is.Table.ID,
		table:           tbl,
		index:           is.Index,
		keepOrder:       is.KeepOrder,
		desc:            is.Desc,
		columns:         is.Columns,
		idxCols:         is.IdxCols,
		colLens:         is.IdxColLens,
		plans:           v.IndexPlans,
		outputColumns:   v.OutputColumns,
	}

	for _, col := range v.OutputColumns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(col.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) *IndexReaderExecutor {
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	return ret
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	indexReq, err := b.constructDAGReq(v.IndexPlans)
	if err != nil {
		return nil, err
	}
	tableReq, err := b.constructDAGReq(v.TablePlans)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	indexReq.OutputOffsets = []uint32{uint32(len(is.Index.Columns))}
	tbl, _ := b.is.TableByID(is.Table.ID)

	for i := 0; i < v.Schema().Len(); i++ {
		tableReq.OutputOffsets = append(tableReq.OutputOffsets, uint32(i))
	}

	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)
	startTS, err := b.getStartTS()
	if err != nil {
		return nil, err
	}
	e := &IndexLookUpExecutor{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
		dagPB:             indexReq,
		startTS:           startTS,
		table:             tbl,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		tableRequest:      tableReq,
		columns:           ts.Columns,
		dataReaderBuilder: &dataReaderBuilder{executorBuilder: b},
		idxCols:           is.IdxCols,
		colLens:           is.IdxColLens,
		idxPlans:          v.IndexPlans,
		tblPlans:          v.TablePlans,
	}

	if v.ExtraHandleCol != nil {
		e.handleIdx = v.ExtraHandleCol.Index
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) *IndexLookUpExecutor {
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ts := v.TablePlans[0].(*plannercore.PhysicalTableScan)

	ret.ranges = is.Ranges
	sctx := b.ctx.GetSessionVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plannercore.Plan
	*executorBuilder
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []int64) (Executor, error) {
	if e.dagPB.CollectExecutionSummaries == nil {
		colExec := true
		e.dagPB.CollectExecutionSummaries = &colExec
	}
	startTS, err := builder.getStartTS()
	if err != nil {
		return nil, err
	}

	sort.Sort(sortutil.Int64Slice(handles))
	var b distsql.RequestBuilder
	kvReq, err := b.SetTableHandles(getPhysicalTableID(e.table), handles).
		SetDAGRequest(e.dagPB).
		SetStartTS(startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
	e.resultHandler = &tableResultHandler{}
	result, err := distsql.Select(ctx, builder.ctx, kvReq, retTypes(e))
	if err != nil {
		return nil, err
	}
	e.resultHandler.open(nil, result)
	return e, nil
}

func getPhysicalTableID(t table.Table) int64 {
	if p, ok := t.(table.PhysicalTable); ok {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}
