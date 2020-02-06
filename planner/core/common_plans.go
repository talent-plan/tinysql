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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Table         table.Table
	tableSchema   *expression.Schema
	tableColNames types.NameSlice
	Columns       []*ast.ColumnName
	Lists         [][]expression.Expression
	SetList       []*expression.Assignment

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	SelectPlan PhysicalPlan

	AllAssignmentsAreConstant bool
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	SelectPlan PhysicalPlan

	TblColPosInfos TblColPosInfoSlice
}

// analyzeInfo is used to store the database name, table name and partition name of analyze task.
type analyzeInfo struct {
	DBName          string
	TableName       string
	PhysicalTableID int64
	Incremental     bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	PKInfo   *model.ColumnInfo
	ColsInfo []*model.ColumnInfo
	TblInfo  *model.TableInfo
	analyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	analyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
	Opts     map[ast.AnalyzeOptionType]uint64
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan Plan
	Format     string
	ExecStmt   ast.StmtNode

	Rows           [][]string
	explainedPlans map[int]bool
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)

	switch {
	case format == ast.ExplainFormatROW:
		fieldNames = []string{"id", "count", "task", "operator info"}
	case format == ast.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == ast.ExplainFormatHint:
		fieldNames = []string{"hint"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetPlan == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		e.explainedPlans = map[int]bool{}
		err := e.explainPlanInRowFormat(e.TargetPlan, "root", "", true)
		if err != nil {
			return err
		}
	case ast.ExplainFormatDOT:
		e.prepareDotInfo(e.TargetPlan.(PhysicalPlan))
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p Plan, taskType, indent string, isLastChild bool) (err error) {
	e.prepareOperatorInfo(p, taskType, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := Indent4Child(indent, isLastChild)

	if physPlan, ok := p.(PhysicalPlan); ok {
		for i, child := range physPlan.Children() {
			if e.explainedPlans[child.ID()] {
				continue
			}
			err = e.explainPlanInRowFormat(child, taskType, childIndent, i == len(physPlan.Children())-1)
			if err != nil {
				return
			}
		}
	}

	switch x := p.(type) {
	case *PhysicalTableReader:
		err = e.explainPlanInRowFormat(x.tablePlan, "cop", childIndent, true)
	case *PhysicalIndexReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop", childIndent, true)
	case *PhysicalIndexLookUpReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop", childIndent, false)
		err = e.explainPlanInRowFormat(x.tablePlan, "cop", childIndent, true)
	case *Insert:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", childIndent, true)
		}
	case *Delete:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", childIndent, true)
		}
	}
	return
}

const (
	// TreeBody indicates the current operator sub-tree is not finished, still
	// has child operators to be attached on.
	TreeBody = '│'
	// TreeMiddleNode indicates this operator is not the last child of the
	// current sub-tree rooted by its parent.
	TreeMiddleNode = '├'
	// TreeLastNode indicates this operator is the last child of the current
	// sub-tree rooted by its parent.
	TreeLastNode = '└'
	// TreeGap is used to represent the gap between the branches of the tree.
	TreeGap = ' '
	// TreeNodeIdentifier is used to replace the treeGap once we need to attach
	// a node to a sub-tree.
	TreeNodeIdentifier = '─'
)

// Indent4Child appends more blank to the `indent` string
func Indent4Child(indent string, isLastChild bool) string {
	if !isLastChild {
		return string(append([]rune(indent), TreeBody, TreeGap))
	}

	// If the current node is the last node of the current operator tree, we
	// need to end this sub-tree by changing the closest treeBody to a treeGap.
	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		if indentBytes[i] == TreeBody {
			indentBytes[i] = TreeGap
			break
		}
	}

	return string(append(indentBytes, TreeBody, TreeGap))
}

// PrettyIdentifier returns a pretty identifier which contains indent and tree node hierarchy indicator
func PrettyIdentifier(id, indent string, isLastChild bool) string {
	if len(indent) == 0 {
		return id
	}

	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		if indentBytes[i] != TreeBody {
			continue
		}

		// Here we attach a new node to the current sub-tree by changing
		// the closest treeBody to a:
		// 1. treeLastNode, if this operator is the last child.
		// 2. treeMiddleNode, if this operator is not the last child..
		if isLastChild {
			indentBytes[i] = TreeLastNode
		} else {
			indentBytes[i] = TreeMiddleNode
		}
		break
	}

	// Replace the treeGap between the treeBody and the node to a
	// treeNodeIdentifier.
	indentBytes[len(indentBytes)-1] = TreeNodeIdentifier
	return string(indentBytes) + id
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, task type, operator info, and the estemated row count.
func (e *Explain) prepareOperatorInfo(p Plan, taskType string, indent string, isLastChild bool) {
	operatorInfo := p.ExplainInfo()
	count := "N/A"
	if si := p.statsInfo(); si != nil {
		count = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}
	explainID := p.ExplainID().String()
	row := []string{PrettyIdentifier(explainID, indent, isLastChild), count, taskType, operatorInfo}
	e.Rows = append(e.Rows, row)
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
	}

	var copTasks []PhysicalPlan
	var pipelines []string

	for planQueue := []PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop.(PhysicalPlan), "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p Plan) (bool, error) {
	if !isAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx), nil
	case *PhysicalTableReader:
		tableScan := v.TablePlans[0].(*PhysicalTableScan)
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint(ctx.GetSessionVars().StmtCtx), nil
	default:
		return false, nil
	}
}

// isAutoCommitTxn checks if session is in autocommit mode and not InTxn
// used for fast plan like point get
func isAutoCommitTxn(ctx sessionctx.Context) bool {
	return ctx.GetSessionVars().IsAutocommit() && !ctx.GetSessionVars().InTxn()
}
