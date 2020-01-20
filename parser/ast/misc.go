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

package ast

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

var (
	_ StmtNode = &AdminStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &BinlogStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &DeallocateStmt{}
	_ StmtNode = &DoStmt{}
	_ StmtNode = &ExecuteStmt{}
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &PrepareStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &UseStmt{}
	_ StmtNode = &FlushStmt{}
	_ StmtNode = &ShutdownStmt{}

	_ Node = &PrivElem{}
	_ Node = &VariableAssignment{}
)

// Isolation level constants.
const (
	ReadCommitted   = "READ-COMMITTED"
	ReadUncommitted = "READ-UNCOMMITTED"
	Serializable    = "SERIALIZABLE"
	RepeatableRead  = "REPEATABLE-READ"

	// Valid formats for explain statement.
	ExplainFormatROW  = "row"
	ExplainFormatDOT  = "dot"
	ExplainFormatHint = "hint"
	PumpType          = "PUMP"
	DrainerType       = "DRAINER"
)

// Transaction mode constants.
const (
	Optimistic  = "OPTIMISTIC"
	Pessimistic = "PESSIMISTIC"
)

var (
	// ExplainFormats stores the valid formats for explain statement, used by validator.
	ExplainFormats = []string{
		ExplainFormatROW,
		ExplainFormatDOT,
		ExplainFormatHint,
	}
)

// TypeOpt is used for parsing data type option from SQL.
type TypeOpt struct {
	IsUnsigned bool
	IsZerofill bool
}

// FloatOpt is used for parsing floating-point type option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html
type FloatOpt struct {
	Flen    int
	Decimal int
}

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// ByAuthString set as true, if AuthString is used for authorization. Otherwise, authorization is done by HashString.
	ByAuthString bool
	AuthString   string
	HashString   string
	// TODO: support auth_plugin
}

// TraceStmt is a statement to trace what sql actually does at background.
type TraceStmt struct {
	stmtNode

	Stmt   StmtNode
	Format string
}

// Accept implements Node Accept interface.
func (n *TraceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TraceStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// ExplainForStmt is a statement to provite information about how is SQL statement executeing
// in connection #ConnectionID
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainForStmt struct {
	stmtNode

	Format       string
	ConnectionID uint64
}

// Restore implements Node interface.
func (n *ExplainForStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("EXPLAIN ")
	ctx.WriteKeyWord("FORMAT ")
	ctx.WritePlain("= ")
	ctx.WriteString(n.Format)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord("FOR ")
	ctx.WriteKeyWord("CONNECTION ")
	ctx.WritePlain(strconv.FormatUint(n.ConnectionID, 10))
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainForStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainForStmt)
	return v.Leave(n)
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt    StmtNode
	Format  string
	Analyze bool
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(DMLNode)
	return v.Leave(n)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	Name    string
	SQLText string
	SQLVar  *VariableExpr
}

// Restore implements Node interface.
func (n *PrepareStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("PREPARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FROM ")
	if n.SQLText != "" {
		ctx.WriteString(n.SQLText)
		return nil
	}
	if n.SQLVar != nil {
		if err := n.SQLVar.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PrepareStmt.SQLVar")
		}
		return nil
	}
	return errors.New("An error occurred while restore PrepareStmt")
}

// Accept implements Node Accept interface.
func (n *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrepareStmt)
	if n.SQLVar != nil {
		node, ok := n.SQLVar.Accept(v)
		if !ok {
			return n, false
		}
		n.SQLVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *DeallocateStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("DEALLOCATE PREPARE ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeallocateStmt)
	return v.Leave(n)
}

// Prepared represents a prepared statement.
type Prepared struct {
	Stmt          StmtNode
	StmtType      string
	Params        []ParamMarkerExpr
	SchemaVersion int64
	UseCache      bool
	CachedPlan    interface{}
	CachedNames   interface{}
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name       string
	UsingVars  []ExprNode
	BinaryArgs interface{}
	ExecID     uint32
	IdxInMulti int
}

// Accept implements Node Accept interface.
func (n *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExecuteStmt)
	for i, val := range n.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
	Mode     string
	ReadOnly bool
	Bound    *TimestampBound
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BeginStmt)
	if n.Bound != nil && n.Bound.Timestamp != nil {
		newTimestamp, ok := n.Bound.Timestamp.Accept(v)
		if !ok {
			return n, false
		}
		n.Bound.Timestamp = newTimestamp.(ExprNode)
	}
	return v.Leave(n)
}

// BinlogStmt is an internal-use statement.
// We just parse and ignore it.
// See http://dev.mysql.com/doc/refman/5.7/en/binlog.html
type BinlogStmt struct {
	stmtNode
	Str string
}

// Restore implements Node interface.
func (n *BinlogStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("BINLOG ")
	ctx.WriteString(n.Str)
	return nil
}

// Accept implements Node Accept interface.
func (n *BinlogStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BinlogStmt)
	return v.Leave(n)
}

// CommitStmt is a statement to commit the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
}

// Accept implements Node Accept interface.
func (n *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CommitStmt)
	return v.Leave(n)
}

// RollbackStmt is a statement to roll back the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *RollbackStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("ROLLBACK")
	return nil
}

// Accept implements Node Accept interface.
func (n *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RollbackStmt)
	return v.Leave(n)
}

// UseStmt is a statement to use the DBName database as the current database.
// See https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Restore implements Node interface.
func (n *UseStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("USE ")
	ctx.WriteName(n.DBName)
	return nil
}

// Accept implements Node Accept interface.
func (n *UseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UseStmt)
	return v.Leave(n)
}

const (
	// SetNames is the const for set names/charset stmt.
	// If VariableAssignment.Name == Names, it should be set names/charset stmt.
	SetNames = "SetNAMES"
)

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	node
	Name     string
	Value    ExprNode
	IsGlobal bool
	IsSystem bool

	// ExtendValue is a way to store extended info.
	// VariableAssignment should be able to store information for SetCharset/SetPWD Stmt.
	// For SetCharsetStmt, Value is charset, ExtendValue is collation.
	// TODO: Use SetStmt to implement set password statement.
	ExtendValue ValueExpr
}

// Accept implements Node interface.
func (n *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableAssignment)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// FlushStmtType is the type for FLUSH statement.
type FlushStmtType int

// Flush statement types.
const (
	FlushNone FlushStmtType = iota
	FlushTables
	FlushPrivileges
	FlushStatus
	FlushTiDBPlugin
	FlushHosts
	FlushLogs
)

// FlushStmt is a statement to flush tables/privileges/optimizer costs and so on.
type FlushStmt struct {
	stmtNode

	Tp              FlushStmtType // Privileges/Tables/...
	NoWriteToBinLog bool
	Tables          []*TableName // For FlushTableStmt, if Tables is empty, it means flush all tables.
	ReadLock        bool
	Plugins         []string
}

// Restore implements Node interface.
func (n *FlushStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("FLUSH ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	switch n.Tp {
	case FlushTables:
		ctx.WriteKeyWord("TABLES")
		for i, v := range n.Tables {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore FlushStmt.Tables[%d]", i)
			}
		}
		if n.ReadLock {
			ctx.WriteKeyWord(" WITH READ LOCK")
		}
	case FlushPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	case FlushStatus:
		ctx.WriteKeyWord("STATUS")
	case FlushTiDBPlugin:
		ctx.WriteKeyWord("TIDB PLUGINS")
		for i, v := range n.Plugins {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(v)
		}
	case FlushHosts:
		ctx.WriteKeyWord("HOSTS")
	case FlushLogs:
		ctx.WriteKeyWord("LOGS")
	default:
		return errors.New("Unsupported type of FlushStmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FlushStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FlushStmt)
	return v.Leave(n)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Accept implements Node Accept interface.
func (n *SetStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetStmt)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

type ChangeStmt struct {
	stmtNode

	NodeType string
	State    string
	NodeID   string
}

// Restore implements Node interface.
func (n *ChangeStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("CHANGE ")
	ctx.WriteKeyWord(n.NodeType)
	ctx.WriteKeyWord(" TO NODE_STATE ")
	ctx.WritePlain("=")
	ctx.WriteString(n.State)
	ctx.WriteKeyWord(" FOR NODE_ID ")
	ctx.WriteString(n.NodeID)
	return nil
}

// SecureText implements SensitiveStatement interface.
func (n *ChangeStmt) SecureText() string {
	return fmt.Sprintf("change %s to node_state='%s' for node_id '%s'", strings.ToLower(n.NodeType), n.State, n.NodeID)
}

// Accept implements Node Accept interface.
func (n *ChangeStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ChangeStmt)
	return v.Leave(n)
}

const (
	TslNone = iota
	Ssl
	X509
	Cipher
	Issuer
	Subject
)

type TLSOption struct {
	Type  int
	Value string
}

func (t *TLSOption) Restore(ctx *RestoreCtx) error {
	switch t.Type {
	case TslNone:
		ctx.WriteKeyWord("NONE")
	case Ssl:
		ctx.WriteKeyWord("SSL")
	case X509:
		ctx.WriteKeyWord("X509")
	case Cipher:
		ctx.WriteKeyWord("CIPHER ")
		ctx.WriteString(t.Value)
	case Issuer:
		ctx.WriteKeyWord("ISSUER ")
		ctx.WriteString(t.Value)
	case Subject:
		ctx.WriteKeyWord("SUBJECT ")
		ctx.WriteString(t.Value)
	default:
		return errors.Errorf("Unsupported TLSOption.Type %d", t.Type)
	}
	return nil
}

const (
	MaxQueriesPerHour = iota + 1
	MaxUpdatesPerHour
	MaxConnectionsPerHour
	MaxUserConnections
)

type ResourceOption struct {
	Type  int
	Count int64
}

func (r *ResourceOption) Restore(ctx *RestoreCtx) error {
	switch r.Type {
	case MaxQueriesPerHour:
		ctx.WriteKeyWord("MAX_QUERIES_PER_HOUR ")
	case MaxUpdatesPerHour:
		ctx.WriteKeyWord("MAX_UPDATES_PER_HOUR ")
	case MaxConnectionsPerHour:
		ctx.WriteKeyWord("MAX_CONNECTIONS_PER_HOUR ")
	case MaxUserConnections:
		ctx.WriteKeyWord("MAX_USER_CONNECTIONS ")
	default:
		return errors.Errorf("Unsupported ResourceOption.Type %d", r.Type)
	}
	ctx.WritePlainf("%d", r.Count)
	return nil
}

// DoStmt is the struct for DO statement.
type DoStmt struct {
	stmtNode

	Exprs []ExprNode
}

// Accept implements Node Accept interface.
func (n *DoStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DoStmt)
	for i, val := range n.Exprs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Exprs[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// AdminStmtType is the type for admin statement.
type AdminStmtType int

// Admin statement types.
const (
	AdminShowDDL = iota + 1
	AdminCheckTable
	AdminShowDDLJobs
	AdminCancelDDLJobs
	AdminCheckIndex
	AdminRecoverIndex
	AdminCleanupIndex
	AdminCheckIndexRange
	AdminShowDDLJobQueries
	AdminChecksumTable
	AdminShowSlow
	AdminShowNextRowID
	AdminReloadExprPushdownBlacklist
	AdminReloadOptRuleBlacklist
	AdminPluginDisable
	AdminPluginEnable
	AdminFlushBindings
	AdminCaptureBindings
	AdminEvolveBindings
)

// HandleRange represents a range where handle value >= Begin and < End.
type HandleRange struct {
	Begin int64
	End   int64
}

// ShowSlowType defines the type for SlowSlow statement.
type ShowSlowType int

const (
	// ShowSlowTop is a ShowSlowType constant.
	ShowSlowTop ShowSlowType = iota
	// ShowSlowRecent is a ShowSlowType constant.
	ShowSlowRecent
)

// ShowSlowKind defines the kind for SlowSlow statement when the type is ShowSlowTop.
type ShowSlowKind int

const (
	// ShowSlowKindDefault is a ShowSlowKind constant.
	ShowSlowKindDefault ShowSlowKind = iota
	// ShowSlowKindInternal is a ShowSlowKind constant.
	ShowSlowKindInternal
	// ShowSlowKindAll is a ShowSlowKind constant.
	ShowSlowKindAll
)

// ShowSlow is used for the following command:
//	admin show slow top [ internal | all] N
//	admin show slow recent N
type ShowSlow struct {
	Tp    ShowSlowType
	Count uint64
	Kind  ShowSlowKind
}

// Restore implements Node interface.
func (n *ShowSlow) Restore(ctx *RestoreCtx) error {
	switch n.Tp {
	case ShowSlowRecent:
		ctx.WriteKeyWord("RECENT ")
	case ShowSlowTop:
		ctx.WriteKeyWord("TOP ")
		switch n.Kind {
		case ShowSlowKindDefault:
			// do nothing
		case ShowSlowKindInternal:
			ctx.WriteKeyWord("INTERNAL ")
		case ShowSlowKindAll:
			ctx.WriteKeyWord("ALL ")
		default:
			return errors.New("Unsupported kind of ShowSlowTop")
		}
	default:
		return errors.New("Unsupported type of ShowSlow")
	}
	ctx.WritePlainf("%d", n.Count)
	return nil
}

// AdminStmt is the struct for Admin statement.
type AdminStmt struct {
	stmtNode

	Tp        AdminStmtType
	Index     string
	Tables    []*TableName
	JobIDs    []int64
	JobNumber int64

	HandleRanges []HandleRange
	ShowSlow     *ShowSlow
	Plugins      []string
	Where        ExprNode
}

// Accept implements Node Accept interface.
func (n *AdminStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*AdminStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}

	return v.Leave(n)
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	node

	Priv mysql.PrivilegeType
	Cols []*ColumnName
}

// Restore implements Node interface.
func (n *PrivElem) Restore(ctx *RestoreCtx) error {
	if n.Priv == 0 {
		ctx.WritePlain("/* UNSUPPORTED TYPE */")
	} else if n.Priv == mysql.AllPriv {
		ctx.WriteKeyWord("ALL")
	} else {
		str, ok := mysql.Priv2Str[n.Priv]
		if ok {
			ctx.WriteKeyWord(str)
		} else {
			return errors.New("Undefined privilege type")
		}
	}
	if n.Cols != nil {
		ctx.WritePlain(" (")
		for i, v := range n.Cols {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore PrivElem.Cols[%d]", i)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PrivElem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrivElem)
	for i, val := range n.Cols {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// ObjectTypeType is the type for object type.
type ObjectTypeType int

const (
	// ObjectTypeNone is for empty object type.
	ObjectTypeNone ObjectTypeType = iota + 1
	// ObjectTypeTable means the following object is a table.
	ObjectTypeTable
)

// Restore implements Node interface.
func (n ObjectTypeType) Restore(ctx *RestoreCtx) error {
	switch n {
	case ObjectTypeNone:
		// do nothing
	case ObjectTypeTable:
		ctx.WriteKeyWord("TABLE")
	default:
		return errors.New("Unsupported object type")
	}
	return nil
}

// ShutdownStmt is a statement to stop the TiDB server.
// See https://dev.mysql.com/doc/refman/5.7/en/shutdown.html
type ShutdownStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *ShutdownStmt) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord("SHUTDOWN")
	return nil
}

// Accept implements Node Accept interface.
func (n *ShutdownStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ShutdownStmt)
	return v.Leave(n)
}

// Ident is the table identifier composed of schema name and table name.
type Ident struct {
	Schema model.CIStr
	Name   model.CIStr
}

// String implements fmt.Stringer interface.
func (i Ident) String() string {
	if i.Schema.O == "" {
		return i.Name.O
	}
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}

// SelectStmtOpts wrap around select hints and switches
type SelectStmtOpts struct {
	Distinct        bool
	SQLBigResult    bool
	SQLBufferResult bool
	SQLCache        bool
	SQLSmallResult  bool
	CalcFoundRows   bool
	StraightJoin    bool
	Priority        mysql.PriorityEnum
	TableHints      []*TableOptimizerHint
}

// TableOptimizerHint is Table level optimizer hint
type TableOptimizerHint struct {
	node
	// HintName is the name or alias of the table(s) which the hint will affect.
	// Table hints has no schema info
	// It allows only table name or alias (if table has an alias)
	HintName model.CIStr
	// QBName is the default effective query block of this hint.
	QBName    model.CIStr
	Tables    []HintTable
	Indexes   []model.CIStr
	StoreType model.CIStr
	// Statement Execution Time Optimizer Hints
	// See https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html#optimizer-hints-execution-time
	MaxExecutionTime uint64
	MemoryQuota      int64
	QueryType        model.CIStr
	HintFlag         bool
}

// HintTable is table in the hint. It may have query block info.
type HintTable struct {
	DBName    model.CIStr
	TableName model.CIStr
	QBName    model.CIStr
}

func (ht *HintTable) Restore(ctx *RestoreCtx) {
	if ht.DBName.L != "" {
		ctx.WriteName(ht.DBName.String())
		ctx.WriteKeyWord(".")
	}
	ctx.WriteName(ht.TableName.String())
	if ht.QBName.L != "" {
		ctx.WriteKeyWord("@")
		ctx.WriteName(ht.QBName.String())
	}
}

// Restore implements Node interface.
func (n *TableOptimizerHint) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(n.HintName.String())
	ctx.WritePlain("(")
	if n.QBName.L != "" {
		if n.HintName.L != "qb_name" {
			ctx.WriteKeyWord("@")
		}
		ctx.WriteName(n.QBName.String())
	}
	// Hints without args except query block.
	switch n.HintName.L {
	case "hash_agg", "stream_agg", "agg_to_cop", "read_consistent_replica", "no_index_merge", "qb_name":
		ctx.WritePlain(")")
		return nil
	}
	if n.QBName.L != "" {
		ctx.WritePlain(" ")
	}
	// Hints with args except query block.
	switch n.HintName.L {
	case "max_execution_time":
		ctx.WritePlainf("%d", n.MaxExecutionTime)
	case "tidb_hj", "tidb_smj", "tidb_inlj", "hash_join", "sm_join", "inl_join":
		for i, table := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			table.Restore(ctx)
		}
	case "use_index", "ignore_index", "use_index_merge":
		n.Tables[0].Restore(ctx)
		ctx.WritePlain(" ")
		for i, index := range n.Indexes {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(index.String())
		}
	case "use_toja", "enable_plan_cache":
		if n.HintFlag {
			ctx.WritePlain("TRUE")
		} else {
			ctx.WritePlain("FALSE")
		}
	case "query_type":
		ctx.WriteKeyWord(n.QueryType.String())
	case "memory_quota":
		ctx.WritePlainf("%d MB", n.MemoryQuota/1024/1024)
	case "read_from_storage":
		ctx.WriteKeyWord(n.StoreType.String())
		for i, table := range n.Tables {
			if i == 0 {
				ctx.WritePlain("[")
			}
			table.Restore(ctx)
			if i == len(n.Tables)-1 {
				ctx.WritePlain("]")
			} else {
				ctx.WritePlain(", ")
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *TableOptimizerHint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableOptimizerHint)
	return v.Leave(n)
}

type BinaryLiteral interface {
	ToString() string
}

// NewDecimal creates a types.Decimal value, it's provided by parser driver.
var NewDecimal func(string) (interface{}, error)

// NewHexLiteral creates a types.HexLiteral value, it's provided by parser driver.
var NewHexLiteral func(string) (interface{}, error)

// NewBitLiteral creates a types.BitLiteral value, it's provided by parser driver.
var NewBitLiteral func(string) (interface{}, error)
