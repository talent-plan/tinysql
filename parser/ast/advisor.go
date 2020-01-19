// Copyright 2019 PingCAP, Inc.
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

var _ StmtNode = &IndexAdviseStmt{}

// IndexAdviseStmt is used to advise indexes
type IndexAdviseStmt struct {
	stmtNode

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *MaxIndexNumClause
	LinesInfo   *LinesClause
}

// Accept implements Node Accept interface.
func (n *IndexAdviseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexAdviseStmt)
	return v.Leave(n)
}

// MaxIndexNumClause represents 'maximum number of indexes' clause in index advise statement.
type MaxIndexNumClause struct {
	PerTable uint64
	PerDB    uint64
}
