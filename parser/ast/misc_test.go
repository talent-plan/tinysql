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

package ast_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser"
	. "github.com/pingcap/tidb/parser/ast"
)

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

type visitor struct{}

func (v visitor) Enter(in Node) (Node, bool) {
	return in, false
}

func (v visitor) Leave(in Node) (Node, bool) {
	return in, true
}

type visitor1 struct {
	visitor
}

func (visitor1) Enter(in Node) (Node, bool) {
	return in, true
}

func (ts *testMiscSuite) TestDDLVisitorCover(c *C) {
	sql := `
create table t (c1 smallint unsigned, c2 int unsigned);
alter table t add column a smallint unsigned after b;
alter table t add column (a int, constraint check (a > 0));
create index t_i on t (id);
create database test character set utf8;
drop database test;
drop index t_i on t;
drop table t;
truncate t;
create table t (
jobAbbr char(4) not null,
constraint foreign key (jobabbr) references ffxi_jobtype (jobabbr) on delete cascade on update cascade
);
`
	parse := parser.New()
	stmts, _, err := parse.Parse(sql, "", "")
	c.Assert(err, IsNil)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testMiscSuite) TestSensitiveStatement(c *C) {
	negative := []StmtNode{
		&AlterTableStmt{},
		&CreateDatabaseStmt{},
		&CreateIndexStmt{},
		&CreateTableStmt{},
		&DropDatabaseStmt{},
		&DropIndexStmt{},
		&DropTableStmt{},
		&RenameTableStmt{},
		&TruncateTableStmt{},
	}
	for _, stmt := range negative {
		_, ok := stmt.(SensitiveStmtNode)
		c.Assert(ok, IsFalse)
	}
}
