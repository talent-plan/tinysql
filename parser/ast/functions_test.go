// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/parser/ast"
	. "github.com/pingcap/tidb/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestFunctionsVisitorCover(c *C) {
	valueExpr := NewValueExpr(42)
	stmts := []Node{
		&AggregateFuncExpr{Args: []ExprNode{valueExpr}},
		&FuncCallExpr{Args: []ExprNode{valueExpr}},
		&FuncCastExpr{Expr: valueExpr},
	}

	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testFunctionsSuite) TestConvert(c *C) {
	// Test case for CONVERT(expr USING transcoding_name).
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CONVERT("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CONVERT("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CONVERT("abc" USING "binary")`, "binary", ""},
		{`SELECT CONVERT("abc" USING biNaRy)`, "binary", ""},
		{`SELECT CONVERT(a USING a)`, "", `[parser:1115]Unknown character set: 'a'`}, // TiDB issue #4436.
		{`SELECT CONVERT("abc" USING CONCAT("utf", "8"))`, "", `[parser:1115]Unknown character set: 'CONCAT'`},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			c.Assert(err.Error(), Equals, testCase.ErrorMessage)
			continue
		}
		c.Assert(err, IsNil)

		st := stmt.(*ast.SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*driver.ValueExpr)
		c.Assert(charsetArg.GetString(), Equals, testCase.CharsetName)
	}
}

func (ts *testFunctionsSuite) TestChar(c *C) {
	// Test case for CHAR(N USING charset_name)
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CHAR("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CHAR("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CHAR("abc" USING "binary")`, "binary", ""},
		{`SELECT CHAR("abc" USING binary)`, "binary", ""},
		{`SELECT CHAR(a USING a)`, "", `[parser:1115]Unknown character set: 'a'`},
		{`SELECT CHAR("abc" USING CONCAT("utf", "8"))`, "", `[parser:1115]Unknown character set: 'CONCAT'`},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			c.Assert(err.Error(), Equals, testCase.ErrorMessage)
			continue
		}
		c.Assert(err, IsNil)

		st := stmt.(*ast.SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*driver.ValueExpr)
		c.Assert(charsetArg.GetString(), Equals, testCase.CharsetName)
	}
}
