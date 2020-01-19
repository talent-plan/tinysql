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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *testEvaluatorSuite) TestCompare(c *C) {
	intVal, uintVal, realVal, stringVal := 1, uint64(1), 1.1, "123"
	// test cases for generating function signatures.
	tests := []struct {
		arg0     interface{}
		arg1     interface{}
		funcName string
		tp       byte
		expected int64
	}{
		{intVal, intVal, ast.LT, mysql.TypeLonglong, 0},
		{stringVal, stringVal, ast.LT, mysql.TypeVarString, 0},
		{realVal, realVal, ast.LT, mysql.TypeDouble, 0},
		{uintVal, uintVal, ast.EQ, mysql.TypeLonglong, 1},
	}

	for _, t := range tests {
		bf, err := funcs[t.funcName].getFunction(s.ctx, s.primitiveValsToConstants([]interface{}{t.arg0, t.arg1}))
		c.Assert(err, IsNil)
		args := bf.getArgs()
		c.Assert(args[0].GetType().Tp, Equals, t.tp)
		c.Assert(args[1].GetType().Tp, Equals, t.tp)
		res, isNil, err := bf.evalInt(chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(isNil, IsFalse)
		c.Assert(res, Equals, t.expected)
	}
}
