// Copyright 2018 PingCAP, Inc.
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

package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func (s *testSuite) TestMergePartialResult4MaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, 4, 4, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, "4", "4", "4"),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, nil, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, nil, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, nil, "4", "4"),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, nil, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, nil, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, nil, "0"),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
