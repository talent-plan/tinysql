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
)

func (s *testSuite) TestMergePartialResult4Sum(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncSum, mysql.TypeLonglong, 5, int64(10), int64(9), int64(19)),
		buildAggTester(ast.AggFuncSum, mysql.TypeDouble, 5, 10.0, 9.0, 19.0),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestSum(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncSum, mysql.TypeLonglong, 5, nil, int64(10)),
		buildAggTester(ast.AggFuncSum, mysql.TypeDouble, 5, nil, 10.0),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
