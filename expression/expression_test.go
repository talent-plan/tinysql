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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	res := NewValuesFunc(s.ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestConstant(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	c.Assert(Zero.IsCorrelated(), IsFalse)
	c.Assert(Zero.ConstItem(), IsTrue)
	c.Assert(Zero.Decorrelate(nil).Equal(s.ctx, Zero), IsTrue)
	c.Assert(Zero.HashCode(sc), DeepEquals, []byte{0x0, 0x8, 0x0})
	c.Assert(Zero.Equal(s.ctx, One), IsFalse)
	res, err := Zero.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x30, 0x22})
}

func (s *testEvaluatorSuite) TestIsBinaryLiteral(c *C) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeEnum)}
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeSet
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeBit
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeDuration
	c.Assert(IsBinaryLiteral(col), IsFalse)

	con := &Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum([]byte{byte(0), byte(1)})}
	c.Assert(IsBinaryLiteral(con), IsTrue)
	con.Value = types.NewIntDatum(1)
	c.Assert(IsBinaryLiteral(con), IsFalse)
}

var (
	year, month, day = time.Now().In(time.UTC).Date()
	tm               = types.Time{
		Time: types.FromDate(year, int(month), day, 12, 59, 59, 0),
		Type: mysql.TypeDatetime,
		Fsp:  types.DefaultFsp}
	tmWithFsp = types.Time{
		Time: types.FromDate(year, int(month), day, 12, 59, 59, 555000),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp}
	duration = types.Duration{
		Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second),
		Fsp:      types.DefaultFsp}
	// durationDatum indicates duration "12:59:59".
	durationDatum   = types.NewDatum(duration)
	durationWithFsp = types.Duration{
		Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond),
		Fsp:      3}
	dt = types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  types.DefaultFsp}
)
