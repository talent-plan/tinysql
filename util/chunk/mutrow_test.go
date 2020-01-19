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

package chunk

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"testing"
)

var allTypes = []*types.FieldType{
	types.NewFieldType(mysql.TypeTiny),
	types.NewFieldType(mysql.TypeShort),
	types.NewFieldType(mysql.TypeInt24),
	types.NewFieldType(mysql.TypeLong),
	types.NewFieldType(mysql.TypeLonglong),
	{
		Tp:      mysql.TypeLonglong,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
	},
	types.NewFieldType(mysql.TypeYear),
	types.NewFieldType(mysql.TypeFloat),
	types.NewFieldType(mysql.TypeDouble),
	types.NewFieldType(mysql.TypeString),
	types.NewFieldType(mysql.TypeVarString),
	types.NewFieldType(mysql.TypeVarchar),
	types.NewFieldType(mysql.TypeBlob),
	types.NewFieldType(mysql.TypeTinyBlob),
	types.NewFieldType(mysql.TypeMediumBlob),
	types.NewFieldType(mysql.TypeLongBlob),
	types.NewFieldType(mysql.TypeDate),
	types.NewFieldType(mysql.TypeDatetime),
	types.NewFieldType(mysql.TypeTimestamp),
	types.NewFieldType(mysql.TypeDuration),
	types.NewFieldType(mysql.TypeNewDecimal),
	{
		Tp:      mysql.TypeSet,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	{
		Tp:      mysql.TypeEnum,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    mysql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	types.NewFieldType(mysql.TypeBit),
}

func (s *testChunkSuite) TestMutRow(c *check.C) {
	mutRow := MutRowFromTypes(allTypes)
	row := mutRow.ToRow()
	sc := new(stmtctx.StatementContext)
	for i := 0; i < row.Len(); i++ {
		val := zeroValForType(allTypes[i])
		d := row.GetDatum(i, allTypes[i])
		d2 := types.NewDatum(val)
		cmp, err := d.CompareDatum(sc, &d2)
		c.Assert(err, check.IsNil)
		c.Assert(cmp, check.Equals, 0)
	}

	mutRow = MutRowFromValues("abc", 123)
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(mutRow.ToRow().GetString(0), check.Equals, "abc")
	c.Assert(row.IsNull(1), check.IsFalse)
	c.Assert(mutRow.ToRow().GetInt64(1), check.Equals, int64(123))
	mutRow.SetValues("abcd", 456)
	row = mutRow.ToRow()
	c.Assert(row.GetString(0), check.Equals, "abcd")
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.GetInt64(1), check.Equals, int64(456))
	c.Assert(row.IsNull(1), check.IsFalse)
	mutRow.SetDatums(types.NewStringDatum("defgh"), types.NewIntDatum(33))
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.GetString(0), check.Equals, "defgh")
	c.Assert(row.IsNull(1), check.IsFalse)
	c.Assert(row.GetInt64(1), check.Equals, int64(33))

	mutRow.SetRow(MutRowFromValues("foobar", nil).ToRow())
	row = mutRow.ToRow()
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.IsNull(1), check.IsTrue)

	nRow := MutRowFromValues(nil, 111).ToRow()
	c.Assert(nRow.IsNull(0), check.IsTrue)
	c.Assert(nRow.IsNull(1), check.IsFalse)
	mutRow.SetRow(nRow)
	row = mutRow.ToRow()
	c.Assert(row.IsNull(0), check.IsTrue)
	c.Assert(row.IsNull(1), check.IsFalse)
}

func BenchmarkMutRowSetDatums(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	datums := []types.Datum{types.NewDatum(1), types.NewDatum("abcd")}
	for i := 0; i < b.N; i++ {
		mutRow.SetDatums(datums...)
	}
}

func BenchmarkMutRowSetValues(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	for i := 0; i < b.N; i++ {
		mutRow.SetValues(1, "abcd")
	}
}

func BenchmarkMutRowFromTypes(b *testing.B) {
	b.ReportAllocs()
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarchar),
	}
	for i := 0; i < b.N; i++ {
		MutRowFromTypes(tps)
	}
}

func BenchmarkMutRowFromDatums(b *testing.B) {
	b.ReportAllocs()
	datums := []types.Datum{types.NewDatum(1), types.NewDatum("abc")}
	for i := 0; i < b.N; i++ {
		MutRowFromDatums(datums)
	}
}

func BenchmarkMutRowFromValues(b *testing.B) {
	b.ReportAllocs()
	values := []interface{}{1, "abc"}
	for i := 0; i < b.N; i++ {
		MutRowFromValues(values)
	}
}

func (s *testChunkSuite) TestMutRowShallowCopyPartialRow(c *check.C) {
	colTypes := make([]*types.FieldType, 0, 3)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})

	mutRow := MutRowFromTypes(colTypes)
	row := MutRowFromValues("abc", 123).ToRow()
	mutRow.ShallowCopyPartialRow(0, row)
	c.Assert(row.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(row.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))

	row.c.Reset()
	d := types.NewStringDatum("dfg")
	row.c.AppendDatum(0, &d)
	d = types.NewIntDatum(567)
	row.c.AppendDatum(1, &d)

	c.Assert(row.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(row.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))
}

var rowsNum = 1024

func BenchmarkMutRowShallowCopyPartialRow(b *testing.B) {
	b.ReportAllocs()
	colTypes := make([]*types.FieldType, 0, 8)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})

	mutRow := MutRowFromTypes(colTypes)
	row := MutRowFromValues("abc", "abcdefg", 123, 456).ToRow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < rowsNum; j++ {
			mutRow.ShallowCopyPartialRow(0, row)
		}
	}
}
