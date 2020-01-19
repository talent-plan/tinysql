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

package table

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (t *testTableSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	col := ToColumn(&model.ColumnInfo{
		FieldType: *types.NewFieldType(mysql.TypeTiny),
		State:     model.StatePublic,
	})
	col.Flen = 2
	col.Decimal = 1
	col.Charset = mysql.DefaultCharset
	col.Collate = mysql.DefaultCollationName
	col.Flag |= mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag

	c.Assert(col.GetTypeDesc(), Equals, "tinyint(2) unsigned zerofill")
	col.ToInfo()
	tbInfo := &model.TableInfo{}
	c.Assert(col.IsPKHandleColumn(tbInfo), Equals, false)
	tbInfo.PKIsHandle = true
	col.Flag |= mysql.PriKeyFlag
	c.Assert(col.IsPKHandleColumn(tbInfo), Equals, true)

	cs := col.String()
	c.Assert(len(cs), Greater, 0)

	col.Tp = mysql.TypeEnum
	col.Flag = 0
	col.Elems = []string{"a", "b"}

	c.Assert(col.GetTypeDesc(), Equals, "enum('a','b')")

	col.Elems = []string{"'a'", "b"}
	c.Assert(col.GetTypeDesc(), Equals, "enum('''a''','b')")

	col.Tp = mysql.TypeFloat
	col.Flen = 8
	col.Decimal = -1
	c.Assert(col.GetTypeDesc(), Equals, "float")

	col.Decimal = 1
	c.Assert(col.GetTypeDesc(), Equals, "float(8,1)")

	col.Tp = mysql.TypeDatetime
	col.Decimal = 6
	c.Assert(col.GetTypeDesc(), Equals, "datetime(6)")

	col.Decimal = 0
	c.Assert(col.GetTypeDesc(), Equals, "datetime")

	col.Decimal = -1
	c.Assert(col.GetTypeDesc(), Equals, "datetime")
}

func (t *testTableSuite) TestFind(c *C) {
	defer testleak.AfterTest(c)()
	cols := []*Column{
		newCol("a"),
		newCol("b"),
		newCol("c"),
	}
	FindCols(cols, []string{"a"}, true)
	FindCols(cols, []string{"d"}, true)
	cols[0].Flag |= mysql.OnUpdateNowFlag
	FindOnUpdateCols(cols)
}

func (t *testTableSuite) TestCheck(c *C) {
	defer testleak.AfterTest(c)()
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag
	cols := []*Column{col, col}
	CheckOnce(cols)
	cols = cols[:1]
	CheckNotNull(cols, types.MakeDatums(nil))
	cols[0].Flag |= mysql.NotNullFlag
	CheckNotNull(cols, types.MakeDatums(nil))
	CheckOnce([]*Column{})
}

func (t *testTableSuite) TestHandleBadNull(c *C) {
	col := newCol("a")
	sc := new(stmtctx.StatementContext)
	d, err := col.HandleBadNull(types.Datum{}, sc)
	c.Assert(err, IsNil)
	cmp, err := d.CompareDatum(sc, &types.Datum{})
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	col.Flag |= mysql.NotNullFlag
	d, err = col.HandleBadNull(types.Datum{}, sc)
	c.Assert(err, NotNil)

	sc.BadNullAsWarning = true
	d, err = col.HandleBadNull(types.Datum{}, sc)
	c.Assert(err, IsNil)
}

func (t *testTableSuite) TestGetZeroValue(c *C) {
	tests := []struct {
		ft    *types.FieldType
		value types.Datum
	}{
		{
			types.NewFieldType(mysql.TypeLong),
			types.NewIntDatum(0),
		},
		{
			&types.FieldType{
				Tp:   mysql.TypeLonglong,
				Flag: mysql.UnsignedFlag,
			},
			types.NewUintDatum(0),
		},
		{
			types.NewFieldType(mysql.TypeFloat),
			types.NewFloat32Datum(0),
		},
		{
			types.NewFieldType(mysql.TypeDouble),
			types.NewFloat64Datum(0),
		},
		{
			types.NewFieldType(mysql.TypeVarchar),
			types.NewStringDatum(""),
		},
		{
			types.NewFieldType(mysql.TypeBlob),
			types.NewBytesDatum([]byte{}),
		},
		{
			&types.FieldType{
				Tp:      mysql.TypeString,
				Flen:    2,
				Charset: charset.CharsetBin,
				Collate: charset.CollationBin,
			},
			types.NewDatum(make([]byte, 2)),
		},
		{
			&types.FieldType{
				Tp:      mysql.TypeString,
				Flen:    2,
				Charset: charset.CharsetUTF8MB4,
				Collate: charset.CollationBin,
			},
			types.NewDatum(""),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		colInfo := &model.ColumnInfo{FieldType: *tt.ft}
		zv := GetZeroValue(colInfo)
		c.Assert(zv.Kind(), Equals, tt.value.Kind())
		cmp, err := zv.CompareDatum(sc, &tt.value)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (t *testTableSuite) TestGetDefaultValue(c *C) {
	ctx := mock.NewContext()
	tests := []struct {
		colInfo *model.ColumnInfo
		strict  bool
		val     types.Datum
		err     error
	}{
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
				OriginDefaultValue: 1.0,
				DefaultValue:       1.0,
			},
			false,
			types.NewIntDatum(1),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
			},
			false,
			types.NewIntDatum(0),
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp: mysql.TypeLonglong,
				},
			},
			false,
			types.Datum{},
			nil,
		},
		{
			&model.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag | mysql.AutoIncrementFlag,
				},
			},
			true,
			types.NewIntDatum(0),
			nil,
		},
	}

	for _, tt := range tests {
		ctx.GetSessionVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetColDefaultValue(ctx, tt.colInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		c.Assert(val, DeepEquals, tt.val)
	}

	for _, tt := range tests {
		ctx.GetSessionVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetColOriginDefaultValue(ctx, tt.colInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		c.Assert(val, DeepEquals, tt.val)
	}
}

func newCol(name string) *Column {
	return ToColumn(&model.ColumnInfo{
		Name:  model.NewCIStr(name),
		State: model.StatePublic,
	})
}
