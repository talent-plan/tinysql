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

package codec

import (
	"bytes"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct {
}

func (s *testCodecSuite) TestCodecKey(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		args, err := Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)

		b, err = EncodeValue(sc, nil, t.Input...)
		c.Assert(err, IsNil)
		size, err := estimateValuesSize(sc, t.Input)
		c.Assert(err, IsNil)
		c.Assert(len(b), Equals, size)
		args, err = Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)
	}
}

func estimateValuesSize(sc *stmtctx.StatementContext, vals []types.Datum) (int, error) {
	size := 0
	for _, val := range vals {
		length, err := EstimateValueSize(sc, val)
		if err != nil {
			return 0, err
		}
		size += length
	}
	return size, nil
}

func (s *testCodecSuite) TestCodecKeyCompare(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Left   []types.Datum
		Right  []types.Datum
		Expect int
	}{
		{
			types.MakeDatums(1),
			types.MakeDatums(1),
			0,
		},
		{
			types.MakeDatums(-1),
			types.MakeDatums(1),
			-1,
		},
		{
			types.MakeDatums(3.15),
			types.MakeDatums(3.12),
			1,
		},
		{
			types.MakeDatums("abc"),
			types.MakeDatums("abcd"),
			-1,
		},
		{
			types.MakeDatums("abcdefgh"),
			types.MakeDatums("abcdefghi"),
			-1,
		},
		{
			types.MakeDatums(1, "abc"),
			types.MakeDatums(1, "abcd"),
			-1,
		},
		{
			types.MakeDatums(1, "abc", "def"),
			types.MakeDatums(1, "abcd", "af"),
			-1,
		},
		{
			types.MakeDatums(3.12, "ebc", "def"),
			types.MakeDatums(2.12, "abcd", "af"),
			1,
		},
		{
			types.MakeDatums([]byte{0x01, 0x00}, []byte{0xFF}),
			types.MakeDatums([]byte{0x01, 0x00, 0xFF}),
			-1,
		},
		{
			types.MakeDatums([]byte{0x01}, uint64(0xFFFFFFFFFFFFFFF)),
			types.MakeDatums([]byte{0x01, 0x10}, 0),
			-1,
		},
		{
			types.MakeDatums(0),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums([]byte{0x00}),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(math.SmallestNonzeroFloat64),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(int64(math.MinInt64)),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(1, int64(math.MinInt64), nil),
			types.MakeDatums(1, nil, uint64(math.MaxUint64)),
			1,
		},
		{
			types.MakeDatums(1, []byte{}, nil),
			types.MakeDatums(1, nil, 123),
			1,
		},
		{
			[]types.Datum{types.MinNotNullDatum()},
			[]types.Datum{types.MaxValueDatum()},
			-1,
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, t := range table {
		b1, err := EncodeKey(sc, nil, t.Left...)
		c.Assert(err, IsNil)

		b2, err := EncodeKey(sc, nil, t.Right...)
		c.Assert(err, IsNil)

		c.Assert(bytes.Compare(b1, b2), Equals, t.Expect, Commentf("%v - %v - %v - %v - %v", t.Left, t.Right, b1, b2, t.Expect))
	}
}

func (s *testCodecSuite) TestNumberCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []int64{
		math.MinInt64,
		math.MinInt32,
		math.MinInt16,
		math.MinInt8,
		0,
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		1<<47 - 1,
		-1 << 47,
		1<<23 - 1,
		-1 << 23,
		1<<55 - 1,
		-1 << 55,
		1,
		-1,
	}

	for _, t := range tblInt64 {
		b := EncodeInt(nil, t)
		_, v, err := DecodeInt(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeIntDesc(nil, t)
		_, v, err = DecodeIntDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeVarint(nil, t)
		_, v, err = DecodeVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableVarint(nil, t)
		_, v, err = DecodeComparableVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblUint64 := []uint64{
		0,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		math.MaxUint64,
		1<<24 - 1,
		1<<48 - 1,
		1<<56 - 1,
		1,
		math.MaxInt16,
		math.MaxInt8,
		math.MaxInt32,
		math.MaxInt64,
	}

	for _, t := range tblUint64 {
		b := EncodeUint(nil, t)
		_, v, err := DecodeUint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUintDesc(nil, t)
		_, v, err = DecodeUintDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUvarint(nil, t)
		_, v, err = DecodeUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableUvarint(nil, t)
		_, v, err = DecodeComparableUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}
	var b []byte
	b = EncodeComparableVarint(b, -1)
	b = EncodeComparableUvarint(b, 1)
	b = EncodeComparableVarint(b, 2)
	b, i, err := DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(-1))
	b, u, err := DecodeComparableUvarint(b)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, uint64(1))
	_, i, err = DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(2))
}

func (s *testCodecSuite) TestNumberOrder(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []struct {
		Arg1 int64
		Arg2 int64
		Ret  int
	}{
		{-1, 1, -1},
		{math.MaxInt64, math.MinInt64, 1},
		{math.MaxInt64, math.MaxInt32, 1},
		{math.MinInt32, math.MaxInt16, -1},
		{math.MinInt64, math.MaxInt8, -1},
		{0, math.MaxInt8, -1},
		{math.MinInt8, 0, -1},
		{math.MinInt16, math.MaxInt16, -1},
		{1, -1, 1},
		{1, 0, 1},
		{-1, 0, -1},
		{0, 0, 0},
		{math.MaxInt16, math.MaxInt16, 0},
	}

	for _, t := range tblInt64 {
		b1 := EncodeInt(nil, t.Arg1)
		b2 := EncodeInt(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeIntDesc(nil, t.Arg1)
		b2 = EncodeIntDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableVarint(nil, t.Arg1)
		b2 = EncodeComparableVarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}

	tblUint64 := []struct {
		Arg1 uint64
		Arg2 uint64
		Ret  int
	}{
		{0, 0, 0},
		{1, 0, 1},
		{0, 1, -1},
		{math.MaxInt8, math.MaxInt16, -1},
		{math.MaxUint32, math.MaxInt32, 1},
		{math.MaxUint8, math.MaxInt8, 1},
		{math.MaxUint16, math.MaxInt32, -1},
		{math.MaxUint64, math.MaxInt64, 1},
		{math.MaxInt64, math.MaxUint32, 1},
		{math.MaxUint64, 0, 1},
		{0, math.MaxUint64, -1},
	}

	for _, t := range tblUint64 {
		b1 := EncodeUint(nil, t.Arg1)
		b2 := EncodeUint(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeUintDesc(nil, t.Arg1)
		b2 = EncodeUintDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableUvarint(nil, t.Arg1)
		b2 = EncodeComparableUvarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestFloatCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblFloat := []float64{
		-1,
		0,
		1,
		math.MaxFloat64,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		math.Inf(-1),
		math.Inf(1),
	}

	for _, t := range tblFloat {
		b := EncodeFloat(nil, t)
		_, v, err := DecodeFloat(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeFloatDesc(nil, t)
		_, v, err = DecodeFloatDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblCmp := []struct {
		Arg1 float64
		Arg2 float64
		Ret  int
	}{
		{1, -1, 1},
		{1, 0, 1},
		{0, -1, 1},
		{0, 0, 0},
		{math.MaxFloat64, 1, 1},
		{math.MaxFloat32, math.MaxFloat64, -1},
		{math.MaxFloat64, 0, 1},
		{math.MaxFloat64, math.SmallestNonzeroFloat64, 1},
		{math.Inf(-1), 0, -1},
		{math.Inf(1), 0, 1},
		{math.Inf(-1), math.Inf(1), -1},
	}

	for _, t := range tblCmp {
		b1 := EncodeFloat(nil, t.Arg1)
		b2 := EncodeFloat(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeFloatDesc(nil, t.Arg1)
		b2 = EncodeFloatDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}

func (s *testCodecSuite) TestBytes(c *C) {
	defer testleak.AfterTest(c)()
	tblBytes := [][]byte{
		{},
		{0x00, 0x01},
		{0xff, 0xff},
		{0x01, 0x00},
		[]byte("abc"),
		[]byte("hello world"),
	}

	for _, t := range tblBytes {
		b := EncodeBytes(nil, t)
		_, v, err := DecodeBytes(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeBytesDesc(nil, t)
		_, v, err = DecodeBytesDesc(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeCompactBytes(nil, t)
		_, v, err = DecodeCompactBytes(b)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))
	}

	tblCmp := []struct {
		Arg1 []byte
		Arg2 []byte
		Ret  int
	}{
		{[]byte{}, []byte{0x00}, -1},
		{[]byte{0x00}, []byte{0x00}, 0},
		{[]byte{0xFF}, []byte{0x00}, 1},
		{[]byte{0xFF}, []byte{0xFF, 0x00}, -1},
		{[]byte("a"), []byte("b"), -1},
		{[]byte("a"), []byte{0x00}, 1},
		{[]byte{0x00}, []byte{0x01}, -1},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x00}, []byte{0x01, 0x02, 0x03}, 1},
		{[]byte{0x01, 0x03, 0x03, 0x04}, []byte{0x01, 0x03, 0x03, 0x05}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
	}

	for _, t := range tblCmp {
		b1 := EncodeBytes(nil, t.Arg1)
		b2 := EncodeBytes(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeBytesDesc(nil, t.Arg1)
		b2 = EncodeBytesDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}

func (s *testCodecSuite) TestCut(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},
		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123456789012345")),
			types.MakeDatums(float64(1), float64(3.15), []byte("123456789012345")),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeKey(sc, nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeValue(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeValue(sc, nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}

	b, err := EncodeValue(sc, nil, types.NewDatum(42))
	c.Assert(err, IsNil)
	rem, n, err := CutColumnID(b)
	c.Assert(err, IsNil)
	c.Assert(rem, HasLen, 0)
	c.Assert(n, Equals, int64(42))
}

func (s *testCodecSuite) TestSetRawValues(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(sc, nil, datums...)
	c.Assert(err, IsNil)
	values := make([]types.Datum, 4)
	err = SetRawValues(rowData, values)
	c.Assert(err, IsNil)
	for i, rawVal := range values {
		c.Assert(rawVal.Kind(), Equals, types.KindRaw)
		encoded, err1 := EncodeValue(sc, nil, datums[i])
		c.Assert(err1, IsNil)
		c.Assert(encoded, BytesEquals, rawVal.GetBytes())
	}
}

func (s *testCodecSuite) TestDecodeOneToChunk(c *C) {
	defer testleak.AfterTest(c)()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums, tps := datumsForTest(sc)
	rowCount := 3
	chk := chunkForTest(c, sc, datums, tps, rowCount)
	for colIdx, tp := range tps {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			got := chk.GetRow(rowIdx).GetDatum(colIdx, tp)
			expect := datums[colIdx]
			if got.IsNull() {
				c.Assert(expect.IsNull(), IsTrue)
			} else {
				cmp, err := got.CompareDatum(sc, &expect)
				c.Assert(err, IsNil)
				c.Assert(cmp, Equals, 0)
			}
		}
	}
}

func datumsForTest(sc *stmtctx.StatementContext) ([]types.Datum, []*types.FieldType) {
	table := []struct {
		value interface{}
		tp    *types.FieldType
	}{
		{nil, types.NewFieldType(mysql.TypeLonglong)},
		{int64(1), types.NewFieldType(mysql.TypeTiny)},
		{int64(1), types.NewFieldType(mysql.TypeShort)},
		{int64(1), types.NewFieldType(mysql.TypeInt24)},
		{int64(1), types.NewFieldType(mysql.TypeLong)},
		{int64(-1), types.NewFieldType(mysql.TypeLong)},
		{int64(1), types.NewFieldType(mysql.TypeLonglong)},
		{uint64(1), types.NewFieldType(mysql.TypeLonglong)},
		{float32(1), types.NewFieldType(mysql.TypeFloat)},
		{float64(1), types.NewFieldType(mysql.TypeDouble)},
		{"abc", types.NewFieldType(mysql.TypeString)},
		{"def", types.NewFieldType(mysql.TypeVarchar)},
		{"ghi", types.NewFieldType(mysql.TypeVarString)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeTinyBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeMediumBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeLongBlob)},
		{int64(1), types.NewFieldType(mysql.TypeYear)},
	}

	datums := make([]types.Datum, 0, len(table)+2)
	tps := make([]*types.FieldType, 0, len(table)+2)
	for _, t := range table {
		tps = append(tps, t.tp)
		datums = append(datums, types.NewDatum(t.value))
	}
	return datums, tps
}

func chunkForTest(c *C, sc *stmtctx.StatementContext, datums []types.Datum, tps []*types.FieldType, rowCount int) *chunk.Chunk {
	decoder := NewDecoder(chunk.New(tps, 32, 32), sc.TimeZone)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		encoded, err := EncodeValue(sc, nil, datums...)
		c.Assert(err, IsNil)
		decoder.buf = make([]byte, 0, len(encoded))
		for colIdx, tp := range tps {
			encoded, err = decoder.DecodeOne(encoded, colIdx, tp)
			c.Assert(err, IsNil)
		}
	}
	return decoder.chk
}

func (s *testCodecSuite) TestDecodeRange(c *C) {
	_, _, err := DecodeRange(nil, 0)
	c.Assert(err, NotNil)

	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(nil, nil, datums...)
	c.Assert(err, IsNil)

	datums1, _, err := DecodeRange(rowData, len(datums))
	c.Assert(err, IsNil)
	for i, datum := range datums1 {
		cmp, err := datum.CompareDatum(nil, &datums[i])
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}

	for _, b := range []byte{NilFlag, bytesFlag, maxFlag, maxFlag + 1} {
		newData := append(rowData, b)
		_, _, err := DecodeRange(newData, len(datums)+1)
		c.Assert(err, IsNil)
	}
}

func testHashChunkRowEqual(c *C, a, b interface{}, equal bool) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf1 := make([]byte, 1)
	buf2 := make([]byte, 1)

	tp1 := new(types.FieldType)
	types.DefaultTypeForValue(a, tp1)
	chk1 := chunk.New([]*types.FieldType{tp1}, 1, 1)
	d := types.Datum{}
	d.SetValue(a)
	chk1.AppendDatum(0, &d)

	tp2 := new(types.FieldType)
	types.DefaultTypeForValue(b, tp2)
	chk2 := chunk.New([]*types.FieldType{tp2}, 1, 1)
	d = types.Datum{}
	d.SetValue(b)
	chk2.AppendDatum(0, &d)

	h := crc32.NewIEEE()
	err1 := HashChunkRow(sc, h, chk1.GetRow(0), []*types.FieldType{tp1}, []int{0}, buf1)
	sum1 := h.Sum32()
	h.Reset()
	err2 := HashChunkRow(sc, h, chk2.GetRow(0), []*types.FieldType{tp2}, []int{0}, buf2)
	sum2 := h.Sum32()
	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	if equal {
		c.Assert(sum1, Equals, sum2)
	} else {
		c.Assert(sum1, Not(Equals), sum2)
	}
	e, err := EqualChunkRow(sc,
		chk1.GetRow(0), []*types.FieldType{tp1}, []int{0},
		chk2.GetRow(0), []*types.FieldType{tp2}, []int{0})
	c.Assert(err, IsNil)
	if equal {
		c.Assert(e, IsTrue)
	} else {
		c.Assert(e, IsFalse)
	}
}

func (s *testCodecSuite) TestHashChunkRow(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf := make([]byte, 1)
	datums, tps := datumsForTest(sc)
	chk := chunkForTest(c, sc, datums, tps, 1)

	colIdx := make([]int, len(tps))
	for i := 0; i < len(tps); i++ {
		colIdx[i] = i
	}
	h := crc32.NewIEEE()
	err1 := HashChunkRow(sc, h, chk.GetRow(0), tps, colIdx, buf)
	sum1 := h.Sum32()
	h.Reset()
	err2 := HashChunkRow(sc, h, chk.GetRow(0), tps, colIdx, buf)
	sum2 := h.Sum32()

	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	c.Assert(sum1, Equals, sum2)
	e, err := EqualChunkRow(sc,
		chk.GetRow(0), tps, colIdx,
		chk.GetRow(0), tps, colIdx)
	c.Assert(err, IsNil)
	c.Assert(e, IsTrue)

	testHashChunkRowEqual(c, uint64(1), int64(1), true)
	testHashChunkRowEqual(c, uint64(18446744073709551615), int64(-1), false)

	testHashChunkRowEqual(c, float32(1.0), float64(1.0), true)
	testHashChunkRowEqual(c, float32(1.0), float64(1.1), false)

	testHashChunkRowEqual(c, "x", []byte("x"), true)
	testHashChunkRowEqual(c, "x", []byte("y"), false)
}

func (s *testCodecSuite) TestValueSizeOfSignedInt(c *C) {
	testCase := []int64{64, 8192, 1048576, 134217728, 17179869184, 2199023255552, 281474976710656, 36028797018963968, 4611686018427387904}
	var b []byte
	for _, v := range testCase {
		b := encodeSignedInt(b[:0], v-10, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(v-10))

		b = encodeSignedInt(b[:0], v, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(v))

		b = encodeSignedInt(b[:0], v+10, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(v+10))

		// Test for negative value.
		b = encodeSignedInt(b[:0], 0-v, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(0-v))

		b = encodeSignedInt(b[:0], 0-v+10, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(0-v+10))

		b = encodeSignedInt(b[:0], 0-v-10, false)
		c.Assert(len(b), Equals, valueSizeOfSignedInt(0-v-10))
	}
}

func (s *testCodecSuite) TestValueSizeOfUnsignedInt(c *C) {
	testCase := []uint64{128, 16384, 2097152, 268435456, 34359738368, 4398046511104, 562949953421312, 72057594037927936, 9223372036854775808}
	var b []byte
	for _, v := range testCase {
		b := encodeUnsignedInt(b[:0], v-10, false)
		c.Assert(len(b), Equals, valueSizeOfUnsignedInt(v-10))

		b = encodeUnsignedInt(b[:0], v, false)
		c.Assert(len(b), Equals, valueSizeOfUnsignedInt(v))

		b = encodeUnsignedInt(b[:0], v+10, false)
		c.Assert(len(b), Equals, valueSizeOfUnsignedInt(v+10))
	}
}

func (s *testCodecSuite) TestHashChunkColumns(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf := make([]byte, 1)
	datums, tps := datumsForTest(sc)
	chk := chunkForTest(c, sc, datums, tps, 3)

	colIdx := make([]int, len(tps))
	for i := 0; i < len(tps); i++ {
		colIdx[i] = i
	}
	hasNull := []bool{false, false, false}
	vecHash := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
	rowHash := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

	// Test hash value of the first `Null` column
	c.Assert(chk.GetRow(0).IsNull(0), Equals, true)
	err1 := HashChunkColumns(sc, vecHash, chk, tps[0], 0, buf, hasNull)
	err2 := HashChunkRow(sc, rowHash[0], chk.GetRow(0), tps, colIdx[0:1], buf)
	err3 := HashChunkRow(sc, rowHash[1], chk.GetRow(1), tps, colIdx[0:1], buf)
	err4 := HashChunkRow(sc, rowHash[2], chk.GetRow(2), tps, colIdx[0:1], buf)
	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	c.Assert(err3, IsNil)
	c.Assert(err4, IsNil)

	c.Assert(hasNull[0], Equals, true)
	c.Assert(hasNull[1], Equals, true)
	c.Assert(hasNull[2], Equals, true)
	c.Assert(vecHash[0].Sum64(), Equals, rowHash[0].Sum64())
	c.Assert(vecHash[1].Sum64(), Equals, rowHash[1].Sum64())
	c.Assert(vecHash[2].Sum64(), Equals, rowHash[2].Sum64())

	// Test hash value of every single column that is not `Null`
	for i := 1; i < len(tps); i++ {
		hasNull = []bool{false, false, false}
		vecHash = []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
		rowHash = []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

		c.Assert(chk.GetRow(0).IsNull(i), Equals, false)
		err1 = HashChunkColumns(sc, vecHash, chk, tps[i], i, buf, hasNull)
		err2 = HashChunkRow(sc, rowHash[0], chk.GetRow(0), tps, colIdx[i:i+1], buf)
		err3 = HashChunkRow(sc, rowHash[1], chk.GetRow(1), tps, colIdx[i:i+1], buf)
		err4 = HashChunkRow(sc, rowHash[2], chk.GetRow(2), tps, colIdx[i:i+1], buf)
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		c.Assert(err3, IsNil)
		c.Assert(err4, IsNil)

		c.Assert(hasNull[0], Equals, false)
		c.Assert(hasNull[1], Equals, false)
		c.Assert(hasNull[2], Equals, false)
		c.Assert(vecHash[0].Sum64(), Equals, rowHash[0].Sum64())
		c.Assert(vecHash[1].Sum64(), Equals, rowHash[1].Sum64())
		c.Assert(vecHash[2].Sum64(), Equals, rowHash[2].Sum64())
	}
}
