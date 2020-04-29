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

package types

import (
	"fmt"
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeConvertSuite{})

type testTypeConvertSuite struct {
}

type invalidMockType struct {
}

// Convert converts the val with type tp.
func Convert(val interface{}, target *FieldType) (v interface{}, err error) {
	d := NewDatum(val)
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	ret, err := d.ConvertTo(sc, target)
	if err != nil {
		return ret.GetValue(), errors.Trace(err)
	}
	return ret.GetValue(), nil
}

func (s *testTypeConvertSuite) TestConvertType(c *C) {
	defer testleak.AfterTest(c)()
	ft := NewFieldType(mysql.TypeBlob)
	ft.Flen = 4
	ft.Charset = "utf8"
	v, err := Convert("123456", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "1234")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 4
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, DeepEquals, []byte("1234"))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(111.114, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(111.11))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.999, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(-999.999, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(-999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(1111.11, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.916, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.914, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.91))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	// For TypeBlob
	ft = NewFieldType(mysql.TypeBlob)
	_, err = Convert(&invalidMockType{}, ft)
	c.Assert(err, NotNil)

	// Nil
	ft = NewFieldType(mysql.TypeBlob)
	v, err = Convert(nil, ft)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	// TypeDouble
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float64(999.92))

	// For TypeString
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "123")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, DeepEquals, []byte("123"))

	// For TypeLonglong
	ft = NewFieldType(mysql.TypeLonglong)
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(100))
	// issue 4287.
	v, err = Convert(math.Pow(2, 63)-1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(math.MaxInt64))
	ft = NewFieldType(mysql.TypeLonglong)
	ft.Flag |= mysql.UnsignedFlag
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(100))
}

func testStrToInt(c *C, str string, expect int64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToInt(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToUint(c *C, str string, expect uint64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToUint(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToFloat(c *C, str string, expect float64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToFloat(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func (s *testTypeConvertSuite) TestStrToNum(c *C) {
	defer testleak.AfterTest(c)()
	testStrToInt(c, "0", 0, true, nil)
	testStrToInt(c, "-1", -1, true, nil)
	testStrToInt(c, "100", 100, true, nil)
	testStrToInt(c, "65.0", 65, false, nil)
	testStrToInt(c, "65.0", 65, true, nil)
	testStrToInt(c, "", 0, false, nil)
	testStrToInt(c, "", 0, true, ErrTruncatedWrongVal)
	testStrToInt(c, "xx", 0, true, ErrTruncatedWrongVal)
	testStrToInt(c, "xx", 0, false, nil)
	testStrToInt(c, "11xx", 11, true, ErrTruncatedWrongVal)
	testStrToInt(c, "11xx", 11, false, nil)
	testStrToInt(c, "xx11", 0, false, nil)

	testStrToUint(c, "0", 0, true, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "-1", 0xffffffffffffffff, false, ErrOverflow)
	testStrToUint(c, "100", 100, true, nil)
	testStrToUint(c, "+100", 100, true, nil)
	testStrToUint(c, "65.0", 65, true, nil)
	testStrToUint(c, "xx", 0, true, ErrTruncatedWrongVal)
	testStrToUint(c, "11xx", 11, true, ErrTruncatedWrongVal)
	testStrToUint(c, "xx11", 0, true, ErrTruncatedWrongVal)

	// TODO: makes StrToFloat return truncated value instead of zero to make it pass.
	testStrToFloat(c, "", 0, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "-1", -1.0, true, nil)
	testStrToFloat(c, "1.11", 1.11, true, nil)
	testStrToFloat(c, "1.11.00", 1.11, false, nil)
	testStrToFloat(c, "1.11.00", 1.11, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "xx", 0.0, false, nil)
	testStrToFloat(c, "0x00", 0.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "xx.11", 0.0, false, nil)

	// for issue #5111
	testStrToFloat(c, "1e649", math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "1e649", math.MaxFloat64, false, nil)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, false, nil)

	// for issue #10806, #11179
	testSelectUpdateDeleteEmptyStringError(c)
}

func testSelectUpdateDeleteEmptyStringError(c *C) {
	testCases := []struct {
		inSelect bool
		inDelete bool
	}{
		{true, false},
		{false, true},
	}
	sc := new(stmtctx.StatementContext)
	for _, tc := range testCases {
		sc.InSelectStmt = tc.inSelect
		sc.InDeleteStmt = tc.inDelete

		str := ""
		expect := 0

		val, err := StrToInt(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(expect))

		val1, err := StrToUint(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val1, Equals, uint64(expect))

		val2, err := StrToFloat(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(expect))
	}
}

func (s *testTypeConvertSuite) TestFieldTypeToStr(c *C) {
	defer testleak.AfterTest(c)()
	v := TypeToStr(mysql.TypeUnspecified, "not binary")
	c.Assert(v, Equals, TypeStr(mysql.TypeUnspecified))
	v = TypeToStr(mysql.TypeBlob, charset.CharsetBin)
	c.Assert(v, Equals, "blob")
	v = TypeToStr(mysql.TypeString, charset.CharsetBin)
	c.Assert(v, Equals, "binary")
}

func accept(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	ft := NewFieldType(tp)
	if unsigned {
		ft.Flag |= mysql.UnsignedFlag
	}
	d := NewDatum(value)
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	sc.IgnoreTruncate = true
	casted, err := d.ConvertTo(sc, ft)
	c.Assert(err, IsNil, Commentf("%v", ft))
	if casted.IsNull() {
		c.Assert(expected, Equals, "<nil>")
	} else {
		str, err := casted.ToString()
		c.Assert(err, IsNil)
		c.Assert(str, Equals, expected)
	}
}

func unsignedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, true, expected)
}

func signedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, false, expected)
}

func deny(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	ft := NewFieldType(tp)
	if unsigned {
		ft.Flag |= mysql.UnsignedFlag
	}
	d := NewDatum(value)
	sc := new(stmtctx.StatementContext)
	casted, err := d.ConvertTo(sc, ft)
	c.Assert(err, NotNil)
	if casted.IsNull() {
		c.Assert(expected, Equals, "<nil>")
	} else {
		str, err := casted.ToString()
		c.Assert(err, IsNil)
		c.Assert(str, Equals, expected)
	}
}

func unsignedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, true, expected)
}

func signedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, false, expected)
}

func strvalue(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func (s *testTypeConvertSuite) TestConvert(c *C) {
	defer testleak.AfterTest(c)()
	// integer ranges
	signedDeny(c, mysql.TypeTiny, -129, "-128")
	signedAccept(c, mysql.TypeTiny, -128, "-128")
	signedAccept(c, mysql.TypeTiny, 127, "127")
	signedDeny(c, mysql.TypeTiny, 128, "127")
	unsignedDeny(c, mysql.TypeTiny, -1, "255")
	unsignedAccept(c, mysql.TypeTiny, 0, "0")
	unsignedAccept(c, mysql.TypeTiny, 255, "255")
	unsignedDeny(c, mysql.TypeTiny, 256, "255")

	signedDeny(c, mysql.TypeShort, int64(math.MinInt16)-1, strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MinInt16), strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MaxInt16), strvalue(int64(math.MaxInt16)))
	signedDeny(c, mysql.TypeShort, int64(math.MaxInt16)+1, strvalue(int64(math.MaxInt16)))
	unsignedDeny(c, mysql.TypeShort, -1, "65535")
	unsignedAccept(c, mysql.TypeShort, 0, "0")
	unsignedAccept(c, mysql.TypeShort, uint64(math.MaxUint16), strvalue(uint64(math.MaxUint16)))
	unsignedDeny(c, mysql.TypeShort, uint64(math.MaxUint16)+1, strvalue(uint64(math.MaxUint16)))

	signedDeny(c, mysql.TypeInt24, -1<<23-1, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, -1<<23, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, 1<<23-1, strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, 1<<23, strvalue(1<<23-1))
	unsignedDeny(c, mysql.TypeInt24, -1, "16777215")
	unsignedAccept(c, mysql.TypeInt24, 0, "0")
	unsignedAccept(c, mysql.TypeInt24, 1<<24-1, strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, 1<<24, strvalue(1<<24-1))

	signedDeny(c, mysql.TypeLong, int64(math.MinInt32)-1, strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MinInt32), strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MaxInt32), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, uint64(math.MaxUint64), strvalue(uint64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, int64(math.MaxInt32)+1, strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, "1343545435346432587475", strvalue(int64(math.MaxInt32)))
	unsignedDeny(c, mysql.TypeLong, -1, "4294967295")
	unsignedAccept(c, mysql.TypeLong, 0, "0")
	unsignedAccept(c, mysql.TypeLong, uint64(math.MaxUint32), strvalue(uint64(math.MaxUint32)))
	unsignedDeny(c, mysql.TypeLong, uint64(math.MaxUint32)+1, strvalue(uint64(math.MaxUint32)))

	signedDeny(c, mysql.TypeLonglong, math.MinInt64*1.1, strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MinInt64), strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MaxInt64), strvalue(int64(math.MaxInt64)))
	signedDeny(c, mysql.TypeLonglong, math.MaxInt64*1.1, strvalue(int64(math.MaxInt64)))
	unsignedAccept(c, mysql.TypeLonglong, -1, "18446744073709551615")
	unsignedAccept(c, mysql.TypeLonglong, 0, "0")
	unsignedAccept(c, mysql.TypeLonglong, uint64(math.MaxUint64), strvalue(uint64(math.MaxUint64)))
	unsignedDeny(c, mysql.TypeLonglong, math.MaxUint64*1.1, strvalue(uint64(math.MaxInt64)))

	// integer from string
	signedAccept(c, mysql.TypeLong, "	  234  ", "234")
	signedAccept(c, mysql.TypeLong, " 2.35e3  ", "2350")
	signedAccept(c, mysql.TypeLong, " 2.e3  ", "2000")
	signedAccept(c, mysql.TypeLong, " -2.e3  ", "-2000")
	signedAccept(c, mysql.TypeLong, " 2e2  ", "200")
	signedAccept(c, mysql.TypeLong, " 0.002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " .002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " 20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " -20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " +2.51 ", "3")
	signedAccept(c, mysql.TypeLong, " -9999.5 ", "-10000")
	signedAccept(c, mysql.TypeLong, " 999.4", "999")
	signedAccept(c, mysql.TypeLong, " -3.58", "-4")
	signedDeny(c, mysql.TypeLong, " 1a ", "1")
	signedDeny(c, mysql.TypeLong, " +1+ ", "1")

	// integer from float
	signedAccept(c, mysql.TypeLong, 234.5456, "235")
	signedAccept(c, mysql.TypeLong, -23.45, "-23")
	unsignedAccept(c, mysql.TypeLonglong, 234.5456, "235")
	unsignedDeny(c, mysql.TypeLonglong, -23.45, "18446744073709551593")

	// float from string
	signedAccept(c, mysql.TypeFloat, "23.523", "23.523")
	signedAccept(c, mysql.TypeFloat, int64(123), "123")
	signedAccept(c, mysql.TypeFloat, uint64(123), "123")
	signedAccept(c, mysql.TypeFloat, int(123), "123")
	signedAccept(c, mysql.TypeFloat, float32(123), "123")
	signedAccept(c, mysql.TypeFloat, float64(123), "123")
	signedAccept(c, mysql.TypeDouble, " -23.54", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54a", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54e2e", "-2354")
	signedDeny(c, mysql.TypeDouble, "+.e", "0")
	signedAccept(c, mysql.TypeDouble, "1e+1", "10")

	// string from string
	signedAccept(c, mysql.TypeString, "abc", "abc")
}

func (s *testTypeConvertSuite) TestRoundIntStr(c *C) {
	cases := []struct {
		a string
		b byte
		c string
	}{
		{"+999", '5', "+1000"},
		{"999", '5', "1000"},
		{"-999", '5', "-1000"},
	}
	for _, cc := range cases {
		c.Assert(roundIntStr(cc.b, cc.a), Equals, cc.c)
	}
}

func (s *testTypeConvertSuite) TestGetValidInt(c *C) {
	tests := []struct {
		origin  string
		valid   string
		signed  bool
		warning bool
	}{
		{"100", "100", true, false},
		{"-100", "-100", true, false},
		{"9223372036854775808", "9223372036854775808", false, false},
		{"1abc", "1", true, true},
		{"-1-1", "-1", true, true},
		{"+1+1", "+1", true, true},
		{"123..34", "123", true, true},
		{"123.23E-10", "123", true, true},
		{"1.1e1.3", "1", true, true},
		{"11e1.3", "11", true, true},
		{"1.", "1", true, true},
		{".1", "0", true, true},
		{"", "0", true, true},
		{"123e+", "123", true, true},
		{"123de", "123", true, true},
	}
	sc := new(stmtctx.StatementContext)
	sc.TruncateAsWarning = true
	sc.CastStrToIntStrict = true
	warningCount := 0
	for _, tt := range tests {
		prefix, err := getValidIntPrefix(sc, tt.origin)
		c.Assert(err, IsNil)
		c.Assert(prefix, Equals, tt.valid)
		if tt.signed {
			_, err = strconv.ParseInt(prefix, 10, 64)
		} else {
			_, err = strconv.ParseUint(prefix, 10, 64)
		}
		c.Assert(err, IsNil)
		warnings := sc.GetWarnings()
		if tt.warning {
			c.Assert(warnings, HasLen, warningCount+1)
			c.Assert(terror.ErrorEqual(warnings[len(warnings)-1].Err, ErrTruncatedWrongVal), IsTrue)
			warningCount += 1
		} else {
			c.Assert(warnings, HasLen, warningCount)
		}
	}

	tests2 := []struct {
		origin  string
		valid   string
		warning bool
	}{
		{"100", "100", false},
		{"-100", "-100", false},
		{"1abc", "1", true},
		{"-1-1", "-1", true},
		{"+1+1", "+1", true},
		{"123..34", "123.", true},
		{"123.23E-10", "0", false},
		{"1.1e1.3", "1.1e1", true},
		{"11e1.3", "11e1", true},
		{"1.", "1", false},
		{".1", "0", false},
		{"", "0", true},
		{"123e+", "123", true},
		{"123de", "123", true},
	}
	sc.TruncateAsWarning = false
	sc.CastStrToIntStrict = false
	for _, tt := range tests2 {
		prefix, err := getValidIntPrefix(sc, tt.origin)
		if tt.warning {
			c.Assert(terror.ErrorEqual(err, ErrTruncatedWrongVal), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(prefix, Equals, tt.valid)
	}
}

func (s *testTypeConvertSuite) TestGetValidFloat(c *C) {
	tests := []struct {
		origin string
		valid  string
	}{
		{"-100", "-100"},
		{"1abc", "1"},
		{"-1-1", "-1"},
		{"+1+1", "+1"},
		{"123..34", "123."},
		{"123.23E-10", "123.23E-10"},
		{"1.1e1.3", "1.1e1"},
		{"11e1.3", "11e1"},
		{"1.1e-13a", "1.1e-13"},
		{"1.", "1."},
		{".1", ".1"},
		{"", "0"},
		{"123e+", "123"},
		{"123.e", "123."},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		prefix, _ := getValidFloatPrefix(sc, tt.origin)
		c.Assert(prefix, Equals, tt.valid)
		_, err := strconv.ParseFloat(prefix, 64)
		c.Assert(err, IsNil)
	}

	tests2 := []struct {
		origin   string
		expected string
	}{
		{"1e9223372036854775807", "1"},
		{"125e342", "125"},
		{"1e21", "1"},
		{"1e5", "100000"},
		{"-123.45678e5", "-12345678"},
		{"+0.5", "1"},
		{"-0.5", "-1"},
		{".5e0", "1"},
		{"+.5e0", "+1"},
		{"-.5e0", "-1"},
		{".5", "1"},
		{"123.456789e5", "12345679"},
		{"123.456784e5", "12345678"},
		{"+999.9999e2", "+100000"},
	}
	for _, t := range tests2 {
		str, err := floatStrToIntStr(sc, t.origin, t.origin)
		c.Assert(err, IsNil)
		c.Assert(str, Equals, t.expected, Commentf("%v, %v", t.origin, t.expected))
	}
}

func (s *testTypeConvertSuite) TestConvertScientificNotation(c *C) {
	cases := []struct {
		input  string
		output string
		succ   bool
	}{
		{"123.456e0", "123.456", true},
		{"123.456e1", "1234.56", true},
		{"123.456e3", "123456", true},
		{"123.456e4", "1234560", true},
		{"123.456e5", "12345600", true},
		{"123.456e6", "123456000", true},
		{"123.456e7", "1234560000", true},
		{"123.456e-1", "12.3456", true},
		{"123.456e-2", "1.23456", true},
		{"123.456e-3", "0.123456", true},
		{"123.456e-4", "0.0123456", true},
		{"123.456e-5", "0.00123456", true},
		{"123.456e-6", "0.000123456", true},
		{"123.456e-7", "0.0000123456", true},
		{"123.456e-", "", false},
		{"123.456e-7.5", "", false},
		{"123.456e", "", false},
	}
	for _, ca := range cases {
		result, err := convertScientificNotation(ca.input)
		if !ca.succ {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(ca.output, Equals, result)
		}
	}
}
