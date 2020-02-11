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

package ast

import (
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/types"
)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncCastExpr{}
)

// List scalar function names.
const (
	IsNull      = "isnull"
	Length      = "length"
	Strcmp      = "strcmp"
	OctetLength = "octet_length"
	If          = "if"
	Ifnull      = "ifnull"
	LogicAnd    = "and"
	LogicOr     = "or"
	GE          = "ge"
	LE          = "le"
	EQ          = "eq"
	NE          = "ne"
	LT          = "lt"
	GT          = "gt"
	Plus        = "plus"
	Minus       = "minus"
	Div         = "div"
	Mul         = "mul"
	UnaryNot    = "not"
	UnaryMinus  = "unaryminus"
	In          = "in"
	RowFunc     = "row"
	SetVar      = "setvar"
	GetVar      = "getvar"
	Values      = "values"
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	// FnName is the function name.
	FnName model.CIStr
	// Args is the function args.
	Args []ExprNode
}

// Format the ExprNode into a Writer.
func (n *FuncCallExpr) Format(w io.Writer) {
	fmt.Fprintf(w, "%s(", n.FnName.L)
	for i, arg := range n.Args {
		arg.Format(w)
		if i != len(n.Args)-1 {
			fmt.Fprint(w, ", ")
		}
	}
	fmt.Fprint(w, ")")
}

// Accept implements Node interface.
func (n *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// CastFunctionType is the type for cast function.
type CastFunctionType int

// CastFunction types
const (
	CastFunction CastFunctionType = iota + 1
	CastConvertFunction
	CastBinaryOperator
)

// FuncCastExpr is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
type FuncCastExpr struct {
	funcNode
	// Expr is the expression to be converted.
	Expr ExprNode
	// Tp is the conversion type.
	Tp *types.FieldType
	// FunctionType is either Cast, Convert or Binary.
	FunctionType CastFunctionType
}

// Restore implements Node interface.
func (n *FuncCastExpr) Restore(ctx *RestoreCtx) error {
	switch n.FunctionType {
	case CastFunction:
		ctx.WriteKeyWord("CAST")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WriteKeyWord(" AS ")
		n.Tp.RestoreAsCastType(ctx)
		ctx.WritePlain(")")
	case CastConvertFunction:
		ctx.WriteKeyWord("CONVERT")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
		ctx.WritePlain(", ")
		n.Tp.RestoreAsCastType(ctx)
		ctx.WritePlain(")")
	case CastBinaryOperator:
		ctx.WriteKeyWord("BINARY ")
		if err := n.Expr.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore FuncCastExpr.Expr")
		}
	}
	return nil
}

// Format the ExprNode into a Writer.
func (n *FuncCastExpr) Format(w io.Writer) {
	switch n.FunctionType {
	case CastFunction:
		fmt.Fprint(w, "CAST(")
		n.Expr.Format(w)
		fmt.Fprint(w, " AS ")
		n.Tp.FormatAsCastType(w)
		fmt.Fprint(w, ")")
	case CastConvertFunction:
		fmt.Fprint(w, "CONVERT(")
		n.Expr.Format(w)
		fmt.Fprint(w, ", ")
		n.Tp.FormatAsCastType(w)
		fmt.Fprint(w, ")")
	case CastBinaryOperator:
		fmt.Fprint(w, "BINARY ")
		n.Expr.Format(w)
	}
}

// Accept implements Node Accept interface.
func (n *FuncCastExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCastExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// TrimDirectionType is the type for trim direction.
type TrimDirectionType int

const (
	// TrimBothDefault trims from both direction by default.
	TrimBothDefault TrimDirectionType = iota
	// TrimBoth trims from both direction with explicit notation.
	TrimBoth
	// TrimLeading trims from left.
	TrimLeading
	// TrimTrailing trims from right.
	TrimTrailing
)

// String implements fmt.Stringer interface.
func (direction TrimDirectionType) String() string {
	switch direction {
	case TrimBoth, TrimBothDefault:
		return "BOTH"
	case TrimLeading:
		return "LEADING"
	case TrimTrailing:
		return "TRAILING"
	default:
		return ""
	}
}

// TrimDirectionExpr is an expression representing the trim direction used in the TRIM() function.
type TrimDirectionExpr struct {
	exprNode
	// Direction is the trim direction
	Direction TrimDirectionType
}

// Restore implements Node interface.
func (n *TrimDirectionExpr) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(n.Direction.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *TrimDirectionExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Direction.String())
}

// Accept implements Node Accept interface.
func (n *TrimDirectionExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// DateArithType is type for DateArith type.
type DateArithType byte

const (
	// DateArithAdd is to run adddate or date_add function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateArithAdd DateArithType = iota + 1
	// DateArithSub is to run subdate or date_sub function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateArithSub
)

const (
	// AggFuncCount is the name of Count function.
	AggFuncCount = "count"
	// AggFuncSum is the name of Sum function.
	AggFuncSum = "sum"
	// AggFuncAvg is the name of Avg function.
	AggFuncAvg = "avg"
	// AggFuncFirstRow is the name of FirstRowColumn function.
	AggFuncFirstRow = "firstrow"
	// AggFuncMax is the name of max function.
	AggFuncMax = "max"
	// AggFuncMin is the name of min function.
	AggFuncMin = "min"
	// AggFuncGroupConcat is the name of group_concat function.
	AggFuncGroupConcat = "group_concat"
	// AggFuncBitOr is the name of bit_or function.
	AggFuncBitOr = "bit_or"
	// AggFuncBitXor is the name of bit_xor function.
	AggFuncBitXor = "bit_xor"
	// AggFuncBitAnd is the name of bit_and function.
	AggFuncBitAnd = "bit_and"
	// AggFuncVarPop is the name of var_pop function
	AggFuncVarPop = "var_pop"
	// AggFuncVarSamp is the name of var_samp function
	AggFuncVarSamp = "var_samp"
	// AggFuncStddevPop is the name of stddev_pop function
	AggFuncStddevPop = "stddev_pop"
	// AggFuncStddevSamp is the name of stddev_samp function
	AggFuncStddevSamp = "stddev_samp"
)

// AggregateFuncExpr represents aggregate function expression.
type AggregateFuncExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// Distinct is true, function hence only aggregate distinct values.
	// For example, column c1 values are "1", "2", "2",  "sum(c1)" is "5",
	// but "sum(distinct c1)" is "3".
	Distinct bool
}

// Restore implements Node interface.
func (n *AggregateFuncExpr) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(n.F)
	ctx.WritePlain("(")
	if n.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	}
	switch strings.ToLower(n.F) {
	case "group_concat":
		for i := 0; i < len(n.Args)-1; i++ {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := n.Args[i].Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
		ctx.WriteKeyWord(" SEPARATOR ")
		if err := n.Args[len(n.Args)-1].Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AggregateFuncExpr.Args SEPARATOR")
		}
	default:
		for i, argv := range n.Args {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := argv.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore AggregateFuncExpr.Args[%d]", i)
			}
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Format the ExprNode into a Writer.
func (n *AggregateFuncExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AggregateFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// TimeUnitType is the type for time and timestamp units.
type TimeUnitType int

const (
	// TimeUnitInvalid is a placeholder for an invalid time or timestamp unit
	TimeUnitInvalid TimeUnitType = iota
	// TimeUnitMicrosecond is the time or timestamp unit MICROSECOND.
	TimeUnitMicrosecond
	// TimeUnitSecond is the time or timestamp unit SECOND.
	TimeUnitSecond
	// TimeUnitMinute is the time or timestamp unit MINUTE.
	TimeUnitMinute
	// TimeUnitHour is the time or timestamp unit HOUR.
	TimeUnitHour
	// TimeUnitDay is the time or timestamp unit DAY.
	TimeUnitDay
	// TimeUnitWeek is the time or timestamp unit WEEK.
	TimeUnitWeek
	// TimeUnitMonth is the time or timestamp unit MONTH.
	TimeUnitMonth
	// TimeUnitQuarter is the time or timestamp unit QUARTER.
	TimeUnitQuarter
	// TimeUnitYear is the time or timestamp unit YEAR.
	TimeUnitYear
	// TimeUnitSecondMicrosecond is the time unit SECOND_MICROSECOND.
	TimeUnitSecondMicrosecond
	// TimeUnitMinuteMicrosecond is the time unit MINUTE_MICROSECOND.
	TimeUnitMinuteMicrosecond
	// TimeUnitMinuteSecond is the time unit MINUTE_SECOND.
	TimeUnitMinuteSecond
	// TimeUnitHourMicrosecond is the time unit HOUR_MICROSECOND.
	TimeUnitHourMicrosecond
	// TimeUnitHourSecond is the time unit HOUR_SECOND.
	TimeUnitHourSecond
	// TimeUnitHourMinute is the time unit HOUR_MINUTE.
	TimeUnitHourMinute
	// TimeUnitDayMicrosecond is the time unit DAY_MICROSECOND.
	TimeUnitDayMicrosecond
	// TimeUnitDaySecond is the time unit DAY_SECOND.
	TimeUnitDaySecond
	// TimeUnitDayMinute is the time unit DAY_MINUTE.
	TimeUnitDayMinute
	// TimeUnitDayHour is the time unit DAY_HOUR.
	TimeUnitDayHour
	// TimeUnitYearMonth is the time unit YEAR_MONTH.
	TimeUnitYearMonth
)

// String implements fmt.Stringer interface.
func (unit TimeUnitType) String() string {
	switch unit {
	case TimeUnitMicrosecond:
		return "MICROSECOND"
	case TimeUnitSecond:
		return "SECOND"
	case TimeUnitMinute:
		return "MINUTE"
	case TimeUnitHour:
		return "HOUR"
	case TimeUnitDay:
		return "DAY"
	case TimeUnitWeek:
		return "WEEK"
	case TimeUnitMonth:
		return "MONTH"
	case TimeUnitQuarter:
		return "QUARTER"
	case TimeUnitYear:
		return "YEAR"
	case TimeUnitSecondMicrosecond:
		return "SECOND_MICROSECOND"
	case TimeUnitMinuteMicrosecond:
		return "MINUTE_MICROSECOND"
	case TimeUnitMinuteSecond:
		return "MINUTE_SECOND"
	case TimeUnitHourMicrosecond:
		return "HOUR_MICROSECOND"
	case TimeUnitHourSecond:
		return "HOUR_SECOND"
	case TimeUnitHourMinute:
		return "HOUR_MINUTE"
	case TimeUnitDayMicrosecond:
		return "DAY_MICROSECOND"
	case TimeUnitDaySecond:
		return "DAY_SECOND"
	case TimeUnitDayMinute:
		return "DAY_MINUTE"
	case TimeUnitDayHour:
		return "DAY_HOUR"
	case TimeUnitYearMonth:
		return "YEAR_MONTH"
	default:
		return ""
	}
}

// TimeUnitExpr is an expression representing a time or timestamp unit.
type TimeUnitExpr struct {
	exprNode
	// Unit is the time or timestamp unit.
	Unit TimeUnitType
}

// Restore implements Node interface.
func (n *TimeUnitExpr) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(n.Unit.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *TimeUnitExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Unit.String())
}

// Accept implements Node Accept interface.
func (n *TimeUnitExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// GetFormatSelectorType is the type for the first argument of GET_FORMAT() function.
type GetFormatSelectorType int

const (
	// GetFormatSelectorDate is the GET_FORMAT selector DATE.
	GetFormatSelectorDate GetFormatSelectorType = iota + 1
	// GetFormatSelectorTime is the GET_FORMAT selector TIME.
	GetFormatSelectorTime
	// GetFormatSelectorDatetime is the GET_FORMAT selector DATETIME and TIMESTAMP.
	GetFormatSelectorDatetime
)

// GetFormatSelectorExpr is an expression used as the first argument of GET_FORMAT() function.
type GetFormatSelectorExpr struct {
	exprNode
	// Selector is the GET_FORMAT() selector.
	Selector GetFormatSelectorType
}

// String implements fmt.Stringer interface.
func (selector GetFormatSelectorType) String() string {
	switch selector {
	case GetFormatSelectorDate:
		return "DATE"
	case GetFormatSelectorTime:
		return "TIME"
	case GetFormatSelectorDatetime:
		return "DATETIME"
	default:
		return ""
	}
}

// Restore implements Node interface.
func (n *GetFormatSelectorExpr) Restore(ctx *RestoreCtx) error {
	ctx.WriteKeyWord(n.Selector.String())
	return nil
}

// Format the ExprNode into a Writer.
func (n *GetFormatSelectorExpr) Format(w io.Writer) {
	fmt.Fprint(w, n.Selector.String())
}

// Accept implements Node Accept interface.
func (n *GetFormatSelectorExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}
