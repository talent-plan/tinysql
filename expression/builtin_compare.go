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
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &compareFunctionClass{}
)

var (
	_ builtinFunc = &builtinLTIntSig{}
	_ builtinFunc = &builtinLTRealSig{}
	_ builtinFunc = &builtinLTDecimalSig{}
	_ builtinFunc = &builtinLTStringSig{}
	_ builtinFunc = &builtinLTDurationSig{}
	_ builtinFunc = &builtinLTTimeSig{}

	_ builtinFunc = &builtinLEIntSig{}
	_ builtinFunc = &builtinLERealSig{}
	_ builtinFunc = &builtinLEDecimalSig{}
	_ builtinFunc = &builtinLEStringSig{}
	_ builtinFunc = &builtinLEDurationSig{}
	_ builtinFunc = &builtinLETimeSig{}

	_ builtinFunc = &builtinGTIntSig{}
	_ builtinFunc = &builtinGTRealSig{}
	_ builtinFunc = &builtinGTDecimalSig{}
	_ builtinFunc = &builtinGTStringSig{}
	_ builtinFunc = &builtinGTTimeSig{}
	_ builtinFunc = &builtinGTDurationSig{}

	_ builtinFunc = &builtinGEIntSig{}
	_ builtinFunc = &builtinGERealSig{}
	_ builtinFunc = &builtinGEDecimalSig{}
	_ builtinFunc = &builtinGEStringSig{}
	_ builtinFunc = &builtinGETimeSig{}
	_ builtinFunc = &builtinGEDurationSig{}

	_ builtinFunc = &builtinNEIntSig{}
	_ builtinFunc = &builtinNERealSig{}
	_ builtinFunc = &builtinNEDecimalSig{}
	_ builtinFunc = &builtinNEStringSig{}
	_ builtinFunc = &builtinNETimeSig{}
	_ builtinFunc = &builtinNEDurationSig{}
)

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return types.ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
		return types.ETInt
	} else if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
		return types.ETDecimal
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.Tp == mysql.TypeJSON) ||
		(lhsFieldType.Tp == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.Tp) || types.IsTypeTime(rhsFieldType.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.Tp == rhsFieldType.Tp {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.Tp == mysql.TypeDuration && rhsFieldType.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(lhs) && isRHSConst ||
			isTemporalColumn(rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType().Tp == mysql.TypeDuration {
				cmpType = types.ETDuration
			} else {
				cmpType = types.ETDatetime
			}
		}
	}
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		return CompareString
	case types.ETDuration:
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		return CompareTime
	case types.ETJson:
		return CompareJSON
	}
	return nil
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func tryToConvertConstantInt(ctx sessionctx.Context, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType().EvalType() == types.ETInt {
		return con, false
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	dt, err = dt.ConvertTo(sc, targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   dt,
				RetType: targetFieldType,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:   dt,
		RetType: targetFieldType,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func RefineComparedConstant(ctx sessionctx.Context, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	if targetFieldType.Tp == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	intDatum, err = dt.ConvertTo(sc, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   intDatum,
				RetType: &targetFieldType,
			}, true
		}
		return con, false
	}
	c, err := intDatum.CompareDatum(sc, &con.Value)
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:   intDatum,
			RetType: &targetFieldType,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType().EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(sc, types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if c, err = doubleDatum.CompareDatum(sc, &intDatum); err != nil {
				return con, false
			}
			if c != 0 {
				return con, true
			}
			return &Constant{
				Value:   intDatum,
				RetType: &targetFieldType,
			}, false
		}
	}
	return con, false
}

// refineArgs will rewrite the arguments if the compare expression is `int column <cmp> non-int constant` or
// `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
func (c *compareFunctionClass) refineArgs(ctx sessionctx.Context, args []Expression) []Expression {
	arg0Type, arg1Type := args[0].GetType(), args[1].GetType()
	arg0IsInt := arg0Type.EvalType() == types.ETInt
	arg1IsInt := arg1Type.EvalType() == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isExceptional, finalArg0, finalArg1 := false, args[0], args[1]
	isPositiveInfinite, isNegativeInfinite := false, false
	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1, isExceptional = RefineComparedConstant(ctx, *arg0Type, arg1, c.op)
		finalArg1 = arg1
		if isExceptional && arg1.GetType().EvalType() == types.ETInt {
			// Judge it is inf or -inf
			// For int:
			//			inf:  01111111 & 1 == 1
			//		   -inf:  10000000 & 1 == 0
			// For uint:
			//			inf:  11111111 & 1 == 1
			//		   -inf:  00000000 & 0 == 0
			if arg1.Value.GetInt64()&1 == 1 {
				isPositiveInfinite = true
			} else {
				isNegativeInfinite = true
			}
		}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0, isExceptional = RefineComparedConstant(ctx, *arg1Type, arg0, symmetricOp[c.op])
		finalArg0 = arg0
		if isExceptional && arg0.GetType().EvalType() == types.ETInt {
			if arg0.Value.GetInt64()&1 == 1 {
				isNegativeInfinite = true
			} else {
				isPositiveInfinite = true
			}
		}
	}

	if isExceptional && (c.op == opcode.EQ || c.op == opcode.NullEQ) {
		// This will always be false.
		return []Expression{Zero.Clone(), One.Clone()}
	}
	if isPositiveInfinite {
		// If the op is opcode.LT, opcode.LE
		// This will always be true.
		// If the op is opcode.GT, opcode.GE
		// This will always be false.
		return []Expression{Zero.Clone(), One.Clone()}
	}
	if isNegativeInfinite {
		// If the op is opcode.GT, opcode.GE
		// This will always be true.
		// If the op is opcode.LT, opcode.LE
		// This will always be false.
		return []Expression{One.Clone(), Zero.Clone()}
	}

	return []Expression{finalArg0, finalArg1}
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx sessionctx.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, err
	}
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sessionctx.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, tp, tp)
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			DisableParseJSONFlag4Expr(args[i])
		}
	}
	bf.tp.Flen = 1
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			sig = &builtinLERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			sig = &builtinGERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			sig = &builtinNERealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEReal)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDecimal)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		}
	case types.ETDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			sig = &builtinLEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			sig = &builtinGTDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			sig = &builtinGEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			sig = &builtinEQDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			sig = &builtinNEDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			sig = &builtinLETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			sig = &builtinGTTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			sig = &builtinGETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			sig = &builtinEQTimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			sig = &builtinNETimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NETime)
		}
	case types.ETJson:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			sig = &builtinLEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			sig = &builtinGTJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			sig = &builtinGEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			sig = &builtinEQJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			sig = &builtinNEJSONSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEJson)
		}
	}
	return
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinLTRealSig) Clone() builtinFunc {
	newSig := &builtinLTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDecimalSig) Clone() builtinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLTDurationSig) Clone() builtinFunc {
	newSig := &builtinLTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLTTimeSig) Clone() builtinFunc {
	newSig := &builtinLTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLTJSONSig) Clone() builtinFunc {
	newSig := &builtinLTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLERealSig struct {
	baseBuiltinFunc
}

func (b *builtinLERealSig) Clone() builtinFunc {
	newSig := &builtinLERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDecimalSig) Clone() builtinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinLEDurationSig) Clone() builtinFunc {
	newSig := &builtinLEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinLETimeSig) Clone() builtinFunc {
	newSig := &builtinLETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinLEJSONSig) Clone() builtinFunc {
	newSig := &builtinLEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTRealSig struct {
	baseBuiltinFunc
}

func (b *builtinGTRealSig) Clone() builtinFunc {
	newSig := &builtinGTRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDecimalSig) Clone() builtinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGTDurationSig) Clone() builtinFunc {
	newSig := &builtinGTDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGTTimeSig) Clone() builtinFunc {
	newSig := &builtinGTTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGTJSONSig) Clone() builtinFunc {
	newSig := &builtinGTJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGERealSig struct {
	baseBuiltinFunc
}

func (b *builtinGERealSig) Clone() builtinFunc {
	newSig := &builtinGERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDecimalSig) Clone() builtinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinGEDurationSig) Clone() builtinFunc {
	newSig := &builtinGEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinGETimeSig) Clone() builtinFunc {
	newSig := &builtinGETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinGEJSONSig) Clone() builtinFunc {
	newSig := &builtinGEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinEQRealSig) Clone() builtinFunc {
	newSig := &builtinEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinEQDurationSig) Clone() builtinFunc {
	newSig := &builtinEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinEQTimeSig) Clone() builtinFunc {
	newSig := &builtinEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinEQJSONSig) Clone() builtinFunc {
	newSig := &builtinEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNERealSig struct {
	baseBuiltinFunc
}

func (b *builtinNERealSig) Clone() builtinFunc {
	newSig := &builtinNERealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareReal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDecimalSig) Clone() builtinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDecimal(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareString(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNEDurationSig) Clone() builtinFunc {
	newSig := &builtinNEDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDuration(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNETimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNETimeSig) Clone() builtinFunc {
	newSig := &builtinNETimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareTime(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNEJSONSig) Clone() builtinFunc {
	newSig := &builtinNEJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareJSON(b.ctx, b.args[0], b.args[1], row, row))
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().Flag), mysql.HasUnsignedFlag(rhsArg.GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

// CompareString compares two strings.
func CompareString(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1)), false, nil
}

// CompareReal compares two float-point values.
func CompareReal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}
