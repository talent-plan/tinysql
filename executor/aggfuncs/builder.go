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

package aggfuncs

import (
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// Build is used to build a specific AggFunc implementation according to the
// input aggFuncDesc.
func Build(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		return buildCount(aggFuncDesc, ordinal)
	case ast.AggFuncSum:
		return buildSum(aggFuncDesc, ordinal)
	case ast.AggFuncAvg:
		return buildAvg(aggFuncDesc, ordinal)
	case ast.AggFuncFirstRow:
		return buildFirstRow(aggFuncDesc, ordinal)
	case ast.AggFuncMax:
		return buildMaxMin(aggFuncDesc, ordinal, true)
	case ast.AggFuncMin:
		return buildMaxMin(aggFuncDesc, ordinal, false)
	}
	return nil
}

// buildCount builds the AggFunc implementation for function "COUNT".
func buildCount(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}

	switch aggFuncDesc.Mode {
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.Args[0].GetType().EvalType() {
		case types.ETInt:
			return &countOriginal4Int{baseCount{base}}
		case types.ETReal:
			return &countOriginal4Real{baseCount{base}}
		case types.ETString:
			return &countOriginal4String{baseCount{base}}
		}
	case aggregation.Partial2Mode, aggregation.FinalMode:
		return &countPartial{baseCount{base}}
	}

	return nil
}

// buildSum builds the AggFunc implementation for function "SUM".
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseSumAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.RetTp.EvalType() {
	case types.ETInt:
		return &sum4Int64{base}
	default:
		return &sum4Float64{base}
	}
}

// buildAvg builds the AggFunc implementation for function "AVG".
func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	switch aggFuncDesc.Mode {
	// Build avg functions which consume the original data and update their
	// partial results.
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.RetTp.EvalType() {
		case types.ETInt:
			return &avgOriginal4Int64{baseAvgInt64{base}}
		default:
			return &avgOriginal4Float64{baseAvgFloat64{base}}
		}

	// Build avg functions which consume the partial result of other avg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.RetTp.Tp {
		case mysql.TypeLonglong:
			return &avgPartial4Int64{baseAvgInt64{base}}
		case mysql.TypeDouble:
			return &avgPartial4Float64{baseAvgFloat64{base}}
		}
	}
	return nil
}

// buildFirstRow builds the AggFunc implementation for function "FIRST_ROW".
func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		evalType = types.ETString
	}
	switch evalType {
	case types.ETInt:
		return &firstRow4Int{base}
	case types.ETReal:
		switch fieldType.Tp {
		case mysql.TypeFloat:
			return &firstRow4Float32{base}
		case mysql.TypeDouble:
			return &firstRow4Float64{base}
		}
	case types.ETString:
		return &firstRow4String{base}
	}
	return nil
}

// buildMaxMin builds the AggFunc implementation for function "MAX" and "MIN".
func buildMaxMin(aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	base := baseMaxMinAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
		isMax: isMax,
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		evalType = types.ETString
	}
	switch evalType {
	case types.ETInt:
		if mysql.HasUnsignedFlag(fieldType.Flag) {
			return &maxMin4Uint{base}
		}
		return &maxMin4Int{base}
	case types.ETReal:
		switch fieldType.Tp {
		case mysql.TypeFloat:
			return &maxMin4Float32{base}
		case mysql.TypeDouble:
			return &maxMin4Float64{base}
		}
	case types.ETString:
		return &maxMin4String{base}
	}
	return nil
}
