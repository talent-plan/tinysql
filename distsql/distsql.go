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

package distsql

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType) (SelectResult, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("distsql.Select", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*kv.Request))(kvReq)
	}

	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars)
	if resp == nil {
		err := errors.New("client returns nil response")
		return nil, err
	}

	return &selectResult{
		label:      "dag",
		resp:       resp,
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
	}, nil
}

// SelectWithRuntimeStats sends a DAG request, returns SelectResult.
// The difference from Select is that SelectWithRuntimeStats will set copPlanIDs into selectResult,
// which can help selectResult to collect runtime stats.
func SelectWithRuntimeStats(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, copPlanIDs []fmt.Stringer, rootPlanID fmt.Stringer) (SelectResult, error) {
	sr, err := Select(ctx, sctx, kvReq, fieldTypes)
	if err == nil {
		if selectResult, ok := sr.(*selectResult); ok {
			selectResult.copPlanIDs = copPlanIDs
			selectResult.rootPlanID = rootPlanID
		}
	}
	return sr, err
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables,
	isRestrict bool) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}

	if isRestrict {

	}
	result := &selectResult{
		label: "analyze",
		resp:  resp,
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label: "checksum",
		resp:  resp,
	}
	return result, nil
}
