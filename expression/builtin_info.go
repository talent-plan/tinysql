// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	// MySQL client need connection_id() function to establish a connection.
	_ functionClass = &connectionIDFunctionClass{}
)

var (
	_ builtinFunc = &builtinConnectionIDSig{}
)

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinConnectionIDSig{bf}
	return sig, nil
}

type builtinConnectionIDSig struct {
	baseBuiltinFunc
}

func (b *builtinConnectionIDSig) Clone() builtinFunc {
	newSig := &builtinConnectionIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinConnectionIDSig) evalInt(_ chunk.Row) (int64, bool, error) {
	data := b.ctx.GetSessionVars()
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable `builtinConnectionIDSig.evalInt`")
	}
	return int64(data.ConnectionID), false, nil
}
