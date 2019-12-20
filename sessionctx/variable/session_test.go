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

package variable_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSessionSuite{})

type testSessionSuite struct {
}

func (*testSessionSuite) TestSetSystemVariable(c *C) {
	v := variable.NewSessionVars()
	v.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	v.TimeZone = time.UTC
	tests := []struct {
		key   string
		value interface{}
		err   bool
	}{
		{variable.TxnIsolation, "SERIALIZABLE", true},
		{variable.TimeZone, "xyz", true},
		{variable.TiDBOptAggPushDown, "1", false},
		{variable.TIDBMemQuotaQuery, "1024", false},
		{variable.TIDBMemQuotaHashJoin, "1024", false},
		{variable.TIDBMemQuotaMergeJoin, "1024", false},
		{variable.TIDBMemQuotaSort, "1024", false},
		{variable.TIDBMemQuotaTopn, "1024", false},
		{variable.TIDBMemQuotaIndexLookupReader, "1024", false},
		{variable.TIDBMemQuotaIndexLookupJoin, "1024", false},
		{variable.TIDBMemQuotaNestedLoopApply, "1024", false},
	}
	for _, t := range tests {
		err := variable.SetSessionSystemVar(v, t.key, types.NewDatum(t.value))
		if t.err {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (*testSessionSuite) TestSession(c *C) {
	ctx := mock.NewContext()

	ss := ctx.GetSessionVars().StmtCtx
	c.Assert(ss, NotNil)

	// For AffectedRows
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(1))
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(2))

	// For RecordRows
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(1))
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(2))

	// For FoundRows
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(1))
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(2))

	// For UpdatedRows
	ss.AddUpdatedRows(1)
	c.Assert(ss.UpdatedRows(), Equals, uint64(1))
	ss.AddUpdatedRows(1)
	c.Assert(ss.UpdatedRows(), Equals, uint64(2))

	// For TouchedRows
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(1))
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(2))

	// For CopiedRows
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(1))
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(2))

	// For last insert id
	ctx.GetSessionVars().SetLastInsertID(1)
	c.Assert(ctx.GetSessionVars().StmtCtx.LastInsertID, Equals, uint64(1))

	ss.ResetForRetry()
	c.Assert(ss.AffectedRows(), Equals, uint64(0))
	c.Assert(ss.FoundRows(), Equals, uint64(0))
	c.Assert(ss.UpdatedRows(), Equals, uint64(0))
	c.Assert(ss.RecordRows(), Equals, uint64(0))
	c.Assert(ss.TouchedRows(), Equals, uint64(0))
	c.Assert(ss.CopiedRows(), Equals, uint64(0))
	c.Assert(ss.WarningCount(), Equals, uint16(0))
}
