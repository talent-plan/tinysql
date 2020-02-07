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

package variable

import (
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testVarsutilSuite{})

type testVarsutilSuite struct {
}

func (s *testVarsutilSuite) TestTiDBOptOn(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		val string
		on  bool
	}{
		{"ON", true},
		{"on", true},
		{"On", true},
		{"1", true},
		{"off", false},
		{"No", false},
		{"0", false},
		{"1.1", false},
		{"", false},
	}
	for _, t := range tbl {
		on := TiDBOptOn(t.val)
		c.Assert(on, Equals, t.on)
	}
}

func (s *testVarsutilSuite) TestNewSessionVars(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewSessionVars()

	c.Assert(vars.IndexLookupSize, Equals, DefIndexLookupSize)
	c.Assert(vars.IndexLookupConcurrency, Equals, DefIndexLookupConcurrency)
	c.Assert(vars.IndexSerialScanConcurrency, Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency, Equals, DefIndexLookupJoinConcurrency)
	c.Assert(vars.HashJoinConcurrency, Equals, DefTiDBHashJoinConcurrency)
	c.Assert(vars.ProjectionConcurrency, Equals, int64(DefTiDBProjectionConcurrency))
	c.Assert(vars.HashAggPartialConcurrency, Equals, DefTiDBHashAggPartialConcurrency)
	c.Assert(vars.HashAggFinalConcurrency, Equals, DefTiDBHashAggFinalConcurrency)
	c.Assert(vars.DistSQLScanConcurrency, Equals, DefDistSQLScanConcurrency)
	c.Assert(vars.MaxChunkSize, Equals, DefMaxChunkSize)
	c.Assert(vars.EnableRadixJoin, Equals, DefTiDBUseRadixJoin)
	c.Assert(vars.AllowWriteRowID, Equals, DefOptWriteRowID)
	c.Assert(vars.TiDBOptJoinReorderThreshold, Equals, DefTiDBOptJoinReorderThreshold)

	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.Concurrency))
	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.BatchSize))
}

func assertFieldsGreaterThanZero(c *C, val reflect.Value) {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		c.Assert(fieldVal.Int(), Greater, int64(0))
	}
}

func (s *testVarsutilSuite) TestSetOverflowBehave(c *C) {
	ddRegWorker := maxDDLReorgWorkerCount + 1
	SetDDLReorgWorkerCounter(ddRegWorker)
	c.Assert(maxDDLReorgWorkerCount, Equals, GetDDLReorgWorkerCounter())

	ddlReorgBatchSize := MaxDDLReorgBatchSize + 1
	SetDDLReorgBatchSize(ddlReorgBatchSize)
	c.Assert(MaxDDLReorgBatchSize, Equals, GetDDLReorgBatchSize())
	ddlReorgBatchSize = MinDDLReorgBatchSize - 1
	SetDDLReorgBatchSize(ddlReorgBatchSize)
	c.Assert(MinDDLReorgBatchSize, Equals, GetDDLReorgBatchSize())

	val := tidbOptInt64("a", 1)
	c.Assert(val, Equals, int64(1))
	val2 := tidbOptFloat64("b", 1.2)
	c.Assert(val2, Equals, 1.2)
}

func (s *testVarsutilSuite) TestValidate(c *C) {
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()
	v.TimeZone = time.UTC

	tests := []struct {
		key   string
		value string
		error bool
	}{
		{DelayKeyWrite, "ON", false},
		{DelayKeyWrite, "OFF", false},
		{DelayKeyWrite, "ALL", false},
		{DelayKeyWrite, "3", true},
		{ForeignKeyChecks, "3", true},
		{MaxSpRecursionDepth, "256", false},
		{SessionTrackGtids, "OFF", false},
		{SessionTrackGtids, "OWN_GTID", false},
		{SessionTrackGtids, "ALL_GTIDS", false},
		{SessionTrackGtids, "ON", true},
		{EnforceGtidConsistency, "OFF", false},
		{EnforceGtidConsistency, "ON", false},
		{EnforceGtidConsistency, "WARN", false},
		{QueryCacheType, "OFF", false},
		{QueryCacheType, "ON", false},
		{QueryCacheType, "DEMAND", false},
		{QueryCacheType, "3", true},
		{SecureAuth, "1", false},
		{SecureAuth, "3", true},
		{MyISAMUseMmap, "ON", false},
		{MyISAMUseMmap, "OFF", false},
		{TiDBEnableTablePartition, "ON", false},
		{TiDBEnableTablePartition, "OFF", false},
		{TiDBEnableTablePartition, "AUTO", false},
		{TiDBEnableTablePartition, "UN", true},
		{TiDBOptCorrelationExpFactor, "a", true},
		{TiDBOptCorrelationExpFactor, "-10", true},
		{TiDBOptCorrelationThreshold, "a", true},
		{TiDBOptCorrelationThreshold, "-2", true},
		{TiDBOptCPUFactor, "a", true},
		{TiDBOptCPUFactor, "-2", true},
		{TiDBOptCopCPUFactor, "a", true},
		{TiDBOptCopCPUFactor, "-2", true},
		{TiDBOptNetworkFactor, "a", true},
		{TiDBOptNetworkFactor, "-2", true},
		{TiDBOptScanFactor, "a", true},
		{TiDBOptScanFactor, "-2", true},
		{TiDBOptDescScanFactor, "a", true},
		{TiDBOptDescScanFactor, "-2", true},
		{TiDBOptSeekFactor, "a", true},
		{TiDBOptSeekFactor, "-2", true},
		{TiDBOptMemoryFactor, "a", true},
		{TiDBOptMemoryFactor, "-2", true},
		{TiDBOptDiskFactor, "a", true},
		{TiDBOptDiskFactor, "-2", true},
		{TiDBOptConcurrencyFactor, "a", true},
		{TiDBOptConcurrencyFactor, "-2", true},
		{TxnIsolation, "READ-UNCOMMITTED", true},
		{TiDBInitChunkSize, "a", true},
		{TiDBInitChunkSize, "-1", true},
		{TiDBMaxChunkSize, "a", true},
		{TiDBMaxChunkSize, "-1", true},
		{TiDBOptJoinReorderThreshold, "a", true},
		{TiDBOptJoinReorderThreshold, "-1", true},
		{TiDBReplicaRead, "invalid", true},
	}

	for _, t := range tests {
		_, err := ValidateSetSystemVar(v, t.key, t.value)
		if t.error {
			c.Assert(err, NotNil, Commentf("%v got err=%v", t, err))
		} else {
			c.Assert(err, IsNil, Commentf("%v got err=%v", t, err))
		}
	}

}
