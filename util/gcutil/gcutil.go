// Copyright 2019 PingCAP, Inc.
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

package gcutil

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	selectVariableValueSQL = `SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name='%s'`
)

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx sessionctx.Context, snapshotTS uint64) error {
	safePointTS, err := GetGCSafePoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(model.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// GetGCSafePoint loads GC safe point time from mysql.tidb.
func GetGCSafePoint(ctx sessionctx.Context) (uint64, error) {
	sql := fmt.Sprintf(selectVariableValueSQL, "tikv_gc_safe_point")
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.New("can not get 'tikv_gc_safe_point'")
	}
	safePointString := rows[0].GetString(0)
	safePointTime, err := util.CompatibleParseGCTime(safePointString)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := variable.GoTimeToTS(safePointTime)
	return ts, nil
}
