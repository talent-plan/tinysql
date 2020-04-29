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

package util

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
)

// LoadDDLReorgVars loads ddl reorg variable from mysql.global_variables.
func LoadDDLReorgVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize})
}

// LoadDDLVars loads ddl variable from mysql.global_variables.
func LoadDDLVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLErrorCountLimit})
}

const loadGlobalVarsSQL = "select HIGH_PRIORITY variable_name, variable_value from mysql.global_variables where variable_name in (%s)"

// LoadGlobalVars loads global variable from mysql.global_variables.
func LoadGlobalVars(ctx sessionctx.Context, varNames []string) error {
	if sctx, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		nameList := ""
		for i, name := range varNames {
			if i > 0 {
				nameList += ", "
			}
			nameList += fmt.Sprintf("'%s'", name)
		}
		sql := fmt.Sprintf(loadGlobalVarsSQL, nameList)
		rows, _, err := sctx.ExecRestrictedSQL(sql)
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range rows {
			varName := row.GetString(0)
			varValue := row.GetString(1)
			variable.SetLocalSystemVar(varName, varValue)
		}
	}
	return nil
}
