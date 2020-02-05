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

package session

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// CreateGloablVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGloablVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE if not exists mysql.tidb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`

	// CreateHelpTopic is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopic = `CREATE TABLE if not exists mysql.help_topic (
  		help_topic_id int(10) unsigned NOT NULL,
  		name char(64) NOT NULL,
  		help_category_id smallint(5) unsigned NOT NULL,
  		description text NOT NULL,
  		example text NOT NULL,
  		url text NOT NULL,
  		PRIMARY KEY (help_topic_id),
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE if not exists mysql.stats_histograms (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		distinct_count bigint(64) NOT NULL,
		null_count bigint(64) NOT NULL DEFAULT 0,
		tot_col_size bigint(64) NOT NULL DEFAULT 0,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) unsigned NOT NULL DEFAULT 0,
		cm_sketch blob,
		stats_ver bigint(64) NOT NULL DEFAULT 0,
		flag bigint(64) NOT NULL DEFAULT 0,
		correlation double NOT NULL DEFAULT 0,
		last_analyze_pos blob DEFAULT NULL,
		unique index tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists mysql.stats_buckets (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		bucket_id bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		repeats bigint(64) NOT NULL,
		upper_bound blob NOT NULL,
		lower_bound blob ,
		unique index tbl(table_id, is_index, hist_id, bucket_id)
	);`

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE if not exists mysql.stats_top_n (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		value longblob,
		count bigint(64) UNSIGNED NOT NULL,
		index tbl(table_id, is_index, hist_id)
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	startTime := time.Now()
	dom := domain.GetDomain(s)
	for {
		b, err := checkBootstrapped(s)
		if err != nil {
			logutil.BgLogger().Fatal("check bootstrap error",
				zap.Error(err))
		}
		// To reduce conflict when multiple TiDB-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose DDL owner to do this.
		if !b && dom.DDL().OwnerManager().IsOwner() {
			doDDLWorks(s)
			doDMLWorks(s)
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// The variable name in mysql.TiDB table.
	// It is used for checking if the store is boostrapped by any TiDB server.
	bootstrappedVar = "bootstrapped"
	// The variable value in mysql.TiDB table for bootstrappedVar.
	// If the value true, the store is already boostrapped by a TiDB server.
	bootstrappedVarTrue = "True"
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB server which bootstrapped the store.
	tidbServerVersionVar = "tidb_server_version"
)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.Execute(context.Background(), fmt.Sprintf("USE %s;", mysql.SystemDB))
	if err != nil && infoschema.ErrDatabaseNotExists.NotEqual(err) {
		logutil.BgLogger().Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in TiDB table.
	sVal, _, err := getTiDBVar(s, bootstrappedVar)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := sVal == bootstrappedVarTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(context.Background()); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s Session, name string) (sVal string, isNull bool, e error) {
	sql := fmt.Sprintf(`SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, name)
	ctx := context.Background()
	rs, err := s.Execute(ctx, sql)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if len(rs) != 1 {
		return "", true, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewChunk()
	err = r.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	// Create global system variable table.
	mustExecute(s, CreateGloablVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range table.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done table.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_topn_store table.
	mustExecute(s, CreateStatsTopNTable)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// Session only variable should not be inserted.
		if v.Scope != variable.ScopeSession {
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, bootstrappedVarTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, bootstrapped)
	mustExecute(s, sql)

	_, err := s.Execute(context.Background(), "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err))
	}
}

func mustExecute(s Session, sql string) {
	_, err := s.Execute(context.Background(), sql)
	if err != nil {
		debug.PrintStack()
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err))
	}
}
