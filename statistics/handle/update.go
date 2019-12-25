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

package handle

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.ColSize == nil {
		item.ColSize = make(map[int64]int64)
	}
	if colSize != nil {
		for key, val := range *colSize {
			item.ColSize[key] += val
		}
	}
	m[id] = item
}

func (h *Handle) merge(s *SessionStatsCollector) {
	for id, item := range s.mapper {
		h.globalMap.update(id, item.Delta, item.Count, &item.ColSize)
	}
	s.mapper = make(tableDeltaMap)
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper tableDeltaMap
	next   *SessionStatsCollector
	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.update(id, delta, count, colSize)
}

var (
	// MinLogScanCount is the minimum scan count for a feedback to be logged.
	MinLogScanCount = int64(1000)
	// MinLogErrorRate is the minimum error rate for a feedback to be logged.
	MinLogErrorRate = 0.5
)

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		next:   h.listHead.next,
	}
	h.listHead.next = newCollector
	return newCollector
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta returns true when only updates a small portion of the table and the time since last update
// do not exceed one hour.
func needDumpStatsDelta(h *Handle, id int64, item variable.TableDelta, currentTime time.Time) bool {
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	tbl, ok := h.statsCache.Load().(statsCache).tables[id]
	if !ok {
		// No need to dump if the stats is invalid.
		return false
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	if tbl.Count == 0 || float64(item.Count)/float64(tbl.Count) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

type dumpMode bool

const (
	// DumpAll indicates dump all the delta info in to kv.
	DumpAll dumpMode = true
	// DumpDelta indicates dump part of the delta info in to kv.
	DumpDelta dumpMode = false
)

// sweepList will loop over the list, merge each session's local stats into handle
// and remove closed session's collector.
func (h *Handle) sweepList() {
	prev := h.listHead
	prev.Lock()
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the session stats into handle and error rate map.
		h.merge(curr)
		if curr.deleted {
			prev.next = curr.next
			// Since the session is already closed, we can safely unlock it here.
			curr.Unlock()
		} else {
			// Unlock the previous lock, so we only holds at most two session's lock at the same time.
			prev.Unlock()
			prev = curr
		}
	}
	prev.Unlock()
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.sweepList()
	currentTime := time.Now()
	for id, item := range h.globalMap {
		if mode == DumpDelta && !needDumpStatsDelta(h, id, item, currentTime) {
			continue
		}
		updated, err := h.dumpTableStatCountToKV(id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if updated {
			h.globalMap.update(id, -item.Delta, -item.Count, nil)
		}
		if err = h.dumpTableStatColSizeToKV(id, item); err != nil {
			return errors.Trace(err)
		}
		if updated {
			delete(h.globalMap, id)
		} else {
			m := h.globalMap[id]
			m.ColSize = nil
			h.globalMap[id] = m
		}
	}
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (updated bool, err error) {
	if delta.Count == 0 {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()

	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return false, errors.Trace(err)
	}
	startTS := txn.StartTS()
	var sql string
	if delta.Delta < 0 {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count - %d, modify_count = modify_count + %d where table_id = %d and count >= %d", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", startTS, delta.Delta, delta.Count, id)
	}
	err = execSQLs(context.Background(), exec, []string{sql})
	updated = h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	return
}

func (h *Handle) dumpTableStatColSizeToKV(id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	if len(values) == 0 {
		return nil
	}
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	return errors.Trace(err)
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// AutoAnalyzeMinCnt means if the count of table is less than this value, we needn't do auto analyze.
var AutoAnalyzeMinCnt int64 = 1000

// TableAnalyzed checks if the table is analyzed.
func TableAnalyzed(tbl *statistics.Table) bool {
	for _, col := range tbl.Columns {
		if col.Count > 0 {
			return true
		}
	}
	for _, idx := range tbl.Indices {
		if idx.Histogram.Len() > 0 {
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
// 1. If the table has never been analyzed, we need to analyze it when it has
//    not been modified for a while.
// 2. If the table had been analyzed before, we need to analyze it when
//    "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//    between `start` and `end`.
func NeedAnalyzeTable(tbl *statistics.Table, limit time.Duration, autoAnalyzeRatio float64, start, end, now time.Time) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		dur := time.Since(t)
		return dur >= limit, fmt.Sprintf("table unanalyzed, time since last updated %vs", dur)
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	if float64(tbl.ModifyCount)/float64(tbl.Count) <= autoAnalyzeRatio {
		return false, ""
	}
	// Tests if current time is within the time period.
	return timeutil.WithinDayTimePeriod(start, end, now), fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tbl.Count, autoAnalyzeRatio)
}

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	sql := fmt.Sprintf("select variable_name, variable_value from mysql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.TiDBAutoAnalyzeRatio, variable.TiDBAutoAnalyzeStartTime, variable.TiDBAutoAnalyzeEndTime)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string)
	for _, row := range rows {
		parameters[row.GetString(0)] = row.GetString(1)
	}
	return parameters
}

func parseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return variable.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

func parseAnalyzePeriod(start, end string) (time.Time, time.Time, error) {
	if start == "" {
		start = variable.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = variable.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(variable.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(variable.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) {
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("[stats] parse auto analyze period failed", zap.Error(err))
		return
	}
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			pi := tblInfo.GetPartitionInfo()
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if pi == nil {
				statsTbl := h.GetTableStats(tblInfo)
				sql := fmt.Sprintf("analyze table %s", tblName)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					return
				}
				continue
			}
			for _, def := range pi.Definitions {
				sql := fmt.Sprintf("analyze table %s partition `%s`", tblName, def.Name.O)
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					return
				}
				continue
			}
		}
	}
}

func (h *Handle) autoAnalyzeTable(tblInfo *model.TableInfo, statsTbl *statistics.Table, start, end time.Time, ratio float64, sql string) bool {
	if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio, start, end, time.Now()); needAnalyze {
		logutil.BgLogger().Info("[stats] auto analyze triggered", zap.String("sql", sql), zap.String("reason", reason))
		h.execAutoAnalyze(sql)
		return true
	}
	for _, idx := range tblInfo.Indices {
		if _, ok := statsTbl.Indices[idx.ID]; !ok && idx.State == model.StatePublic {
			sql = fmt.Sprintf("%s index `%s`", sql, idx.Name.O)
			logutil.BgLogger().Info("[stats] auto analyze for unanalyzed", zap.String("sql", sql))
			h.execAutoAnalyze(sql)
			return true
		}
	}
	return false
}

func (h *Handle) execAutoAnalyze(sql string) {
	startTime := time.Now()
	_, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	dur := time.Since(startTime)

	if err != nil {
		logutil.BgLogger().Error("[stats] auto analyze failed", zap.String("sql", sql), zap.Duration("cost_time", dur), zap.Error(err))

	} else {

	}
}
