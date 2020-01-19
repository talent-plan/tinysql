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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// TiDBDriver implements IDriver.
type TiDBDriver struct {
	store kv.Storage
}

// NewTiDBDriver creates a new TiDBDriver.
func NewTiDBDriver(store kv.Storage) *TiDBDriver {
	driver := &TiDBDriver{
		store: store,
	}
	return driver
}

// TiDBContext implements QueryCtx.
type TiDBContext struct {
	session   session.Session
	currentDB string
}

// OpenCtx implements IDriver.
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (QueryCtx, error) {
	se, err := session.CreateSession(qd.store)
	if err != nil {
		return nil, err
	}
	se.SetTLSState(tlsState)
	err = se.SetCollation(int(collation))
	if err != nil {
		return nil, err
	}
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &TiDBContext{
		session:   se,
		currentDB: dbname,
	}
	return tc, nil
}

// Status implements QueryCtx Status method.
func (tc *TiDBContext) Status() uint16 {
	return tc.session.Status()
}

// LastInsertID implements QueryCtx LastInsertID method.
func (tc *TiDBContext) LastInsertID() uint64 {
	return tc.session.LastInsertID()
}

// Value implements QueryCtx Value method.
func (tc *TiDBContext) Value(key fmt.Stringer) interface{} {
	return tc.session.Value(key)
}

// SetValue implements QueryCtx SetValue method.
func (tc *TiDBContext) SetValue(key fmt.Stringer, value interface{}) {
	tc.session.SetValue(key, value)
}

// CommitTxn implements QueryCtx CommitTxn method.
func (tc *TiDBContext) CommitTxn(ctx context.Context) error {
	return tc.session.CommitTxn(ctx)
}

// RollbackTxn implements QueryCtx RollbackTxn method.
func (tc *TiDBContext) RollbackTxn() {
	tc.session.RollbackTxn(context.TODO())
}

// AffectedRows implements QueryCtx AffectedRows method.
func (tc *TiDBContext) AffectedRows() uint64 {
	return tc.session.AffectedRows()
}

// LastMessage implements QueryCtx LastMessage method.
func (tc *TiDBContext) LastMessage() string {
	return tc.session.LastMessage()
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *TiDBContext) CurrentDB() string {
	return tc.currentDB
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	return tc.session.GetSessionVars().StmtCtx.WarningCount()
}

// Execute implements QueryCtx Execute method.
func (tc *TiDBContext) Execute(ctx context.Context, sql string) (rs []ResultSet, err error) {
	rsList, err := tc.session.Execute(ctx, sql)
	if err != nil {
		return
	}
	if len(rsList) == 0 { // result ok
		return
	}
	rs = make([]ResultSet, len(rsList))
	for i := 0; i < len(rsList); i++ {
		rs[i] = &tidbResultSet{
			recordSet: rsList[i],
		}
	}
	return
}

// SetClientCapability implements QueryCtx SetClientCapability method.
func (tc *TiDBContext) SetClientCapability(flags uint32) {
	tc.session.SetClientCapability(flags)
}

// Close implements QueryCtx Close method.
func (tc *TiDBContext) Close() error {
	tc.session.Close()
	return nil
}

// FieldList implements QueryCtx FieldList method.
func (tc *TiDBContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	fields, err := tc.session.FieldList(table)
	if err != nil {
		return nil, err
	}
	columns = make([]*ColumnInfo, 0, len(fields))
	for _, f := range fields {
		columns = append(columns, convertColumnInfo(f))
	}
	return columns, nil
}

// SetCommandValue implements QueryCtx SetCommandValue method.
func (tc *TiDBContext) SetCommandValue(command byte) {
	tc.session.SetCommandValue(command)
}

// GetSessionVars return SessionVars.
func (tc *TiDBContext) GetSessionVars() *variable.SessionVars {
	return tc.session.GetSessionVars()
}

type tidbResultSet struct {
	recordSet sqlexec.RecordSet
	columns   []*ColumnInfo
	rows      []chunk.Row
	closed    int32
}

func (trs *tidbResultSet) NewChunk() *chunk.Chunk {
	return trs.recordSet.NewChunk()
}

func (trs *tidbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return trs.recordSet.Next(ctx, req)
}

func (trs *tidbResultSet) StoreFetchedRows(rows []chunk.Row) {
	trs.rows = rows
}

func (trs *tidbResultSet) GetFetchedRows() []chunk.Row {
	if trs.rows == nil {
		trs.rows = make([]chunk.Row, 0, 1024)
	}
	return trs.rows
}

func (trs *tidbResultSet) Close() error {
	if !atomic.CompareAndSwapInt32(&trs.closed, 0, 1) {
		return nil
	}
	err := trs.recordSet.Close()
	trs.recordSet = nil
	return err
}

// OnFetchReturned implements fetchNotifier#OnFetchReturned
func (trs *tidbResultSet) OnFetchReturned() {
	if cl, ok := trs.recordSet.(fetchNotifier); ok {
		cl.OnFetchReturned()
	}
}

func (trs *tidbResultSet) Columns() []*ColumnInfo {
	if trs.columns != nil {
		return trs.columns
	}
	if trs.columns == nil {
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trs.columns = append(trs.columns, convertColumnInfo(v))
		}
	}
	return trs.columns
}

func convertColumnInfo(fld *ast.ResultField) (ci *ColumnInfo) {
	ci = &ColumnInfo{
		Name:    fld.ColumnAsName.O,
		OrgName: fld.Column.Name.O,
		Table:   fld.TableAsName.O,
		Schema:  fld.DBName.O,
		Flag:    uint16(fld.Column.Flag),
		Charset: uint16(mysql.CharsetNameToID(fld.Column.Charset)),
		Type:    fld.Column.Tp,
	}

	if fld.Table != nil {
		ci.OrgTable = fld.Table.Name.O
	}
	if fld.Column.Flen == types.UnspecifiedLength {
		ci.ColumnLength = 0
	} else {
		ci.ColumnLength = uint32(fld.Column.Flen)
	}
	if fld.Column.Tp == mysql.TypeNewDecimal {
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.Decimal > int(types.DefaultFsp) {
			// Consider the decimal point.
			ci.ColumnLength++
		}
	} else if types.IsString(fld.Column.Tp) {
		// Fix issue #4540.
		// The flen is a hint, not a precise value, so most client will not use the value.
		// But we found in rare MySQL client, like Navicat for MySQL(version before 12) will truncate
		// the `show create table` result. To fix this case, we must use a large enough flen to prevent
		// the truncation, in MySQL, it will multiply bytes length by a multiple based on character set.
		// For examples:
		// * latin, the multiple is 1
		// * gb2312, the multiple is 2
		// * Utf-8, the multiple is 3
		// * utf8mb4, the multiple is 4
		// We used to check non-string types to avoid the truncation problem in some MySQL
		// client such as Navicat. Now we only allow string type enter this branch.
		charsetDesc, err := charset.GetCharsetDesc(fld.Column.Charset)
		if err != nil {
			ci.ColumnLength = ci.ColumnLength * 4
		} else {
			ci.ColumnLength = ci.ColumnLength * uint32(charsetDesc.Maxlen)
		}
	}

	if fld.Column.Decimal == types.UnspecifiedLength {
		if fld.Column.Tp == mysql.TypeDuration {
			ci.Decimal = uint8(types.DefaultFsp)
		} else {
			ci.Decimal = mysql.NotFixedDec
		}
	} else {
		ci.Decimal = uint8(fld.Column.Decimal)
	}

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}
