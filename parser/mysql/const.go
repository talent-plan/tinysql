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

package mysql

import (
	"fmt"
)

// Version information.
var (
	// TiDBReleaseVersion is initialized by (git describe --tags) in Makefile.
	TiDBReleaseVersion = "None"

	// ServerVersion is the version information of this tidb-server in MySQL's format.
	ServerVersion = fmt.Sprintf("TinySQL-lab0-%s", TiDBReleaseVersion)
)

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

// Auth name information.
const (
	AuthNativePassword      = "mysql_native_password"
	AuthCachingSha2Password = "caching_sha2_password"
	AuthSocket              = "auth_socket"
)

// Protocol Features
const AuthSwitchRequest byte = 0xfe

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

// Command information.
const (
	ComSleep byte = iota
	ComQuit
	ComInitDB
	ComQuery
	ComFieldList
	ComCreateDB
	ComDropDB
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComTableDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
	ComDaemon
	ComBinlogDumpGtid
	ComResetConnection
	ComEnd
)

// Command2Str is the command information to command name.
var Command2Str = map[byte]string{
	ComSleep:            "Sleep",
	ComQuit:             "Quit",
	ComInitDB:           "Init DB",
	ComQuery:            "Query",
	ComFieldList:        "Field List",
	ComCreateDB:         "Create DB",
	ComDropDB:           "Drop DB",
	ComRefresh:          "Refresh",
	ComShutdown:         "Shutdown",
	ComStatistics:       "Statistics",
	ComProcessInfo:      "Processlist",
	ComConnect:          "Connect",
	ComProcessKill:      "Kill",
	ComDebug:            "Debug",
	ComPing:             "Ping",
	ComTime:             "Time",
	ComDelayedInsert:    "Delayed Insert",
	ComChangeUser:       "Change User",
	ComBinlogDump:       "Binlog Dump",
	ComTableDump:        "Table Dump",
	ComConnectOut:       "Connect out",
	ComRegisterSlave:    "Register Slave",
	ComStmtPrepare:      "Prepare",
	ComStmtExecute:      "Execute",
	ComStmtSendLongData: "Long Data",
	ComStmtClose:        "Close stmt",
	ComStmtReset:        "Reset stmt",
	ComSetOption:        "Set option",
	ComStmtFetch:        "Fetch",
	ComDaemon:           "Daemon",
	ComBinlogDumpGtid:   "Binlog Dump",
	ComResetConnection:  "Reset connect",
}

// Client information.
const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAtts
	ClientPluginAuthLenencClientData
)

// Identifier length limitations.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxTableNameLength is max length of table name identifier.
	MaxTableNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)
