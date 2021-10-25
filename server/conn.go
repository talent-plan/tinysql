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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tinysql/parser/mysql"
	"tinysql/parser/terror"
	"tinysql/session"
	"tinysql/util/arena"
	"tinysql/util/hack"
	"tinysql/util/logutil"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: s.globalConnID.NextID(),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
		status:       connStatusDispatching,
		lastActive:   time.Now(),
		authPlugin:   mysql.AuthNativePassword,
	}
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint64            // atomically allocated by a global variable, unique in process scope.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
	lastPacket   []byte            // latest sql query string, currently used for logging error.
	ctx          *TiDBContext      // an interface to execute sql statements.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	peerHost     string            // peer host
	peerPort     string            // peer port
	status       int32             // dispatching/reading/shutdown/waitshutdown
	lastCode     uint16            // last error code
	collation    uint8             // collation used by client, may be different from the collation used by database.
	lastActive   time.Time         // last active time
	authPlugin   string            // default authentication plugin
	isUnixSocket bool              // connection is Unix Socket file

	// mu is used for cancelling the execution of current transaction.
	mu struct {
		sync.RWMutex
		cancelFunc context.CancelFunc
	}
}

func (cc *clientConn) String() string {
	//	collationStr := mysql.Collations[cc.collation]
	//	return fmt.Sprintf("id:%d, addr:%s status:%b, collation:%s, user:%s",
	//		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), collationStr, cc.user,
	//	)
	return "TODO"
}

// authSwitchRequest is used by the server to ask the client to switch to a different authentication
// plugin. MySQL 8.0 libmysqlclient based clients by default always try `caching_sha2_password`, even
// when the server advertises the its default to be `mysql_native_password`. In addition to this switching
// may be needed on a per user basis as the authentication method is set per user.
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
// https://bugs.mysql.com/bug.php?id=93044
func (cc *clientConn) authSwitchRequest(ctx context.Context, plugin string) ([]byte, error) {
	enclen := 1 + len(plugin) + 1 + len(cc.salt) + 1
	data := cc.alloc.AllocWithLen(4, enclen)
	data = append(data, mysql.AuthSwitchRequest) // switch request
	data = append(data, []byte(plugin)...)
	data = append(data, byte(0x00)) // requires null
	data = append(data, cc.salt...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return nil, err
	}
	resp, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail", zap.Error(err))
		}
		return nil, err
	}
	cc.authPlugin = plugin
	return resp, nil
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send sql query to server.
func (cc *clientConn) handshake(ctx context.Context) error {
	if err := cc.writeInitialHandshake(ctx); err != nil {
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("Could not send handshake due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("Write init handshake to client fail", zap.Error(errors.SuspendStack(err)))
		}
		return err
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		err1 := cc.writeError(ctx, err)
		if err1 != nil {
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		return err
	}

	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, mysql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.sequence = 0
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return err
	}

	err = cc.flush(ctx)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return err
	}
	return err
}

func (cc *clientConn) Close() error {
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	return closeConn(cc, connections)
}

func closeConn(cc *clientConn, connections int) error {
	err := cc.bufReadConn.Close()
	terror.Log(err)
	return nil
}

func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc, len(cc.server.clients))
}

// writeInitialHandshake sends server version, connection ID, server capability, collation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake(ctx context.Context) error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.collation == 0 {
		cc.collation = uint8(mysql.DefaultCollationID)
	}
	data = append(data, cc.collation)
	// status
	data = dumpUint16(data, mysql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	if cc.ctx == nil {
		err := cc.openSession()
		if err != nil {
			return err
		}
	}

	defAuthPlugin := "mysql_native_password"
	cc.authPlugin = defAuthPlugin
	data = append(data, []byte(defAuthPlugin)...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush(ctx)
}

func (cc *clientConn) readPacket() ([]byte, error) {
	return cc.pkt.readPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	return cc.pkt.writePacket(data)
}

// getSessionVarsWaitTimeout get session variable wait_timeout
func (cc *clientConn) getSessionVarsWaitTimeout(ctx context.Context) uint64 {
	// Let’s simply set it to 0 now.
	return 0
}

type handshakeResponse41 struct {
	Capability uint32
	Collation  uint8
	User       string
	DBName     string
	Auth       []byte
	AuthPlugin string
	Attrs      map[string]string
}

// parseOldHandshakeResponseHeader parses the old version handshake header HandshakeResponse320
func parseOldHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse320
	logutil.Logger(ctx).Debug("try to parse hanshake response as Protocol::HandshakeResponse320", zap.ByteString("packetData", data))
	if len(data) < 2+3 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}
	offset := 0
	// capability
	capability := binary.LittleEndian.Uint16(data[:2])
	packet.Capability = uint32(capability)

	// be compatible with Protocol::HandshakeResponse41
	packet.Capability |= mysql.ClientProtocol41

	offset += 2
	// skip max packet size
	offset += 3
	// usa default CharsetID
	packet.Collation = mysql.CollationNames["utf8mb4_general_ci"]

	return offset, nil
}

// parseOldHandshakeResponseBody parse the HandshakeResponse for Protocol::HandshakeResponse320 (except the common header part).
func parseOldHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data), zap.Stack("stack"))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
		if len(data[offset:]) > 0 {
			packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
	}

	return nil
}

// parseHandshakeResponseHeader parses the common header of SSLRequest and HandshakeResponse41.
func parseHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if len(data) < 4+4+1+23 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}

	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// parseHandshakeResponseBody parse the HandshakeResponse (except the common header part).
func parseHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
		// MySQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/mysql/mysql-server/blob/5.7/sql-common/client.c#L3478
		if data[offset] == 0x1 { // No auth data
			offset += 2
		} else {
			num, null, off := parseLengthEncodedInt(data[offset:])
			offset += off
			if !null {
				packet.Auth = data[offset : offset+int(num)]
				offset += int(num)
			}
		}
	} else if packet.Capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		packet.Auth = data[offset : offset+authLen]
		offset += authLen
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(packet.Auth) + 1
	}

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset += idx + 1
		}
	}

	if packet.Capability&mysql.ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[offset:], 0)
		s := offset
		f := offset + idx
		if s < f { // handle unexpected bad packets
			packet.AuthPlugin = string(data[s:f])
		}
		offset += idx + 1
	}

	if packet.Capability&mysql.ClientConnectAtts > 0 {
		if len(data[offset:]) == 0 {
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		if num, null, off := parseLengthEncodedInt(data[offset:]); !null {
			offset += off
			row := data[offset : offset+int(num)]
			attrs, err := parseAttrs(row)
			if err != nil {
				logutil.Logger(ctx).Warn("parse attrs failed", zap.Error(err))
				return nil
			}
			packet.Attrs = attrs
		}
	}

	return nil
}

func parseAttrs(data []byte) (map[string]string, error) {
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		key, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off
		value, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off

		attrs[string(key)] = string(value)
	}
	return attrs, nil
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("wait handshake response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("wait handshake response fail", zap.Error(err))
		}
		return err
	}

	isOldVersion := false

	var resp handshakeResponse41
	var pos int

	if len(data) < 2 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return mysql.ErrMalformPacket
	}

	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 > 0 {
		pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
	} else {
		pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
		isOldVersion = true
	}

	if err != nil {
		terror.Log(err)
		return err
	}

	if resp.Capability&mysql.ClientSSL > 0 {
		err := errSecureTransportRequired.FastGenByArgs()
		terror.Log(err)
		return err
	}

	// Read the remaining part of the packet.
	if isOldVersion {
		err = parseOldHandshakeResponseBody(ctx, &resp, data, pos)
	} else {
		err = parseHandshakeResponseBody(ctx, &resp, data, pos)
	}
	if err != nil {
		terror.Log(err)
		return err
	}

	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.collation = resp.Collation
	cc.attrs = resp.Attrs

	newAuth, err := cc.checkAuthPlugin(ctx, &resp.AuthPlugin)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to check the user authplugin", zap.Error(err))
	}
	if len(newAuth) > 0 {
		resp.Auth = newAuth
	}

	err = cc.openSessionAndDoAuth(resp.Auth)
	if err != nil {
		logutil.Logger(ctx).Warn("open new session or authentication failure", zap.Error(err))
	}
	return err
}

func (cc *clientConn) authSha(ctx context.Context) ([]byte, error) {

	const (
		ShaCommand       = 1
		RequestRsaPubKey = 2
		FastAuthOk       = 3
		FastAuthFail     = 4
	)

	err := cc.writePacket([]byte{0, 0, 0, 0, ShaCommand, FastAuthFail})
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet write failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet flush failed", zap.Error(err))
		return nil, err
	}

	data, err := cc.readPacket()
	if err != nil {
		logutil.Logger(ctx).Error("authSha packet read failed", zap.Error(err))
		return nil, err
	}
	return bytes.Trim(data, "\x00"), nil
}

func (cc *clientConn) SessionStatusToString() string {
	return "session status"
}

func (cc *clientConn) openSession() error {
	se, err := session.CreateSession()
	if err != nil {
		return err
	}
	cc.ctx = &TiDBContext{
		Session: se,
	}
	return nil
}

func (cc *clientConn) openSessionAndDoAuth(authData []byte) error {
	return nil
}

// Check if the Authentication Plugin of the server, client and user configuration matches
func (cc *clientConn) checkAuthPlugin(ctx context.Context, authPlugin *string) ([]byte, error) {
	return nil, nil
}

func (cc *clientConn) PeerHost(hasPassword string) (host, port string, err error) {
	if len(cc.peerHost) > 0 {
		return cc.peerHost, "", nil
	}
	//	host = variable.DefHostname
	host = "localhost"
	if cc.isUnixSocket {
		cc.peerHost = host
		return
	}
	addr := cc.bufReadConn.RemoteAddr().String()
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		err = errAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.peerHost = host
	cc.peerPort = port
	return
}

// skipInitConnect follows MySQL's rules of when init-connect should be skipped.
// In 5.7 it is any user with SUPER privilege, but in 8.0 it is:
// - SUPER or the CONNECTION_ADMIN dynamic privilege.
// - (additional exception) users with expired passwords (not yet supported)
// In TiDB CONNECTION_ADMIN is satisfied by SUPER, so we only need to check once.
func (cc *clientConn) skipInitConnect() bool {
	//	checker := privilege.GetPrivilegeManager(cc.ctx.Session)
	//	activeRoles := cc.ctx.GetSessionVars().ActiveRoles
	//	return checker != nil && checker.RequestDynamicVerification(activeRoles, "CONNECTION_ADMIN", false)
	return false
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	const size = 4096
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("connection running loop panic",
				//				zap.Stringer("lastSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)
			err := cc.writeError(ctx, errors.New(fmt.Sprintf("%v", r)))
			terror.Log(err)
			//		metrics.PanicCounter.WithLabelValues(metrics.LabelSession).Inc()
		}
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}
	}()
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) ||
			// The judge below will not be hit by all means,
			// But keep it stayed as a reminder and for the code reference for connStatusWaitShutdown.
			atomic.LoadInt32(&cc.status) == connStatusWaitShutdown {
			return
		}

		cc.alloc.Reset()
		// close connection when idle time is more than wait_timeout
		waitTimeout := cc.getSessionVarsWaitTimeout(ctx)
		cc.pkt.setReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					idleTime := time.Since(start)
					logutil.Logger(ctx).Info("read packet timeout, close this connection",
						zap.Duration("idle", idleTime),
						zap.Uint64("waitTimeout", waitTimeout),
						zap.Error(err),
					)
				} else {
					errStack := errors.ErrorStack(err)
					if !strings.Contains(errStack, "use of closed network connection") {
						logutil.Logger(ctx).Warn("read packet failed, close this connection",
							zap.Error(errors.SuspendStack(err)))
					}
				}
			}
			return
		}

		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			return
		}

		if err = cc.dispatch(ctx, data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
				return
			} else if terror.ErrCritical.Equal(err) {
				logutil.Logger(ctx).Fatal("critical error, stop the server", zap.Error(err))
			}
			var txnMode string
			logutil.Logger(ctx).Info("command dispatched failed",
				zap.String("connInfo", cc.String()),
				zap.String("command", mysql.Command2Str[data[0]]),
				zap.String("status", cc.SessionStatusToString()),
				zap.String("txn_mode", txnMode),
			)
			err1 := cc.writeError(ctx, err)
			terror.Log(err1)
		}
		cc.pkt.sequence = 0
	}
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
func (cc *clientConn) ShutdownOrNotify() bool {
	if (cc.ctx.Status() & mysql.ServerStatusInTrans) > 0 {
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the loop in clientConn.Run
	// will detect it and then exit.
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	defer func() {
		// reset killed for each request
		atomic.StoreUint32(&cc.ctx.GetSessionVars().Killed, 0)
	}()

	var cancelFunc context.CancelFunc
	ctx, cancelFunc = context.WithCancel(ctx)
	cc.mu.Lock()
	cc.mu.cancelFunc = cancelFunc
	cc.mu.Unlock()

	cc.lastPacket = data
	cmd := data[0]
	data = data[1:]

	vars := cc.ctx.GetSessionVars()
	// reset killed for each request
	atomic.StoreUint32(&vars.Killed, 0)

	dataStr := string(hack.String(data))

	switch cmd {
	case mysql.ComSleep:
		// TODO: According to mysql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComInitDB:
		return cc.writeOK(ctx)
	case mysql.ComQuery: // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related mysql document about it, but mysql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.mysql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
			dataStr = string(hack.String(data))
		}
		return cc.handleQuery(ctx, dataStr)
	case mysql.ComShutdown: // redirect to SQL
		if err := cc.handleQuery(ctx, "SHUTDOWN"); err != nil {
			return err
		}
		return cc.writeOK(ctx)
	case mysql.ComStatistics:
		return cc.writeStats(ctx)
	// ComProcessInfo, ComConnect, ComProcessKill, ComDebug
	case mysql.ComPing:
		return cc.writeOK(ctx)
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, cmd)
	}
}

func (cc *clientConn) writeStats(ctx context.Context) error {
	msg := []byte("Uptime: 0  Threads: 0  Questions: 0  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000")
	data := cc.alloc.AllocWithLen(4, len(msg))
	data = append(data, msg...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) flush(ctx context.Context) error {
	defer func() {
		trace.StartRegion(ctx, "FlushClientConn").End()
	}()
	return cc.pkt.flush()
}

func (cc *clientConn) writeOK(ctx context.Context) error {
	//	msg := cc.ctx.LastMessage()
	msg := "Welcome TinySQL"
	return cc.writeOkWith(ctx, msg, 0, 0, cc.ctx.Status(), 0)
}

func (cc *clientConn) writeOkWith(ctx context.Context, msg string, affectedRows, lastInsertID uint64, status, warnCnt uint16) error {
	enclen := 0
	if len(msg) > 0 {
		enclen = lengthEncodedIntSize(uint64(len(msg))) + len(msg)
	}

	data := cc.alloc.AllocWithLen(4, 32+enclen)
	data = append(data, mysql.OKHeader)
	data = dumpLengthEncodedInt(data, affectedRows)
	data = dumpLengthEncodedInt(data, lastInsertID)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = dumpUint16(data, status)
		data = dumpUint16(data, warnCnt)
	}
	if enclen > 0 {
		// although MySQL manual says the info message is string<EOF>(https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html),
		// it is actually string<lenenc>
		data = dumpLengthEncodedString(data, []byte(msg))
	}

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) writeError(ctx context.Context, e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = terror.ToSQLError(te)
	} else {
		e := errors.Cause(originErr)
		switch y := e.(type) {
		case *terror.Error:
			m = terror.ToSQLError(y)
		default:
			m = mysql.NewErrf(mysql.ErrUnknown, "%s", nil, e.Error())
		}
	}

	cc.lastCode = m.Code
	//	defer errno.IncrementError(m.Code, cc.user, cc.peerHost)
	data := cc.alloc.AllocWithLen(4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeEOF writes an EOF packet.
// Note this function won't flush the stream because maybe there are more
// packets following it.
// serverStatus, a flag bit represents server information
// in the packet.
func (cc *clientConn) writeEOF(serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		//data = dumpUint16(data, cc.ctx.WarningCount())
		data = dumpUint16(data, 0)
		status := cc.ctx.Status()
		status |= serverStatus
		data = dumpUint16(data, status)
	}

	err := cc.writePacket(data)
	return err
}

func (cc *clientConn) writeReq(ctx context.Context, filePath string) error {
	data := cc.alloc.AllocWithLen(4, 5+len(filePath))
	data = append(data, mysql.LocalInFileHeader)
	data = append(data, filePath...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

// getDataFromPath gets file contents from file path.
func (cc *clientConn) getDataFromPath(ctx context.Context, path string) ([]byte, error) {
	err := cc.writeReq(ctx, path)
	if err != nil {
		return nil, err
	}
	var prevData, curData []byte
	for {
		curData, err = cc.readPacket()
		if err != nil && terror.ErrorNotEqual(err, io.EOF) {
			return nil, err
		}
		if len(curData) == 0 {
			break
		}
		prevData = append(prevData, curData...)
	}
	return prevData, nil
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// There is a special query `load data` that does not return result, which is handled differently.
// Query `load stats` does not return result either.
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {
	// Now just simply call `writeOK`. 
	// In the subsequent lab, we will gradually improve the logic here.
	return cc.writeOK(ctx)
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.setBufferedReadConn(cc.bufReadConn)
	}
}
