// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"context"
	"flag"
	"net"
	"os"
	"os/user"
	"sync"

	proxyprotocol "github.com/blacktear23/go-proxyprotocol"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"tinysql/config"
	"tinysql/dbterror"
	"tinysql/errno"
	"tinysql/parser/mysql"
	"tinysql/parser/terror"
	"tinysql/util"
	"tinysql/util/fastrand"
	"tinysql/util/logutil"
)

var (
	serverPID   int
	osUser      string
	osVersion   string
	runInGoTest bool
)

func init() {
	serverPID = os.Getpid()
	currentUser, err := user.Current()
	if err != nil {
		osUser = ""
	} else {
		osUser = currentUser.Name
	}
	osVersion = ""
	runInGoTest = flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil
}

var (
	errUnknownFieldType        = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)
	errInvalidSequence         = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)
	errInvalidType             = dbterror.ClassServer.NewStd(errno.ErrInvalidType)
	errNotAllowedCommand       = dbterror.ClassServer.NewStd(errno.ErrNotAllowedCommand)
	errAccessDenied            = dbterror.ClassServer.NewStd(errno.ErrAccessDenied)
	errConCount                = dbterror.ClassServer.NewStd(errno.ErrConCount)
	errSecureTransportRequired = dbterror.ClassServer.NewStd(errno.ErrSecureTransportRequired)
	errMultiStatementDisabled  = dbterror.ClassServer.NewStd(errno.ErrMultiStatementDisabled)
	errNewAbortingConnection   = dbterror.ClassServer.NewStd(errno.ErrNewAbortingConnection)
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

// Server is the MySQL protocol server
type Server struct {
	cfg            *config.Config
	listener       net.Listener
	inShutdownMode bool
	globalConnID   util.GlobalConnID
	rwlock         sync.RWMutex
	clients        map[uint64]*clientConn
	capability     uint32
}

func NewServer() (*Server, error) {
	s := &Server{
		globalConnID: util.GlobalConnID{ServerID: 0, Is64bits: true},
		clients:      make(map[uint64]*clientConn),
	}

	s.capability = defaultCapability

	var err error
	addr := "127.0.0.1:4000"
	tcpProto := "tcp"
	if s.listener, err = net.Listen(tcpProto, addr); err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func (s *Server) Run() error {
	// If error should be reported and exit the server it can be sent on this
	// channel. Otherwise end with sending a nil error to signal "done"
	errChan := make(chan error)
	go s.startNetworkListener(s.listener, errChan)
	err := <-errChan
	if err != nil {
		return err
	}
	return <-errChan

}

func (s *Server) startNetworkListener(listener net.Listener, errChan chan error) {
	if listener == nil {
		errChan <- nil
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					if s.inShutdownMode {
						errChan <- nil
					} else {
						errChan <- err
					}
					return
				}
			}

			// If we got PROXY protocol error, we should continue accept.
			if proxyprotocol.IsProxyProtocolError(err) {
				logutil.BgLogger().Error("PROXY protocol failed", zap.Error(err))
				continue
			}

			logutil.BgLogger().Error("accept failed", zap.Error(err))
			errChan <- err
			return
		}

		clientConn := s.newConn(conn)
		go s.onConn(clientConn)
	}
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(conn *clientConn) {
	ctx := logutil.WithConnID(context.Background(), conn.connectionID)
	if err := conn.handshake(ctx); err != nil {
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we only log errors here.
		terror.Log(errors.Trace(err))
		terror.Log(errors.Trace(conn.Close()))
		return
	}

	logutil.Logger(ctx).Debug("new connection", zap.String("remoteAddr", conn.bufReadConn.RemoteAddr().String()))

	defer func() {
		logutil.Logger(ctx).Debug("connection closed")
	}()
	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	s.rwlock.Unlock()

	conn.Run(ctx)
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	cc.setConn(conn)
	cc.salt = fastrand.Buf(20)
	return cc
}
