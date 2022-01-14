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
	"fmt"
	"go.uber.org/zap"
	"log"
	"net"
	"sync"
)

var (
	baseConnID uint32
)

const (
	defaultHost = "127.0.0.1"
	defaultPort = 4001
)

type Server struct {
	host     string
	port     uint
	rwlock   sync.RWMutex
	clients  map[uint32]*clientConn
	listener net.Listener
}

func NewServer() (*Server, error) {
	addr := fmt.Sprintf("%v:%v", defaultHost, defaultPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{
		host:     defaultHost,
		port:     uint(defaultPort),
		listener: listener,
		clients:  make(map[uint32]*clientConn),
	}, nil
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
			log.Print("failed to accept client conn", zap.Error(err))
			errChan <- err
			return
		}

		clientConn := s.newConn(conn)
		go s.onConn(clientConn)
	}
}

func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	cc.setConn(conn)
	return cc
}

func (s *Server) onConn(cc *clientConn) {
	ctx := context.WithValue(context.Background(), cc, cc.connectionID)
	if err := cc.handshake(ctx); err != nil {
		log.Printf("failed to handshake with client: %s\n", err.Error())
		return
	}

	s.rwlock.Lock()
	s.clients[cc.connectionID] = cc
	s.rwlock.Unlock()

	cc.Run(ctx)
}
