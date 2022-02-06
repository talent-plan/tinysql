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
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"tinysql/protocol"
	"tinysql/util"

	. "github.com/pingcap/check"
)

type ConnTestSuite struct {
}

var _ = Suite(&ConnTestSuite{})

func (ts *ConnTestSuite) simpleClient(c *C) {
	addr := fmt.Sprintf("%s:%d", defaultHost, uint(defaultPort))
	conn, err := net.Dial("tcp", addr)
	bufReadConn := util.NewBufferedReadConn(conn)
	bufWriter := bufio.NewWriterSize(bufReadConn, defaultWriterSize)
	c.Assert(err, IsNil)
	msg := protocol.Message{
		MsgType: protocol.StartupMessageType,
		Content: util.Slice("test"),
	}
	_, err = bufWriter.Write(msg.Encode(nil))
	c.Assert(err, IsNil)
	err = bufWriter.Flush()
	c.Assert(err, IsNil)

	msgLength := make([]byte, 4)
	_, err = io.ReadFull(bufReadConn, msgLength)
	c.Assert(err, IsNil)
	msgLen := binary.BigEndian.Uint32(msgLength)
	c.Assert(msgLen, GreaterEqual, 5)
	msgType := make([]byte, 1)
	_, err = io.ReadFull(bufReadConn, msgType)
	c.Assert(err, IsNil)
	c.Assert(msgType, Equals, protocol.CompleteMessageType)
}

func (ts *ConnTestSuite) TestHandshake(c *C) {
	c.Parallel()
	server, err := NewServer()
	c.Assert(err, IsNil)
	go ts.simpleClient(c)
	conn, err := server.listener.Accept()
	c.Assert(err, IsNil)
	cc := server.newConn(conn)
	ctx := context.WithValue(context.Background(), cc, cc.connectionID)
	c.Assert(ctx, NotNil)
	msg, err := cc.readPacket(ctx)
	c.Assert(err, IsNil)
	c.Assert(msg.MsgType, Equals, protocol.StartupMessageType)
	c.Assert(msg.Content, NotNil)
	completeMsg := protocol.Message{
		MsgType: protocol.CompleteMessageType,
	}

	_, err = cc.bufWriter.Write(completeMsg.Encode(nil))
	c.Assert(err, NotNil)
	err = cc.flush()
	c.Assert(err, NotNil)
	//TODO session has not implement
	cc.OpenSession()
}
