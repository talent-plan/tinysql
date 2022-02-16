package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"tinysql/protocol"
	"tinysql/util"
)

const defaultWriterSize = 16 * 1024

type Client struct {
	host   string
	port   uint
	dbname string
	user   string
	reader *bufio.Reader

	bufWriter   *bufio.Writer
	bufReadConn *util.BufferedReadConn
}

func NewClient() (*Client, error) {
	p, err := strconv.Atoi(*port)
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", *host, uint(p))

	conn, err := net.Dial("tcp", addr)

	bufReadConn := util.NewBufferedReadConn(conn)
	bufWriter := bufio.NewWriterSize(bufReadConn, defaultWriterSize)

	if err != nil {
		return nil, err
	}

	return &Client{
		host:   *host,
		port:   uint(p),
		user:   *user,
		reader: bufio.NewReader(os.Stdin),

		bufReadConn: bufReadConn,
		bufWriter:   bufWriter,
	}, nil
}

func (c *Client) Close() {
	c.bufReadConn.Close()
}

// SendMessage send message to server
// msgType only support QueryMessageType byte = 'Q',
// ExitMessageType byte = 'E' and StartupMessageType byte = 'S'.
func (c *Client) SendMessage(content string, msgType byte) (err error) {

	switch msgType {
	case protocol.ExitMessageType:
		msg := protocol.Message{
			MsgType: msgType,
		}
		_, err = c.bufWriter.Write(msg.Encode(nil))
	case protocol.QueryMessageType:
		msg := protocol.Message{
			MsgType: msgType,
			Content: util.Slice(content),
		}
		_, err = c.bufWriter.Write(msg.Encode(nil))
	case protocol.StartupMessageType:
		msg := protocol.Message{
			MsgType: msgType,
			Content: util.Slice(content),
		}
		_, err = c.bufWriter.Write(msg.Encode(nil))
	default:
		return errors.New("don't support this message type: " + string(msgType))
	}

	if err != nil {
		return err
	}

	return c.flush()
}

func (c Client) ReceiveMessage() (*protocol.Message, error) {
	msgLength := make([]byte, 4)
	if _, err := io.ReadFull(c.bufReadConn, msgLength); err != nil {
		return nil, err
	}

	msgLen := binary.BigEndian.Uint32(msgLength)

	msgType := make([]byte, 1)

	if _, err := io.ReadFull(c.bufReadConn, msgType); err != nil {
		return nil, err
	}

	msgContent := make([]byte, msgLen-5)

	if _, err := io.ReadFull(c.bufReadConn, msgContent); err != nil {
		return nil, err
	}

	return &protocol.Message{
		MsgType:   msgType[0],
		MsgLength: msgLen,
		Content:   msgContent,
	}, nil
}

func (c *Client) flush() error {
	return c.bufWriter.Flush()
}

func (c *Client) Handshake() error {
	if err := c.SendMessage(*user, protocol.StartupMessageType); err != nil {
		return err
	}

	if err := c.flush(); err != nil {
		return err
	}

	reply, err := c.ReceiveMessage()
	if err != nil {
		return err
	}

	if reply.MsgType != protocol.CompleteMessageType {
		return errors.New("failed to handshake: not receive complete messages")
	}

	return nil
}
