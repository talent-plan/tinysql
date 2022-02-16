package protocol

import "encoding/binary"

// Here is the communication protocol standard between client and server,
// you can understand the communication rules between client and server via this file.
// In TinySQL, the communication protocol is our-defined,
// which is simpler than the protocol of MySQL or oracle.

// The message between the client and the server are defined as follows:
// MessageLength(4 bytes and include itself) + MessageType(1 byte) + Content(Unlimited length)
//
// MessageType in Client to Server:
// 'S': Used to establish a connection with the server, Content is user's name.
// 'Q': Used to send query message, and information is, Content is query's statement.
// 'E': Used to exit the connection, Content is empty.
//
// MessageType in Server to Client:
// 'd': Used to return query result, a message represents a row of data, Content is result.
// 'c': Used to represent that the server is ready for query or completed previous query , Content is empty.
// 'e': Used to return an error, Content is error reason.

// Handshake between client and server:
// 1. client send startup message to server.
// 2. server received startup message, and reply complete message.
// 3. client received complete message, then start to query.

const (
	StartupMessageType byte = 'S'
	QueryMessageType   byte = 'Q'
	ExitMessageType    byte = 'E'

	DataMessageType     byte = 'd'
	CompleteMessageType byte = 'c'
	ErrorMessageType    byte = 'e'
)

type Message struct {
	MsgType   byte
	MsgLength uint32
	Content   []byte
}

func (msg Message) Encode(dst []byte) []byte {
	msg.MsgLength = uint32(5 + len(msg.Content))
	wp := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[wp:], msg.MsgLength)

	dst = append(dst, msg.MsgType)

	dst = append(dst, msg.Content...)

	return dst
}
