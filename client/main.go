package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"tinysql/protocol"
	"tinysql/util"
)

const (
	nmHost = "H"
	nmPort = "P"
	nmUser = "U"
)

var (
	host = flag.String(nmHost, "127.0.0.1", "tidb server host")
	port = flag.String(nmPort, "4001", "tidb server port")
	user = flag.String(nmUser, "root", "tidb server port")
)

func main() {
	flag.Parse()

	client, err := NewClient()

	if err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
		os.Exit(1)
	}

	if err := client.Handshake(); err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
		os.Exit(1)
	}

	fmt.Fprintln(os.Stdout, "successfully connected to client")

	OnCmd(client)
	return
}

func OnCmd(client *Client) {
	defer client.Close()

	for {
		fmt.Printf("tinysql > ")
		str, err := client.reader.ReadString(';')

		if err != nil {
			fmt.Fprintln(os.Stdout, err.Error())
			continue
		}

		content := strings.ToLower(strings.Trim(str, "\r\n"))

		if strings.EqualFold(content, "exit;") {
			client.SendMessage("", protocol.ExitMessageType)
			os.Exit(0)
		}

		err = client.SendMessage(content, protocol.QueryMessageType)
		if err != nil {
			fmt.Fprintln(os.Stdout, err.Error())
			continue
		}

		reply, err := client.ReceiveMessage()
		if err != nil {
			fmt.Fprintln(os.Stdout, err.Error())
			continue
		}

		fmt.Fprintln(os.Stdout, util.String(reply.Content))
	}
}
