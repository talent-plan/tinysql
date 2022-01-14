.PHONY: all client clean

all: fmt client

client: main.go client.go
	GO111MODULE=on go build -o ../bin/client main.go client.go