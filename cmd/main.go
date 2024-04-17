package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

var (
	port = 63790
	host = "0.0.0.0"
)

func main() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()

	log.Printf("Listening on %s:%d", host, port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	log.Printf("Connection from %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		var cmd string
		var err error
		if cmd, err = reader.ReadString('\n'); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("Error reading message: %s", err)
				return
			}
			break
		}
		cmd = strings.TrimSuffix(cmd, "\r\n")
		fmt.Printf("Message incoming: %s\n", cmd)
		handleCommand(conn, cmd)
	}
}

func handleCommand(conn net.Conn, cmd string) {
	if cmd == "ping" {
		conn.Write([]byte("pong\t\n"))
	} else {
		conn.Write([]byte("-ERR unknown command\t\n"))
	}
}
