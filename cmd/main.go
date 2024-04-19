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

const LineSuffix = "\t\n"

var (
	port  = 63790
	host  = "0.0.0.0"
	store = map[string]string{}
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
		cmd = strings.TrimSuffix(cmd, LineSuffix)
		fmt.Printf("Message incoming: %s\n", cmd)
		handleCommand(conn, cmd)
	}
}

func handleCommand(conn net.Conn, cmd string) {
	if cmd == "ping" {
		conn.Write([]byte("pong" + LineSuffix))
	} else if strings.HasPrefix(cmd, "set") {
		sp := strings.Split(cmd, " ")
		if len(sp) != 3 {
			conn.Write([]byte("-ERR invalid command" + LineSuffix))
			return
		}
		key, val := sp[1], sp[2]
		store[key] = val
		conn.Write([]byte("OK" + LineSuffix))
		return
	} else {
		conn.Write([]byte("-ERR unknown command" + LineSuffix))
		return
	}
}
