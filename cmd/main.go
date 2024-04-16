package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
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
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading message: %s", err)
			return
		}
		fmt.Printf("Message incoming: %s", string(message))
		conn.Write([]byte("Message received.\n"))
	}
}
