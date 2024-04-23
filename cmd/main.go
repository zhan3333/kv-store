package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const LineSuffix = "\t\n"

var (
	port  = 63790
	host  = "0.0.0.0"
	store = sync.Map{}
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
		if resp, err := handleCommand(cmd); err != nil {
			conn.Write([]byte(fmt.Sprintf("Error: %s%s", err, LineSuffix)))
		} else {
			if resp == "" {
				resp = "OK"
			}
			conn.Write([]byte(fmt.Sprintf("%s%s", resp, LineSuffix)))
		}
	}
}

func handleCommand(cmd string) (resp string, err error) {
	if cmd == "ping" {
		resp = handlePing()
	} else if strings.HasPrefix(cmd, "set") {
		sp := strings.Split(cmd, " ")
		if len(sp) != 3 {
			return "", errors.New("invalid command")
		}
		key, val := sp[1], sp[2]
		handleSet(key, val)
	} else if strings.HasPrefix(cmd, "get") {
		sp := strings.Split(cmd, " ")
		if len(sp) != 2 {
			return "", errors.New("invalid command")
		}
		key := sp[1]
		resp = handleGet(key)
	} else if cmd == "keys" {
		resp = handleKeys()
	} else {
		return "", errors.New("unknown command")
	}
	return resp, nil
}

func handlePing() string {
	return "pong"
}

func handleGet(key string) string {
	if v, ok := store.Load(key); ok {
		if val, ok := v.(string); ok {
			return val
		} else {
			return ""
		}
	}
	return ""
}

func handleSet(key, value string) {
	store.Store(key, value)
}

func handleKeys() string {
	var keys []string
	store.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return strings.Join(keys, " ")
}