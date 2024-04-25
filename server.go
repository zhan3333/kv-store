package kvstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const LineSuffix = "\t\n"

type Server struct {
	addr  string
	store sync.Map
}

func New(addr string) *Server {
	return &Server{addr: addr, store: sync.Map{}}
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("new listen failed: %w", err)
	}

	defer func() { _ = listener.Close() }()

	log.Printf("Server started at %s", s.addr)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				return fmt.Errorf("accept connection failed: %w", err)
			}

			go s.handleLine(conn)
		}
	}
}

func (s *Server) handleLine(conn net.Conn) {
	defer func() { _ = conn.Close() }()

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
		if resp, err := s.handleCommand(cmd); err != nil {
			_, err2 := conn.Write([]byte(fmt.Sprintf("Error: %s%s", err, LineSuffix)))
			if err2 != nil {
				log.Printf("Error writing message: %s", err2)
				return
			}
		} else {
			if resp == "" {
				resp = "OK"
			}
			_, err2 := conn.Write([]byte(fmt.Sprintf("%s%s", resp, LineSuffix)))
			if err2 != nil {
				log.Printf("Error writing message: %s", err2)
				return
			}
		}
	}
}

func (s *Server) handleCommand(cmd string) (resp string, err error) {
	if cmd == "ping" {
		resp = s.handlePing()
	} else if strings.HasPrefix(cmd, "set") {
		sp := strings.Split(cmd, " ")
		if len(sp) != 3 {
			return "", errors.New("invalid command")
		}
		key, val := sp[1], sp[2]
		s.handleSet(key, val)
	} else if strings.HasPrefix(cmd, "get") {
		sp := strings.Split(cmd, " ")
		if len(sp) != 2 {
			return "", errors.New("invalid command")
		}
		key := sp[1]
		resp = s.handleGet(key)
	} else if cmd == "keys" {
		resp = s.handleKeys()
	} else {
		return "", errors.New("unknown command")
	}
	return resp, nil
}

func (s *Server) handlePing() string {
	return "pong"
}

func (s *Server) handleGet(key string) string {
	if v, ok := s.store.Load(key); ok {
		if val, ok := v.(string); ok {
			return val
		} else {
			return ""
		}
	}
	return ""
}

func (s *Server) handleSet(key, value string) {
	s.store.Store(key, value)
}

func (s *Server) handleKeys() string {
	var keys []string
	s.store.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return strings.Join(keys, " ")
}
