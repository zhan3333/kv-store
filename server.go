package kvstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const LineSuffix = "\t\n"

type Server struct {
	addr           string
	store          sync.Map
	backupFile     string
	backupInterval time.Duration
}

type ServerOptions struct {
	StartedCh      chan struct{}
	Backup         bool
	BackupPath     string
	BackupInterval time.Duration
}

func New(addr string) *Server {
	return &Server{addr: addr, store: sync.Map{}}
}

func (s *Server) Run(ctx context.Context, options *ServerOptions) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("new listen failed: %w", err)
	}
	if options != nil {
		if options.StartedCh != nil {
			options.StartedCh <- struct{}{}
		}
		if options.Backup {
			if options.BackupPath == "" {
				options.BackupPath = "."
			}
			s.backupFile = fmt.Sprintf("%s/backup.json", options.BackupPath)
			if options.BackupInterval < 1*time.Second {
				options.BackupInterval = 1 * time.Second
			}
			s.backupInterval = options.BackupInterval
			s.AsyncBackupRun()
		}
	}

	defer func() { _ = listener.Close() }()

	log.Printf("Server started at %s", s.addr)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept connection failed: %w", err)
		}

		go s.handleLine(conn)
	}
}

func (s *Server) ReadBackup() error {
	f, err := os.OpenFile(s.backupFile, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open backup.json failed: %s", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read backup.json failed: %s", err)
	}
	if len(b) > 0 {
		store := map[string]any{}
		err = json.Unmarshal(b, &store)
		if err != nil {
			return fmt.Errorf("unmarshal backup.json failed: %s", err)
		}
		for k, v := range store {
			s.store.Store(k, v)
		}
	}
	return nil
}

func (s *Server) WriteBackup() error {
	f, err := os.OpenFile(s.backupFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open backup.json failed: %s", err)
	}
	defer func() { _ = f.Close() }()
	store := map[string]any{}
	s.store.Range(func(k, v any) bool {
		store[k.(string)] = v
		return true
	})
	b, err := json.Marshal(store)
	if err != nil {
		return fmt.Errorf("marshal store failed: %s", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("write backup.json failed: %s", err)
	}
	return nil
}

func (s *Server) AsyncBackupRun() {
	// load from backup
	if err := s.ReadBackup(); err != nil {
		log.Printf("read backup failed: %s", err)
	}
	go func() {
		t := time.NewTicker(s.backupInterval)
		for {
			<-t.C
			//fmt.Println("backup...")
			if err := s.WriteBackup(); err != nil {
				log.Printf("async backup failed: %s", err)
			}
			//fmt.Println("backup done")
		}
	}()
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