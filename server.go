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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const LineSuffix = "\t\n"

type Server struct {
	addr           string
	store          sync.Map
	backupFile     string
	aofFile        *os.File
	backupInterval time.Duration
	BackupType     BackupType
}

type ServerOptions struct {
	StartedCh      chan struct{}
	Backup         bool
	BackupPath     string
	BackupInterval time.Duration
	BackupType     BackupType
}

type BackupType string

var (
	BackupAOF BackupType = "aof"
	BackupRDB BackupType = "rdb"
)

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
			if options.BackupType == "" {
				options.BackupType = BackupRDB
			}
			if options.BackupType == BackupRDB {
				s.backupFile = fmt.Sprintf("%s/backup-rdb.json", options.BackupPath)
			} else {
				s.backupFile = fmt.Sprintf("%s/backup-aof.txt", options.BackupPath)
			}
			if options.BackupInterval < 1*time.Second {
				options.BackupInterval = 1 * time.Second
			}
			s.backupInterval = options.BackupInterval
			s.BackupType = options.BackupType
			if s.BackupType == BackupRDB {
				s.AsyncBackupRun()
			} else {
				if err := s.openAOFFile(); err != nil {
					return fmt.Errorf("open aof file failed: %w", err)
				}
				if err := s.recoverAOF(); err != nil {
					return fmt.Errorf("recover aof file failed: %w", err)
				}
			}
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

func (s *Server) openAOFFile() error {
	f, err := os.OpenFile(s.backupFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open %s failed: %s", s.backupFile, err)
	}
	s.aofFile = f
	return nil
}

func (s *Server) recoverAOF() error {
	reader := bufio.NewReader(s.aofFile)

	recoverCmdCount := 0
	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read aof file line failed: %w", err)
		}
		if cmd == "" || cmd == "\n" {
			continue
		}

		if _, err := s.handleCommand(cmd, false); err != nil {
			return fmt.Errorf("handle command %s failed: %w", cmd, err)
		}
		recoverCmdCount++
	}

	log.Printf("Recovered %d commands", recoverCmdCount)

	return nil
}

func (s *Server) appendAOF(cmd string) error {
	_, err := s.aofFile.WriteString(fmt.Sprintf("%s\n", cmd))
	if err != nil {
		return fmt.Errorf("append aof failed: %w", err)
	}
	return nil
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
		if resp, err := s.handleCommand(cmd, s.BackupType == BackupAOF); err != nil {
			_, err2 := conn.Write([]byte(fmt.Sprintf("Error: %s%s", err, LineSuffix)))
			if err2 != nil {
				log.Printf("Error writing message: %s", err2)
				return
			}
		} else {
			_, err2 := conn.Write([]byte(fmt.Sprintf("%s%s", resp, LineSuffix)))
			if err2 != nil {
				log.Printf("Error writing message: %s", err2)
				return
			}
		}
	}
}

func (s *Server) handleCommand(c string, aof bool) (resp string, err error) {
	defer func() {
		if err == nil && aof {
			if err := s.appendAOF(c); err != nil {
				log.Printf("appand aof file failed: %s", err)
			}
		}
	}()
	cmd := NewCmd(c)

	switch cmd.Name {
	case "ping":
		resp = s.handlePing()
	case "get":
		if len(cmd.Args) != 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		resp = s.handleGet(cmd.Args[0])
	case "set":
		if len(cmd.Args) < 2 || len(cmd.Args)%2 != 0 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		m := map[string]string{}
		for i := 0; i < len(cmd.Args); i += 2 {
			m[cmd.Args[i]] = cmd.Args[i+1]
		}
		s.handleSet(m)
		resp = "OK"
	case "exists":
		if len(cmd.Args) != 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		resp = s.handleExists(cmd.Args[0])
	case "keys":
		resp = s.handleKeys()
	case "del":
		if len(cmd.Args) < 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		s.handleDel(cmd.Args...)
		resp = "OK"
	case "lpush":
		if len(cmd.Args) < 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		s.handleLPush(cmd.Args[0], cmd.Args[1:]...)
		resp = "OK"
	case "rpush":
		if len(cmd.Args) < 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		s.handleRPush(cmd.Args[0], cmd.Args[1:]...)
		resp = "OK"
	case "lpop":
		if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		var n = 1
		if len(cmd.Args) == 2 {
			n, err = strconv.Atoi(cmd.Args[1])
			if err != nil || n < 1 {
				return "", fmt.Errorf("invalid n value: %s", cmd.FullName)
			}
		}
		resp = s.handleLPop(cmd.Args[0], n)
	case "llen":
		if len(cmd.Args) != 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		resp = s.handleLLen(cmd.Args[0])
	default:
		return "", fmt.Errorf("unknown command: %s", cmd.FullName)
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

func (s *Server) handleSet(m map[string]string) {
	for k, v := range m {
		s.store.Store(k, v)
	}
}

func (s *Server) handleDel(keys ...string) {
	for _, key := range keys {
		s.store.Delete(key)
	}
}

func (s *Server) handleLPush(key string, values ...string) {
	val, _ := s.store.LoadOrStore(key, "")
	valStr := val.(string)

	// reverse values
	for i := 0; i < len(values)/2; i++ {
		values[i], values[len(values)-i-1] = values[len(values)-i-1], values[i]
	}

	if valStr == "" {
		valStr = strings.Join(values, ",")
	} else {
		values = append(values, valStr)
		valStr = strings.Join(values, ",")
	}
	s.store.Store(key, valStr)
}

func (s *Server) handleRPush(key string, values ...string) {
	val, _ := s.store.LoadOrStore(key, "")
	valStr := val.(string)

	if valStr == "" {
		valStr = strings.Join(values, ",")
	} else {
		valStr = fmt.Sprintf("%s,%s", valStr, strings.Join(values, ","))
	}
	s.store.Store(key, valStr)
}

func (s *Server) handleLPop(key string, n int) string {
	val, _ := s.store.LoadOrStore(key, "")
	valStr := val.(string)

	if valStr == "" {
		return ""
	} else {
		values := strings.Split(valStr, ",")
		if len(values) <= n {
			s.store.Store(key, "")
			return strings.Join(values, ",")
		} else {
			s.store.Store(key, strings.Join(values[n:], ","))
			return strings.Join(values[0:n], ",")
		}
	}
}
func (s *Server) handleLLen(key string) string {
	val, _ := s.store.LoadOrStore(key, "")
	valStr := val.(string)
	if len(valStr) == 0 {
		return "0"
	}

	return strconv.Itoa(len(strings.Split(valStr, ",")))
}

func (s *Server) handleKeys() string {
	var keys []string
	s.store.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (s *Server) handleExists(key string) string {
	_, ok := s.store.Load(key)
	if ok {
		return "true"
	} else {
		return "false"
	}
}
