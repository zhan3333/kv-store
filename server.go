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
		keys := s.handleKeys()
		resp = strings.Join(keys, ",")
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
		if err := s.handleLPush(cmd.Args[0], cmd.Args[1:]...); err != nil {
			return "", err
		}
		resp = "OK"
	case "rpush":
		if len(cmd.Args) < 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		if err := s.handleRPush(cmd.Args[0], cmd.Args[1:]...); err != nil {
			return "", err
		}
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
		if values, err := s.handleLPop(cmd.Args[0], n); err != nil {
			return "", err
		} else {
			resp = strings.Join(values, ",")
		}
	case "llen":
		if len(cmd.Args) != 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		if l, err := s.handleLLen(cmd.Args[0]); err != nil {
			return "", err
		} else {
			resp = strconv.Itoa(int(l))
		}
	case "lrange":
		if len(cmd.Args) != 3 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		key := cmd.Args[0]
		start, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			return "", fmt.Errorf("invalid start value: %s", cmd.Args[0])
		}
		stop, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			return "", fmt.Errorf("invalid stop value: %s", cmd.Args[1])
		}
		if l, err := s.handleLRange(key, start, stop); err != nil {
			return "", err
		} else {
			resp = strings.Join(l, ",")
		}
	case "ltrim":
		if len(cmd.Args) != 3 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		key := cmd.Args[0]
		start, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			return "", fmt.Errorf("invalid start value: %s", cmd.Args[0])
		}
		stop, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			return "", fmt.Errorf("invalid stop value: %s", cmd.Args[1])
		}
		if err := s.handleLTrim(key, start, stop); err != nil {
			return "", err
		} else {
			resp = "OK"
		}
	case "lindex":
		if len(cmd.Args) != 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		key := cmd.Args[0]
		index, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			return "", fmt.Errorf("invalid index value: %s", cmd.Args[0])
		}
		if val, err := s.handleLIndex(key, index); err != nil {
			return "", err
		} else {
			resp = val
		}
	case "sadd":
		if len(cmd.Args) < 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		if err := s.handleSAdd(cmd.Args[0], cmd.Args[1:]...); err != nil {
			return "", err
		}
		resp = "OK"
	case "smembers":
		if len(cmd.Args) != 1 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		key := cmd.Args[0]

		if l, err := s.handleLSMembers(key); err != nil {
			return "", err
		} else {
			resp = strings.Join(l, ",")
		}
	case "sismember":
		if len(cmd.Args) != 2 {
			return "", fmt.Errorf("invalid args number: %s", cmd.FullName)
		}
		key := cmd.Args[0]
		val := cmd.Args[1]

		if b, err := s.handleLSIsMember(key, val); err != nil {
			return "", err
		} else {
			if b {
				resp = "true"
			} else {
				resp = "false"
			}
		}
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
		switch v.(type) {
		case string:
			return v.(string)
		case *List:
			return strings.Join(v.(*List).Values, ",")
		default:
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

func (s *Server) handleLPush(key string, values ...string) error {
	raw, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := raw.(*List); ok {
		val.LPush(values...)
		return nil
	} else {
		return fmt.Errorf("invalid list type: %T", raw)
	}
}

func (s *Server) handleRPush(key string, values ...string) error {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		val.Values = append(val.Values, values...)
		return nil
	} else {
		return fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleLPop(key string, n int) ([]string, error) {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		if len(val.Values) <= n {
			values := val.Values
			val.Values = []string{}
			return values, nil
		} else {
			values := val.Values[:n]
			val.Values = val.Values[n:]
			return values, nil
		}
	} else {
		return nil, fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleLRange(key string, start int, stop int) ([]string, error) {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		if len(val.Values) == 0 {
			return []string{}, nil
		}
		if start >= len(val.Values) {
			return []string{}, nil
		}
		if stop > len(val.Values)-1 {
			stop = len(val.Values) - 1
		}
		if stop < 0 {
			stop = len(val.Values) + stop
		}
		return val.Values[start : stop+1], nil
	} else {
		return nil, fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleLTrim(key string, start int, stop int) error {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		if len(val.Values) == 0 {
			return nil
		}
		if start >= len(val.Values) {
			val.Values = []string{}
			return nil
		}
		if stop > len(val.Values)-1 {
			stop = len(val.Values) - 1
		}
		if stop < 0 {
			stop = len(val.Values) + stop
		}
		val.Values = val.Values[start : stop+1]
		return nil
	} else {
		return fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleLIndex(key string, index int) (string, error) {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		if len(val.Values) == 0 {
			return "", nil
		}
		if index > len(val.Values)-1 {
			return "", nil
		}
		if index < 0 {
			index = len(val.Values) + index
		}
		return val.Values[index], nil
	} else {
		return "", fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleLLen(key string) (int64, error) {
	val, _ := s.store.LoadOrStore(key, &List{})
	if val, ok := val.(*List); ok {
		return int64(len(val.Values)), nil
	} else {
		return 0, fmt.Errorf("invalid list type: %T", val)
	}
}

func (s *Server) handleKeys() []string {
	var keys []string
	s.store.Range(func(key, _ any) bool {
		keys = append(keys, key.(string))
		return true
	})
	sort.Strings(keys)
	return keys
}

func (s *Server) handleExists(key string) string {
	_, ok := s.store.Load(key)
	if ok {
		return "true"
	} else {
		return "false"
	}
}

func (s *Server) handleSAdd(key string, values ...string) error {
	raw, _ := s.store.LoadOrStore(key, &Set{Map: map[string]bool{}})
	if val, ok := raw.(*Set); ok {
		val.Add(values...)
		return nil
	} else {
		return fmt.Errorf("invalid set type: %T", raw)
	}
}

func (s *Server) handleLSMembers(key string) ([]string, error) {
	raw, _ := s.store.LoadOrStore(key, &Set{Map: map[string]bool{}})
	if val, ok := raw.(*Set); ok {
		var keys []string
		for k := range val.Map {
			keys = append(keys, k)
		}
		return keys, nil
	} else {
		return nil, fmt.Errorf("invalid set type: %T", raw)
	}
}

func (s *Server) handleLSIsMember(key string, val string) (bool, error) {
	raw, _ := s.store.LoadOrStore(key, &Set{Map: map[string]bool{}})
	if set, ok := raw.(*Set); ok {
		return set.Has(val), nil
	} else {
		return false, fmt.Errorf("invalid set type: %T", raw)
	}
}
