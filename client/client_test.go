package client_test

import (
	"context"
	"fmt"
	"github.com/zhan3333/kystore/client"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kvstore "github.com/zhan3333/kystore"
)

var (
	serverAddr = "localhost:63790"
	cli        *client.Client
)

func TestMain(m *testing.M) {
	// start test server
	server := kvstore.New(serverAddr)
	ctx, cancel := context.WithCancel(context.Background())
	startedCh := make(chan struct{})
	go func() {
		if err := server.Run(ctx, &kvstore.ServerOptions{StartedCh: startedCh}); err != nil {
			panic(err)
		} else {
			fmt.Println("Server stopped")
		}
	}()

	<-startedCh

	// new client
	var err error
	cli, err = client.NewClient(serverAddr)
	if err != nil {
		panic(err)
	}

	m.Run()

	cancel()
}

func TestPing(t *testing.T) {
	if val, err := cli.Ping(context.Background()).Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "pong", val)
	}
}

func TestSetGet(t *testing.T) {
	start := time.Now()
	defer func() {
		t.Logf("used: %s", time.Since(start))
	}()
	if val, err := cli.Set(context.Background(), "key", "val").Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", val)
	}

	if val, err := cli.Set(context.Background(), "key1", "val1", "key2", "val2").Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", val)
	}

	if val, err := cli.Get(context.Background(), "key").Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "val", val)
	}

	if val, err := cli.Keys(context.Background()).Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, []string{"key", "key1", "key2"}, val)
	}
}
