package kvstore_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kvstore "github.com/zhan3333/kystore"
)

var (
	serverAddr = "localhost:63790"
	cli        *kvstore.Client
)

func TestMain(m *testing.M) {
	var err error
	cli, err = kvstore.NewClient(serverAddr)
	if err != nil {
		panic(err)
	}
	m.Run()
}

func TestPing(t *testing.T) {
	if resp, err := cli.Request("ping"); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "pong", resp)
	}
}

func TestSetGet(t *testing.T) {
	start := time.Now()
	defer func() {
		t.Logf("used: %s", time.Since(start))
	}()
	if resp, err := cli.Request("set key value"); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", resp)
	}

	if resp, err := cli.Request("get key"); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "value", resp)
	}

	if resp, err := cli.Request("keys"); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "key", resp)
	}
}
