package client_test

import (
	"context"
	"fmt"
	"github.com/zhan3333/kystore/client"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
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

	if val, err := cli.Del(context.Background(), "key").Result(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", val)
	}
}

func TestList(t *testing.T) {
	start := time.Now()
	defer func() {
		t.Logf("used: %s", time.Since(start))
	}()
	t.Run("llen", func(t *testing.T) {
		if val, err := cli.LLen(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 0, val)
		}
	})

	t.Run("lpush", func(t *testing.T) {
		if val, err := cli.LPush(context.Background(), "lpushkey", "val", "val1").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.Get(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val1,val", val)
		}

		if val, err := cli.LLen(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 2, val)
		}
	})

	t.Run("lpush two values", func(t *testing.T) {
		// left push
		if val, err := cli.LPush(context.Background(), "lpushkey", "val2", "val3").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.Get(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val3,val2,val1,val", val)
		}
	})

	t.Run("lpop", func(t *testing.T) {
		// left pop
		if val, err := cli.LPop(context.Background(), "lpushkey", 1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val3"}, val)
		}

		if val, err := cli.Get(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val2,val1,val", val)
		}

		if val, err := cli.LLen(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 3, val)
		}
	})

	t.Run("lpop two values", func(t *testing.T) {
		// left pop two values
		if val, err := cli.LPop(context.Background(), "lpushkey", 2).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2", "val1"}, val)
		}

		if val, err := cli.Get(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val", val)
		}
	})

	t.Run("left pop all values", func(t *testing.T) {
		// left pop all values
		if val, err := cli.LPop(context.Background(), "lpushkey", 2).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val"}, val)
		}

		if val, err := cli.Get(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "", val)
		}

		if val, err := cli.LLen(context.Background(), "lpushkey").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 0, val)
		}
	})
}

func TestCmdable_RPush(t *testing.T) {
	t.Run("rpush", func(t *testing.T) {
		var rpushKey = uuid.NewString()
		if val, err := cli.RPush(context.Background(), rpushKey, "val", "val1").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.Get(context.Background(), rpushKey).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val,val1", val)
		}

		if val, err := cli.LLen(context.Background(), rpushKey).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 2, val)
		}
	})
}

func TestExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		key := "existsKey"
		assert.NoError(t, cli.Set(context.Background(), key, "val").Err())

		if val, err := cli.Exists(context.Background(), key).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, true, val)
		}
	})

	t.Run("no exists", func(t *testing.T) {
		key := "noExistsKey"

		if val, err := cli.Exists(context.Background(), key).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, false, val)
		}
	})
}

func TestLRange(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		key := "lrangetest"
		if val, err := cli.LRange(context.Background(), key, 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 0, len(val))
		}
	})

	t.Run("has value list", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val").Err())

		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val"}, val)
		}

		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val2").Err())
		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2", "val"}, val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2"}, val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, 1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2", "val"}, val)
		}
	})
}

func TestCmdable_LTrim(t *testing.T) {
	t.Run("trim empty list", func(t *testing.T) {
		if val, err := cli.LTrim(context.Background(), t.Name(), 0, 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}
	})

	t.Run("trim exists list in range", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val").Err())

		if val, err := cli.LTrim(context.Background(), t.Name(), 0, 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val"}, val)
		}

		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val2").Err())

		if val, err := cli.LTrim(context.Background(), t.Name(), 0, 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2"}, val)
		}
	})

	t.Run("trim end < 0", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val", "val1", "val2").Err())

		if val, err := cli.LTrim(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2", "val1", "val"}, val)
		}

		if val, err := cli.LTrim(context.Background(), t.Name(), 0, -2).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "OK", val)
		}

		if val, err := cli.LRange(context.Background(), t.Name(), 0, -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val2", "val1"}, val)
		}
	})
}

func TestCmdable_LIndex(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		if val, err := cli.LIndex(context.Background(), t.Name(), 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "", val)
		}
	})

	t.Run("out of index", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val").Err())
		if val, err := cli.LIndex(context.Background(), t.Name(), 1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "", val)
		}
	})

	t.Run("index", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val").Err())
		if val, err := cli.LIndex(context.Background(), t.Name(), 0).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val", val)
		}
	})

	t.Run("end index", func(t *testing.T) {
		assert.NoError(t, cli.LPush(context.Background(), t.Name(), "val").Err())
		if val, err := cli.LIndex(context.Background(), t.Name(), -1).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, "val", val)
		}
	})
}

func TestCmdable_SAdd(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		if val, err := cli.SMembers(context.Background(), t.Name()).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, 0, len(val))
		}

		assert.NoError(t, cli.SAdd(context.Background(), t.Name(), "val").Err())

		if val, err := cli.SMembers(context.Background(), t.Name()).Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, []string{"val"}, val)
		}

		assert.NoError(t, cli.SAdd(context.Background(), t.Name(), "val2").Err())

		if val, err := cli.SMembers(context.Background(), t.Name()).Result(); err != nil {
			t.Fatal(err)
		} else {
			sort.Strings(val)
			assert.Equal(t, []string{"val", "val2"}, val)
		}

		if val, err := cli.SIsMember(context.Background(), t.Name(), "val").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, true, val)
		}

		if val, err := cli.SIsMember(context.Background(), t.Name(), "val2").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, true, val)
		}

		if val, err := cli.SIsMember(context.Background(), t.Name(), "val3").Result(); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, false, val)
		}
	})
}
