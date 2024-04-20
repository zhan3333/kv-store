package client_test

import (
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const LineSuffix = "\t\n"

var (
	serverAddr = "localhost:63790"
	cli *Client
)

type Client struct {
	conn *net.TCPConn
}

func NewClient() (*Client, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Client{conn}, nil
}

func TestMain(m *testing.M) {
	var err error
	cli, err = NewClient()
	if err != nil {
		panic(err)
	}
	m.Run()
}

func (c *Client) Send(s string) error {
	_, err := c.conn.Write([]byte(s + LineSuffix))
	return err
}

func (c *Client) Recv() (string, error) {
	reply := make([]byte, 1024)

	if n, err := c.conn.Read(reply); err != nil {
		return "", err
	} else {
		return strings.TrimSuffix(string(reply[:n]), LineSuffix), nil
	}
}

func TestPing(t *testing.T) {
	ping := "ping"

	if err := cli.Send(ping); err != nil {
		t.Fatal(err)
	}

	if resp, err := cli.Recv(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "pong", resp)
	}
}

func TestSet(t *testing.T) {
	set := "set key value"

	if err := cli.Send(set); err != nil {
		t.Fatal(err)
	}

	if resp, err := cli.Recv(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", resp)
	}
}

func TestGet(t *testing.T) {
	// set value
	set := "set key value"

	if err := cli.Send(set); err != nil {
		t.Fatal(err)
	}

	if resp, err := cli.Recv(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", resp)
	}


	// get value
	get := "get key"

	if err := cli.Send(get); err != nil {
		t.Fatal(err)
	}

	if resp, err := cli.Recv(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "value", resp)
	}
}