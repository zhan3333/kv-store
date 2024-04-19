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
	cli, err := NewClient()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

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
	cli, err := NewClient()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if err := cli.Send(set); err != nil {
		t.Fatal(err)
	}

	if resp, err := cli.Recv(); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, "OK", resp)
	}
}