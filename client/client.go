package client

import (
	"context"
	"github.com/zhan3333/kystore"
	"net"
	"strings"
)

type Client struct {
	conn *net.TCPConn
	cmdable
}

func NewClient(serverAddr string) (*Client, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	cli := &Client{conn: conn}
	cli.cmdable = cli.process
	return cli, nil
}

func (c *Client) process(ctx context.Context, cmd Cmder) error {
	if err := send(c.conn, cmd.String()); err != nil {
		cmd.SetErr(err)
		return err
	}

	if resp, err := receive(c.conn); err != nil {
		cmd.SetErr(err)
		return err
	} else {
		cmd.setReplay(resp)
		return nil
	}
}

func send(conn *net.TCPConn, s string) error {
	_, err := conn.Write([]byte(s + kvstore.LineSuffix))
	return err
}

func receive(conn *net.TCPConn) (string, error) {
	reply := make([]byte, 1024)

	if n, err := conn.Read(reply); err != nil {
		return "", err
	} else {
		return strings.TrimSuffix(string(reply[:n]), kvstore.LineSuffix), nil
	}
}
