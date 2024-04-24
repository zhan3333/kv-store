package kvstore

import (
	"fmt"
	"net"
	"strings"
)

const LineSuffix = "\t\n"

type Client struct {
	conn *net.TCPConn
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
	return &Client{conn}, nil
}


func (c *Client) Request(s string) (string, error ){
	if err := c.Send(s); err != nil{
		return "", fmt.Errorf("send failed: %w", err)
	}
	
	if resp, err := c.Recv(); err != nil {
		return "", fmt.Errorf("receive failed: %w", err)
	} else {
		return resp, nil
	}
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