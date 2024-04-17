package client_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	ping := "ping"
	serverAddr := "localhost:63790"
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if assert.NoError(t, err) {

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if assert.NoError(t, err) {

			defer conn.Close()
			_, err = conn.Write([]byte(ping + "\r\n"))
			if assert.NoError(t, err) {
				println("write to server = ", ping)

				reply := make([]byte, 1024)

				if n, err := conn.Read(reply); err != nil {
					assert.NoError(t, err)
				} else {
					resp := string(reply[:n])
					println("reply from server=", resp)
					assert.Equal(t, "pong\t\n", resp)
				}
			}
		}
	}
}
