package client_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	strEcho := "Halo"
	serverAddr := "localhost:63790"
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if assert.NoError(t, err) {

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if assert.NoError(t, err) {
			defer conn.Close()
			_, err = conn.Write([]byte(strEcho + "\n"))
			if assert.NoError(t, err) {
				println("write to server = ", strEcho)

				reply := make([]byte, 1024)

				_, err = conn.Read(reply)
				if assert.NoError(t, err) {
					println("reply from server=", string(reply))
				}
			}
		}
	}
}
