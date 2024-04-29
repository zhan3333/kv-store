package main

import (
	"context"
	"fmt"
	kvstore "github.com/zhan3333/kystore"
	"time"
)

var (
	port = 63790
	host = "0.0.0.0"
)

func main() {
	server := kvstore.New(fmt.Sprintf("%s:%d", host, port))
	if err := server.Run(context.Background(), &kvstore.ServerOptions{Backup: true, BackupInterval: 5 * time.Second}); err != nil {
		fmt.Printf("Error: %s\n", err)
	} else {
		fmt.Println("Server stopped")
	}
}
