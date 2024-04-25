package main

import (
	"context"
	"fmt"
	kvstore "github.com/zhan3333/kystore"
)

var (
	port = 63790
	host = "0.0.0.0"
)

func main() {
	server := kvstore.New(fmt.Sprintf("%s:%d", host, port))
	if err := server.Run(context.Background()); err != nil {
		fmt.Printf("Error: %s\n", err)
	} else {
		fmt.Println("Server stopped")
	}
}
