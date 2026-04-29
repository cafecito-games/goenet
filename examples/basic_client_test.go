package examples_test

import (
	"context"
	"fmt"

	"github.com/cafecito-games/goenet"
)

func ExampleNewHost_connect() {
	host, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		panic(err)
	}
	defer host.Close()

	peer, err := host.Connect("127.0.0.1:9000", 1, 0xCAFE)
	if err != nil {
		panic(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		panic(err)
	}

	fmt.Printf("state=%d\n", peer.State())
	// Output:
	// state=1
}
