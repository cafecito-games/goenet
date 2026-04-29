package examples_test

import (
	"fmt"

	"github.com/cafecito-games/goenet"
)

func ExampleListen() {
	cfg := goenet.DefaultConfig()
	cfg.PeerCount = 64
	cfg.ChannelLimit = 2

	host, err := goenet.Listen("127.0.0.1:0", cfg)
	if err != nil {
		panic(err)
	}
	defer host.Close()

	fmt.Printf("peers=%d channels=%d\n", host.Config().PeerCount, host.Config().ChannelLimit)
	// Output:
	// peers=64 channels=2
}
