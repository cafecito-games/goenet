package examples_test

import (
	"fmt"

	"github.com/cafecito-games/goenet"
)

func Example_basicServerConfig() {
	cfg := goenet.DefaultConfig()
	cfg.PeerCount = 64
	cfg.ChannelLimit = 2

	fmt.Printf("peers=%d channels=%d mtu=%d\n", cfg.PeerCount, cfg.ChannelLimit, cfg.MTU)
	// Output:
	// peers=64 channels=2 mtu=1392
}
