package examples_test

import (
	"context"
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestPublicAPIListenAndConnectSmoke(t *testing.T) {
	server, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	client, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	peer, err := client.Connect("127.0.0.1:9000", 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != goenet.PeerStateConnecting {
		t.Fatalf("peer state = %d, want %d", got, goenet.PeerStateConnecting)
	}
}
