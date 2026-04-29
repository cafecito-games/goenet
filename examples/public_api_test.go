package examples_test

import (
	"context"
	"testing"
	"time"

	goenet "github.com/cafecito-games/goenet/pkg"
)

func TestPublicAPIListenAndConnectSmoke(t *testing.T) {
	server, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 4, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()

	client, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()

	peer, err := client.Connect(server.LocalAddr().String(), 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Drive the server so it accepts and verifies the inbound connect.
		if _, err := server.Service(context.Background(), 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Service(context.Background(), 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if peer.State() == goenet.PeerStateConnected {
			return
		}
	}
	t.Fatalf("client never reached PeerStateConnected; last state = %d", peer.State())
}

func TestDefaultConfigSupportsClientConnect(t *testing.T) {
	// Regression for the README quick-start: DefaultConfig must allocate at least
	// one peer slot so a fresh client can call Connect.
	client, err := goenet.NewHost(goenet.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	if _, err := client.Connect("127.0.0.1:9", 1, 0); err != nil {
		t.Fatalf("Connect against DefaultConfig should succeed: %v", err)
	}
}
