package goenet_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cafecito-games/goenet"
)

// TestHostConcurrentServiceAndOps verifies the documented "Host is safe for
// concurrent use" contract: a Service goroutine and several other goroutines
// each calling Send/Connect/State/BandwidthLimit at once must not race or panic.
func TestHostConcurrentServiceAndOps(t *testing.T) {
	t.Parallel()

	host, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 4, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = host.Close()
	}()

	client, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	peer, err := client.Connect(host.LocalAddr().String(), 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			_, _ = host.Service(context.Background(), 5*time.Millisecond)
			_, _ = client.Service(context.Background(), 5*time.Millisecond)
			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_ = peer.State()
				_ = peer.Send(0, &goenet.Packet{Data: []byte("ping"), Flags: goenet.PacketFlagReliable})
				_ = client.BandwidthLimit(0, 0)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
}
