package goenet_test

import (
	"context"
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestListenReturnsUsableHost(t *testing.T) {
	host, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 4, ChannelLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if host.Config().PeerCount != 4 {
		t.Fatalf("peer count = %d, want 4", host.Config().PeerCount)
	}
}

func TestNewHostReturnsClientCapableHost(t *testing.T) {
	host, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if host.Config().ChannelLimit != 1 {
		t.Fatalf("channel limit = %d, want 1", host.Config().ChannelLimit)
	}
}

func TestCloseMakesFurtherOperationsFail(t *testing.T) {
	host, err := goenet.Listen("127.0.0.1:0", goenet.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err == nil {
		t.Fatal("expected flush after close to fail")
	}
}
