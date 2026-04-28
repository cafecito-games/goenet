package peer

import (
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestNewChannelStartsAtZeroSequences(t *testing.T) {
	ch := NewChannel()
	if ch.OutgoingReliableSequenceNumber != 0 {
		t.Fatalf("got %d", ch.OutgoingReliableSequenceNumber)
	}
	if ch.OutgoingUnreliableSequenceNumber != 0 {
		t.Fatalf("got %d", ch.OutgoingUnreliableSequenceNumber)
	}
	if ch.IncomingReliableSequenceNumber != 0 {
		t.Fatalf("got %d", ch.IncomingReliableSequenceNumber)
	}
	if ch.IncomingUnreliableSequenceNumber != 0 {
		t.Fatalf("got %d", ch.IncomingUnreliableSequenceNumber)
	}
}

func TestConfigDefaultsMatchENet(t *testing.T) {
	cfg := goenet.DefaultConfig()
	if cfg.MTU != 1392 {
		t.Fatalf("MTU = %d", cfg.MTU)
	}
	if cfg.MaximumPacketSize != 32*1024*1024 {
		t.Fatalf("MaximumPacketSize = %d", cfg.MaximumPacketSize)
	}
	if cfg.MaximumWaitingData != 32*1024*1024 {
		t.Fatalf("MaximumWaitingData = %d", cfg.MaximumWaitingData)
	}
}
