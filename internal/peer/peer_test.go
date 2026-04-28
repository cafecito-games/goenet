package peer

import (
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestPeerStateOrdinalsMatchENet(t *testing.T) {
	states := []goenet.PeerState{
		goenet.PeerStateDisconnected,
		goenet.PeerStateConnecting,
		goenet.PeerStateAcknowledgingConnect,
		goenet.PeerStateConnectionPending,
		goenet.PeerStateConnectionSucceeded,
		goenet.PeerStateConnected,
		goenet.PeerStateDisconnectLater,
		goenet.PeerStateDisconnecting,
		goenet.PeerStateAcknowledgingDisconnect,
		goenet.PeerStateZombie,
	}

	for want, got := range states {
		if uint8(got) != uint8(want) {
			t.Fatalf("state %d = %d", want, got)
		}
	}
}

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
	if got := ch.IncomingReliableCommands.Len(); got != 0 {
		t.Fatalf("IncomingReliableCommands.Len() = %d", got)
	}
	if got := ch.IncomingUnreliableCommands.Len(); got != 0 {
		t.Fatalf("IncomingUnreliableCommands.Len() = %d", got)
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
