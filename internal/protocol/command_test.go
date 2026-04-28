package protocol_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestConnectCommandMatchesGolden(t *testing.T) {
	// connect.bin is an ENetProtocolConnect payload with:
	// command=CONNECT|ACKNOWLEDGE, channelID=0xff, reliableSequenceNumber=3,
	// outgoingPeerID=7, incomingSessionID=1, outgoingSessionID=2,
	// mtu=1400, windowSize=32768, channelCount=2,
	// incomingBandwidth=60000, outgoingBandwidth=30000,
	// packetThrottleInterval=5000, packetThrottleAcceleration=2,
	// packetThrottleDeceleration=3, connectID=0xdeadbeef encoded raw as efbeadde,
	// data=0x10203040.
	wire, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol", "connect.bin"))
	if err != nil {
		t.Fatal(err)
	}

	if got, want := wire[40:44], []byte{0xef, 0xbe, 0xad, 0xde}; !bytes.Equal(got, want) {
		t.Fatalf("connectID bytes = %x, want %x", got, want)
	}

	cmd, flags, n, err := protocol.ParseCommand(wire)
	if err != nil {
		t.Fatal(err)
	}
	if flags != protocol.CommandFlagAcknowledge {
		t.Fatalf("flags = 0x%02x, want 0x%02x", flags, protocol.CommandFlagAcknowledge)
	}
	if n != len(wire) {
		t.Fatalf("consumed = %d, want %d", n, len(wire))
	}

	connect, ok := cmd.(protocol.Connect)
	if !ok {
		t.Fatalf("command type = %T", cmd)
	}

	if connect.ChannelID != 0xff ||
		connect.ReliableSequenceNumber != 3 ||
		connect.OutgoingPeerID != 7 ||
		connect.IncomingSessionID != 1 ||
		connect.OutgoingSessionID != 2 ||
		connect.MTU != 1400 ||
		connect.WindowSize != 32768 ||
		connect.ChannelCount != 2 ||
		connect.IncomingBandwidth != 60000 ||
		connect.OutgoingBandwidth != 30000 ||
		connect.PacketThrottleInterval != 5000 ||
		connect.PacketThrottleAcceleration != 2 ||
		connect.PacketThrottleDeceleration != 3 ||
		connect.ConnectID != 0xdeadbeef ||
		connect.Data != 0x10203040 {
		t.Fatalf("connect mismatch: %+v", connect)
	}

	if got := connect.MarshalBinary(nil); !bytes.Equal(got, wire) {
		t.Fatalf("connect marshal mismatch: %x != %x", got, wire)
	}
}

func TestAcknowledgeCommandMatchesGolden(t *testing.T) {
	// ack.bin is an ENetProtocolAcknowledge payload with:
	// command=ACKNOWLEDGE, channelID=0x02, reliableSequenceNumber=5,
	// receivedReliableSequenceNumber=4, receivedSentTime=1234.
	wire, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol", "ack.bin"))
	if err != nil {
		t.Fatal(err)
	}

	cmd, flags, n, err := protocol.ParseCommand(wire)
	if err != nil {
		t.Fatal(err)
	}
	if flags != 0 {
		t.Fatalf("flags = 0x%02x, want 0x00", flags)
	}
	if n != len(wire) {
		t.Fatalf("consumed = %d, want %d", n, len(wire))
	}

	ack, ok := cmd.(protocol.Acknowledge)
	if !ok {
		t.Fatalf("command type = %T", cmd)
	}

	if ack.ChannelID != 0x02 || ack.ReliableSequenceNumber != 5 || ack.ReceivedReliableSequenceNumber != 4 || ack.ReceivedSentTime != 1234 {
		t.Fatalf("ack mismatch: %+v", ack)
	}

	if got := ack.MarshalBinary(nil); !bytes.Equal(got, wire) {
		t.Fatalf("ack marshal mismatch: %x != %x", got, wire)
	}
}
