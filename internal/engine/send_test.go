package engine

import (
	"context"
	"encoding/binary"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet"
	ipeer "github.com/cafecito-games/goenet/internal/peer"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
	isocket "github.com/cafecito-games/goenet/internal/socket"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestReliableSendQueuesAcknowledgeableCommand(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	packet := &goenet.Packet{Data: []byte("abc"), Flags: goenet.PacketFlagReliable}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}
	if got := peer.SentReliableCount(); got != 1 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if sock.WriteCount() != 0 {
		t.Fatalf("writes before flush = %d", sock.WriteCount())
	}

	cmd := peer.mustOutgoingSendReliable(t)
	if cmd.Command.Header.Command != iprotocol.CommandSendReliable {
		t.Fatalf("command = %v", cmd.Command.Header.Command)
	}
	if cmd.Command.Header.Flags != iprotocol.CommandFlagAcknowledge {
		t.Fatalf("flags = 0x%02x", cmd.Command.Header.Flags)
	}
	if cmd.ReliableSequenceNumber != 1 {
		t.Fatalf("reliable sequence = %d", cmd.ReliableSequenceNumber)
	}
	if cmd.UnreliableSequenceNumber != 0 {
		t.Fatalf("unreliable sequence = %d", cmd.UnreliableSequenceNumber)
	}
}

func TestUnreliableSendDoesNotAdvanceReliableCounters(t *testing.T) {
	host, _ := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	packet := &goenet.Packet{Data: []byte("abc")}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}

	if got := peer.SentReliableCount(); got != 0 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if got := peer.OutgoingCount(); got != 1 {
		t.Fatalf("OutgoingCount = %d", got)
	}

	cmd := peer.mustOutgoing(t)
	if cmd.Command.Header.Command != iprotocol.CommandSendUnreliable {
		t.Fatalf("command = %v", cmd.Command.Header.Command)
	}
	if cmd.Command.Header.Flags != 0 {
		t.Fatalf("flags = 0x%02x", cmd.Command.Header.Flags)
	}
	if cmd.ReliableSequenceNumber != 0 {
		t.Fatalf("reliable sequence = %d", cmd.ReliableSequenceNumber)
	}
	if cmd.UnreliableSequenceNumber != 1 {
		t.Fatalf("unreliable sequence = %d", cmd.UnreliableSequenceNumber)
	}
	if got := peer.Raw.Channels[0].OutgoingReliableSequenceNumber; got != 0 {
		t.Fatalf("channel reliable sequence = %d", got)
	}
}

func TestFlushWritesOnlyWhenCommandsAreQueued(t *testing.T) {
	host, sock := newTestHost(t)

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("writes after empty flush = %d", got)
	}

	peer := &testPeer{Raw: host.MustConnectedPeer()}
	packet := &goenet.Packet{Data: []byte("abc"), Flags: goenet.PacketFlagReliable}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("writes after queued flush = %d", got)
	}
}

func TestFlushMovesReliableCommandsInFlightWithWireMetadata(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	packet := &goenet.Packet{Data: []byte("abc"), Flags: goenet.PacketFlagReliable}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := peer.SentReliableCount(); got != 0 {
		t.Fatalf("queued reliable count = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 1 {
		t.Fatalf("in-flight reliable count = %d", got)
	}

	cmd := peer.mustInFlightReliable(t)
	if cmd.SendAttempts != 1 {
		t.Fatalf("send attempts = %d", cmd.SendAttempts)
	}
	if cmd.SentTime != 77 {
		t.Fatalf("sent time = %d", cmd.SentTime)
	}
	if cmd.RoundTripTimeout != 500 {
		t.Fatalf("round trip timeout = %d", cmd.RoundTripTimeout)
	}

	write := sock.MustWrite(t, 0)
	header, err := iprotocol.ParseHeader(write.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if header.Flags != iprotocol.HeaderFlagSentTime {
		t.Fatalf("header flags = 0x%04x", header.Flags)
	}
	if header.SentTime != 77 {
		t.Fatalf("header sent time = %d", header.SentTime)
	}

	if got := write.Payload[4]; iprotocol.Command(got&byte(iprotocol.CommandMask)) != iprotocol.CommandSendReliable {
		t.Fatalf("wire command = 0x%02x", got)
	}
	if got := write.Payload[5]; got != 0 {
		t.Fatalf("wire channel id = %d", got)
	}
	if got := binary.BigEndian.Uint16(write.Payload[6:8]); got != 1 {
		t.Fatalf("wire reliable sequence = %d", got)
	}
	if got := binary.BigEndian.Uint16(write.Payload[8:10]); got != uint16(len(packet.Data)) {
		t.Fatalf("wire data length = %d", got)
	}
	if got := string(write.Payload[10:]); got != "abc" {
		t.Fatalf("wire payload = %q", got)
	}
}

func newTestHost(t *testing.T) (*Host, *testsupport.FakeSocket) {
	t.Helper()

	addr, err := goenet.NewAddress(netip.MustParseAddrPort("127.0.0.1:9001"), 0)
	if err != nil {
		t.Fatal(err)
	}

	cfg := goenet.DefaultConfig()
	cfg.ChannelLimit = 1

	sock := testsupport.NewFakeSocket()
	host := NewHost(cfg, sock, 77)
	host.AddPeer(addr, goenet.PeerStateConnected)
	return host, sock
}

type testPeer struct {
	Raw *ipeer.Peer
}

func (p *testPeer) SentReliableCount() int {
	return p.Raw.OutgoingSendReliableCommands.Len()
}

func (p *testPeer) OutgoingCount() int {
	return p.Raw.OutgoingCommands.Len()
}

func (p *testPeer) InFlightReliableCount() int {
	return p.Raw.SentReliableCommands.Len()
}

func (p *testPeer) mustOutgoingSendReliable(t *testing.T) *ipeer.OutgoingCommand {
	t.Helper()
	elem := p.Raw.OutgoingSendReliableCommands.Front()
	if elem == nil {
		t.Fatal("expected queued reliable command")
	}
	return elem.Value()
}

func (p *testPeer) mustOutgoing(t *testing.T) *ipeer.OutgoingCommand {
	t.Helper()
	elem := p.Raw.OutgoingCommands.Front()
	if elem == nil {
		t.Fatal("expected queued command")
	}
	return elem.Value()
}

func (p *testPeer) mustInFlightReliable(t *testing.T) *ipeer.OutgoingCommand {
	t.Helper()
	elem := p.Raw.SentReliableCommands.Front()
	if elem == nil {
		t.Fatal("expected in-flight reliable command")
	}
	return elem.Value()
}

var _ isocket.DatagramSocket = (*testsupport.FakeSocket)(nil)
