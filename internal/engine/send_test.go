package engine

import (
	"context"
	"encoding/binary"
	"math"
	"net/netip"
	"strings"
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

func TestFlushWithoutReliableCommandsOmitsSentTimeMetadata(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	packet := &goenet.Packet{Data: []byte("abc")}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	write := sock.MustWrite(t, 0)
	header, err := iprotocol.ParseHeader(write.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if header.Flags != 0 {
		t.Fatalf("header flags = 0x%04x", header.Flags)
	}
	if header.SentTime != 0 {
		t.Fatalf("header sent time = %d", header.SentTime)
	}

	if got := write.Payload[2]; iprotocol.Command(got&byte(iprotocol.CommandMask)) != iprotocol.CommandSendUnreliable {
		t.Fatalf("wire command = 0x%02x", got)
	}
	if got := binary.BigEndian.Uint16(write.Payload[4:6]); got != 0 {
		t.Fatalf("wire reliable sequence = %d", got)
	}
	if got := binary.BigEndian.Uint16(write.Payload[6:8]); got != 1 {
		t.Fatalf("wire unreliable sequence = %d", got)
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

func TestSendRejectsPacketThatExceedsNoFragmentationLimit(t *testing.T) {
	host, _ := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	packet := &goenet.Packet{
		Data: bytesOfLen(int(host.config.MTU-9), 'x'),
	}
	err := host.Send(peer.Raw, 0, packet)
	if err == nil {
		t.Fatal("expected oversize packet error")
	}
	if !strings.Contains(err.Error(), "packet exceeds no-fragmentation limit") {
		t.Fatalf("error = %v", err)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount = %d", got)
	}
	if got := peer.SentReliableCount(); got != 0 {
		t.Fatalf("SentReliableCount = %d", got)
	}
}

func TestFlushStopsBeforeExceedingMTUBudget(t *testing.T) {
	host, sock := newSizedTestHost(t, 20)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	first := &goenet.Packet{Data: []byte("1234567890")}
	second := &goenet.Packet{Data: []byte("abcdefghij")}
	if err := host.Send(peer.Raw, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := host.Send(peer.Raw, 0, second); err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount after first flush = %d", got)
	}
	if got := peer.OutgoingCount(); got != 1 {
		t.Fatalf("OutgoingCount after first flush = %d", got)
	}

	firstWrite := sock.MustWrite(t, 0)
	if len(firstWrite.Payload) != 20 {
		t.Fatalf("first datagram length = %d", len(firstWrite.Payload))
	}
	if got := countWireCommands(t, firstWrite.Payload); got != 1 {
		t.Fatalf("first datagram command count = %d", got)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("WriteCount after second flush = %d", got)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount after second flush = %d", got)
	}
}

func TestFlushStopsAtMaximumCommandCount(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: host.MustConnectedPeer()}

	for i := 0; i < int(iprotocol.MaximumPacketCommands)+1; i++ {
		if err := host.Send(peer.Raw, 0, &goenet.Packet{Data: []byte{'a' + byte(i%26)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount after first flush = %d", got)
	}
	if got := countWireCommands(t, sock.MustWrite(t, 0).Payload); got != int(iprotocol.MaximumPacketCommands) {
		t.Fatalf("first datagram command count = %d", got)
	}
	if got := peer.OutgoingCount(); got != 1 {
		t.Fatalf("OutgoingCount after first flush = %d", got)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("WriteCount after second flush = %d", got)
	}
	if got := countWireCommands(t, sock.MustWrite(t, 1).Payload); got != 1 {
		t.Fatalf("second datagram command count = %d", got)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount after second flush = %d", got)
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

func newSizedTestHost(t *testing.T, mtu uint32) (*Host, *testsupport.FakeSocket) {
	t.Helper()

	addr, err := goenet.NewAddress(netip.MustParseAddrPort("127.0.0.1:9001"), 0)
	if err != nil {
		t.Fatal(err)
	}

	cfg := goenet.DefaultConfig()
	cfg.ChannelLimit = 1
	cfg.MTU = mtu

	sock := testsupport.NewFakeSocket()
	host := NewHost(cfg, sock, 77)
	host.AddPeer(addr, goenet.PeerStateConnected)
	return host, sock
}

func countWireCommands(t *testing.T, payload []byte) int {
	t.Helper()

	header, err := iprotocol.ParseHeader(payload)
	if err != nil {
		t.Fatal(err)
	}

	offset := 2
	if header.Flags&iprotocol.HeaderFlagSentTime != 0 {
		offset = 4
	}

	count := 0
	for offset < len(payload) {
		cmd := iprotocol.Command(payload[offset] & byte(iprotocol.CommandMask))
		switch cmd {
		case iprotocol.CommandSendReliable:
			if offset+6 > len(payload) {
				t.Fatalf("truncated reliable command at offset %d", offset)
			}
			dataLen := int(binary.BigEndian.Uint16(payload[offset+4 : offset+6]))
			offset += 6 + dataLen
		case iprotocol.CommandSendUnreliable:
			if offset+8 > len(payload) {
				t.Fatalf("truncated unreliable command at offset %d", offset)
			}
			dataLen := int(binary.BigEndian.Uint16(payload[offset+6 : offset+8]))
			offset += 8 + dataLen
		default:
			t.Fatalf("unexpected command %d at offset %d", cmd, offset)
		}
		count++
	}

	if offset != len(payload) {
		t.Fatalf("wire payload ended at %d of %d", offset, len(payload))
	}

	return count
}

func bytesOfLen(n int, b byte) []byte {
	if n <= 0 {
		return nil
	}

	if n > math.MaxInt32 {
		panic("test payload too large")
	}

	buf := make([]byte, n)
	for i := range buf {
		buf[i] = b
	}
	return buf
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
