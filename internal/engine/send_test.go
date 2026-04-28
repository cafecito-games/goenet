package engine

import (
	"context"
	"encoding/binary"
	"math"
	"net/netip"
	"strings"
	"testing"

	"github.com/cafecito-games/goenet"
	"github.com/cafecito-games/goenet/internal/core"
	ipeer "github.com/cafecito-games/goenet/internal/peer"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
	isocket "github.com/cafecito-games/goenet/internal/socket"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestReliableSendQueuesAcknowledgeableCommand(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

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
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

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

	peer := &testPeer{Raw: mustConnectedPeer(t, host)}
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
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

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
	if header.PeerID != peer.Raw.OutgoingPeerID {
		t.Fatalf("header peer id = %d", header.PeerID)
	}
	if header.SessionID != peer.Raw.OutgoingSessionID {
		t.Fatalf("header session id = %d", header.SessionID)
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
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

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
	if header.PeerID != peer.Raw.OutgoingPeerID {
		t.Fatalf("header peer id = %d", header.PeerID)
	}
	if header.SessionID != peer.Raw.OutgoingSessionID {
		t.Fatalf("header session id = %d", header.SessionID)
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

func TestFlushMovesAckCommandFromGeneralQueueInFlightWithWireMetadata(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	peer.Raw.OutgoingReliableSequenceNumber = 8
	err := host.queueOutgoingControlCommand(peer.Raw, ipeer.Command{
		Header: ipeer.Header{
			Command:   iprotocol.CommandPing,
			ChannelID: 0xff,
			Flags:     iprotocol.CommandFlagAcknowledge,
		},
		Payload: &protocolCommand{
			command:   iprotocol.CommandPing,
			flags:     iprotocol.CommandFlagAcknowledge,
			channelID: 0xff,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 1 {
		t.Fatalf("InFlightReliableCount = %d", got)
	}

	cmd := peer.mustInFlightReliable(t)
	if cmd.Command.Header.Command != iprotocol.CommandPing {
		t.Fatalf("command = %v", cmd.Command.Header.Command)
	}
	if cmd.Command.Header.Flags != iprotocol.CommandFlagAcknowledge {
		t.Fatalf("flags = 0x%02x", cmd.Command.Header.Flags)
	}
	if cmd.ReliableSequenceNumber != 9 {
		t.Fatalf("reliable sequence = %d", cmd.ReliableSequenceNumber)
	}
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
	if header.PeerID != peer.Raw.OutgoingPeerID {
		t.Fatalf("header peer id = %d", header.PeerID)
	}
	if header.SessionID != peer.Raw.OutgoingSessionID {
		t.Fatalf("header session id = %d", header.SessionID)
	}
}

func TestSendRejectsPacketThatExceedsNoFragmentationLimit(t *testing.T) {
	host, _ := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	packet := &goenet.Packet{
		Data: bytesOfLen(int(peer.Raw.MTU-9), 'x'),
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

func TestSendUsesPeerMTUForValidation(t *testing.T) {
	host, _ := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}
	peer.Raw.MTU = 20

	err := host.Send(peer.Raw, 0, &goenet.Packet{
		Data: bytesOfLen(11, 'x'),
	})
	if err == nil {
		t.Fatal("expected oversize packet error")
	}
	if !strings.Contains(err.Error(), "packet exceeds no-fragmentation limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestFlushStopsBeforeExceedingMTUBudget(t *testing.T) {
	host, sock := newSizedTestHost(t, 20)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

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

	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("WriteCount after flush = %d", got)
	}

	firstWrite := sock.MustWrite(t, 0)
	if len(firstWrite.Payload) != 20 {
		t.Fatalf("first datagram length = %d", len(firstWrite.Payload))
	}
	if got := countWireCommands(t, firstWrite.Payload); got != 1 {
		t.Fatalf("first datagram command count = %d", got)
	}
	secondWrite := sock.MustWrite(t, 1)
	if len(secondWrite.Payload) != 20 {
		t.Fatalf("second datagram length = %d", len(secondWrite.Payload))
	}
	if got := countWireCommands(t, secondWrite.Payload); got != 1 {
		t.Fatalf("second datagram command count = %d", got)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount after flush = %d", got)
	}
}

func TestFlushStopsAtMaximumCommandCount(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	for i := 0; i < int(iprotocol.MaximumPacketCommands)+1; i++ {
		if err := host.Send(peer.Raw, 0, &goenet.Packet{Data: []byte{'a' + byte(i%26)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("WriteCount after flush = %d", got)
	}
	if got := countWireCommands(t, sock.MustWrite(t, 0).Payload); got != int(iprotocol.MaximumPacketCommands) {
		t.Fatalf("first datagram command count = %d", got)
	}
	if got := countWireCommands(t, sock.MustWrite(t, 1).Payload); got != 1 {
		t.Fatalf("second datagram command count = %d", got)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount after flush = %d", got)
	}
}

func TestFlushMergesQueuesByQueueTime(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	if err := host.Send(peer.Raw, 0, &goenet.Packet{Data: []byte("r1"), Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if err := host.Send(peer.Raw, 0, &goenet.Packet{Data: []byte("u2")}); err != nil {
		t.Fatal(err)
	}
	if err := host.Send(peer.Raw, 0, &goenet.Packet{Data: []byte("r3"), Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	commands := parseWireCommands(t, sock.MustWrite(t, 0).Payload)
	if len(commands) != 3 {
		t.Fatalf("wire command count = %d", len(commands))
	}
	if got := string(commands[0].payload); got != "r1" {
		t.Fatalf("command 0 payload = %q", got)
	}
	if got := string(commands[1].payload); got != "u2" {
		t.Fatalf("command 1 payload = %q", got)
	}
	if got := string(commands[2].payload); got != "r3" {
		t.Fatalf("command 2 payload = %q", got)
	}
}

func TestFlushDoesNotMutateQueuesWhenContextCanceled(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	packet := &goenet.Packet{Data: []byte("abc"), Flags: goenet.PacketFlagReliable}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := host.Flush(ctx)
	if err == nil {
		t.Fatal("expected flush error")
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
	if got := peer.SentReliableCount(); got != 1 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 0 {
		t.Fatalf("InFlightReliableCount = %d", got)
	}

	cmd := peer.mustOutgoingSendReliable(t)
	if cmd.SendAttempts != 0 {
		t.Fatalf("send attempts = %d", cmd.SendAttempts)
	}
	if cmd.SentTime != 0 {
		t.Fatalf("sent time = %d", cmd.SentTime)
	}
	if cmd.RoundTripTimeout != 0 {
		t.Fatalf("round trip timeout = %d", cmd.RoundTripTimeout)
	}
}

func TestFlushDoesNotMutateQueuesWhenWriteFails(t *testing.T) {
	host, sock := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	packet := &goenet.Packet{Data: []byte("abc"), Flags: goenet.PacketFlagReliable}
	if err := host.Send(peer.Raw, 0, packet); err != nil {
		t.Fatal(err)
	}
	sock.SetWriteError(assertErr("write failed"))

	err := host.Flush(context.Background())
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("error = %v", err)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
	if got := peer.SentReliableCount(); got != 1 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 0 {
		t.Fatalf("InFlightReliableCount = %d", got)
	}

	cmd := peer.mustOutgoingSendReliable(t)
	if cmd.SendAttempts != 0 {
		t.Fatalf("send attempts = %d", cmd.SendAttempts)
	}
	if cmd.SentTime != 0 {
		t.Fatalf("sent time = %d", cmd.SentTime)
	}
	if cmd.RoundTripTimeout != 0 {
		t.Fatalf("round trip timeout = %d", cmd.RoundTripTimeout)
	}
}

func TestFlushErrorsWhenQueuedCommandCannotFitWithinPeerMTU(t *testing.T) {
	host, sock := newSizedTestHost(t, 7)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	err := host.queueOutgoingControlCommand(peer.Raw, ipeer.Command{
		Header: ipeer.Header{
			Command:   iprotocol.CommandPing,
			ChannelID: 0xff,
			Flags:     iprotocol.CommandFlagAcknowledge,
		},
		Payload: &protocolCommand{
			command:   iprotocol.CommandPing,
			flags:     iprotocol.CommandFlagAcknowledge,
			channelID: 0xff,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = host.Flush(context.Background())
	if err == nil {
		t.Fatal("expected unsendable command error")
	}
	if !strings.Contains(err.Error(), "cannot fit within peer MTU") {
		t.Fatalf("error = %v", err)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
	if got := peer.OutgoingCount(); got != 1 {
		t.Fatalf("OutgoingCount = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 0 {
		t.Fatalf("InFlightReliableCount = %d", got)
	}

	cmd := peer.mustOutgoing(t)
	if cmd.SendAttempts != 0 {
		t.Fatalf("send attempts = %d", cmd.SendAttempts)
	}
	if cmd.SentTime != 0 {
		t.Fatalf("sent time = %d", cmd.SentTime)
	}
	if cmd.RoundTripTimeout != 0 {
		t.Fatalf("round trip timeout = %d", cmd.RoundTripTimeout)
	}
}

func TestFlushPreservesQueueStateWhenLaterQueuedCommandCannotFitWithinPeerMTU(t *testing.T) {
	host, sock := newSizedTestHost(t, 10)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	if err := host.queueOutgoingControlCommand(peer.Raw, ipeer.Command{
		Header: ipeer.Header{
			Command:   iprotocol.CommandPing,
			ChannelID: 0xff,
			Flags:     iprotocol.CommandFlagAcknowledge,
		},
		Payload: &protocolCommand{
			command:   iprotocol.CommandPing,
			flags:     iprotocol.CommandFlagAcknowledge,
			channelID: 0xff,
		},
	}); err != nil {
		t.Fatal(err)
	}
	err := host.queueOutgoingControlCommand(peer.Raw, ipeer.Command{
		Header: ipeer.Header{
			Command:   iprotocol.CommandPing,
			ChannelID: 0xff,
			Flags:     iprotocol.CommandFlagAcknowledge,
		},
		Payload: &sizedProtocolCommand{
			protocolCommand: protocolCommand{
				command:   iprotocol.CommandPing,
				flags:     iprotocol.CommandFlagAcknowledge,
				channelID: 0xff,
			},
			extra: []byte("xyz"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = host.Flush(context.Background())
	if err == nil {
		t.Fatal("expected unsendable command error")
	}
	if !strings.Contains(err.Error(), "cannot fit within peer MTU") {
		t.Fatalf("error = %v", err)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
	if got := peer.SentReliableCount(); got != 0 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if got := peer.OutgoingCount(); got != 2 {
		t.Fatalf("OutgoingCount = %d", got)
	}
	if got := peer.InFlightReliableCount(); got != 0 {
		t.Fatalf("InFlightReliableCount = %d", got)
	}

	for elem := peer.Raw.OutgoingCommands.Front(); elem != nil; elem = elem.Next() {
		control := elem.Value()
		if control.SendAttempts != 0 {
			t.Fatalf("control send attempts = %d", control.SendAttempts)
		}
		if control.SentTime != 0 {
			t.Fatalf("control sent time = %d", control.SentTime)
		}
	}
}

func TestSendRejectsUnsequencedPacketsForThisMilestone(t *testing.T) {
	host, _ := newTestHost(t)
	peer := &testPeer{Raw: mustConnectedPeer(t, host)}

	err := host.Send(peer.Raw, 0, &goenet.Packet{
		Data:  []byte("abc"),
		Flags: goenet.PacketFlagUnsequenced,
	})
	if err == nil {
		t.Fatal("expected unsequenced error")
	}
	if !strings.Contains(err.Error(), "unsequenced packets are not supported") {
		t.Fatalf("error = %v", err)
	}
	if got := peer.OutgoingCount(); got != 0 {
		t.Fatalf("OutgoingCount = %d", got)
	}
	if got := peer.SentReliableCount(); got != 0 {
		t.Fatalf("SentReliableCount = %d", got)
	}
	if got := peer.Raw.Channels[0].OutgoingUnreliableSequenceNumber; got != 0 {
		t.Fatalf("channel unreliable sequence = %d", got)
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
	host := NewHost(coreConfigForTest(cfg), sock, 77)
	host.AddPeer(addr, goenet.PeerStateConnected)
	return host, sock
}

func mustConnectedPeer(t *testing.T, host *Host) *ipeer.Peer {
	t.Helper()

	for _, p := range host.peers {
		if p.State == goenet.PeerStateConnected {
			return p
		}
	}

	t.Fatal("expected connected peer")
	return nil
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
	host := NewHost(coreConfigForTest(cfg), sock, 77)
	host.AddPeer(addr, goenet.PeerStateConnected)
	return host, sock
}

func countWireCommands(t *testing.T, payload []byte) int {
	t.Helper()

	return len(parseWireCommands(t, payload))
}

type wireCommand struct {
	command iprotocol.Command
	payload []byte
}

func parseWireCommands(t *testing.T, payload []byte) []wireCommand {
	t.Helper()

	header, err := iprotocol.ParseHeader(payload)
	if err != nil {
		t.Fatal(err)
	}

	offset := 2
	if header.Flags&iprotocol.HeaderFlagSentTime != 0 {
		offset = 4
	}

	var commands []wireCommand
	for offset < len(payload) {
		cmd := iprotocol.Command(payload[offset] & byte(iprotocol.CommandMask))
		switch cmd {
		case iprotocol.CommandSendReliable:
			if offset+6 > len(payload) {
				t.Fatalf("truncated reliable command at offset %d", offset)
			}
			dataLen := int(binary.BigEndian.Uint16(payload[offset+4 : offset+6]))
			if offset+6+dataLen > len(payload) {
				t.Fatalf("truncated reliable payload at offset %d", offset)
			}
			commands = append(commands, wireCommand{
				command: cmd,
				payload: append([]byte(nil), payload[offset+6:offset+6+dataLen]...),
			})
			offset += 6 + dataLen
		case iprotocol.CommandSendUnreliable:
			if offset+8 > len(payload) {
				t.Fatalf("truncated unreliable command at offset %d", offset)
			}
			dataLen := int(binary.BigEndian.Uint16(payload[offset+6 : offset+8]))
			if offset+8+dataLen > len(payload) {
				t.Fatalf("truncated unreliable payload at offset %d", offset)
			}
			commands = append(commands, wireCommand{
				command: cmd,
				payload: append([]byte(nil), payload[offset+8:offset+8+dataLen]...),
			})
			offset += 8 + dataLen
		case iprotocol.CommandPing:
			if offset+4 > len(payload) {
				t.Fatalf("truncated ping command at offset %d", offset)
			}
			commands = append(commands, wireCommand{command: cmd})
			offset += 4
		default:
			t.Fatalf("unexpected command %d at offset %d", cmd, offset)
		}
	}

	if offset != len(payload) {
		t.Fatalf("wire payload ended at %d of %d", offset, len(payload))
	}

	return commands
}

func coreConfigForTest(cfg goenet.Config) core.Config {
	return core.Config{
		PeerCount:          cfg.PeerCount,
		ChannelLimit:       cfg.ChannelLimit,
		MTU:                cfg.MTU,
		MaximumPacketSize:  cfg.MaximumPacketSize,
		MaximumWaitingData: cfg.MaximumWaitingData,
		Checksum:           cfg.Checksum,
		Compressor:         cfg.Compressor,
	}
}

type protocolCommand struct {
	command                iprotocol.Command
	flags                  iprotocol.CommandFlag
	channelID              uint8
	reliableSequenceNumber uint16
}

func (c *protocolCommand) setOutgoingSequenceNumbers(reliable, _ uint16) {
	c.reliableSequenceNumber = reliable
}

func (c *protocolCommand) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 4)...)
	dst[start] = byte(c.command | iprotocol.Command(c.flags))
	dst[start+1] = c.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], c.reliableSequenceNumber)
	return dst
}

type sizedProtocolCommand struct {
	protocolCommand
	extra []byte
}

func (c *sizedProtocolCommand) setOutgoingSequenceNumbers(reliable, unreliable uint16) {
	c.protocolCommand.setOutgoingSequenceNumbers(reliable, unreliable)
}

func (c *sizedProtocolCommand) MarshalBinary(dst []byte) []byte {
	dst = c.protocolCommand.MarshalBinary(dst)
	return append(dst, c.extra...)
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
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
