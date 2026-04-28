package goenet

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestListenReturnsUsableHost(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{PeerCount: 4, ChannelLimit: 2})
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
	host, err := NewHost(Config{PeerCount: 1, ChannelLimit: 1})
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
	host, err := Listen("127.0.0.1:0", Config{})
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

func TestServiceReturnsStablePeerHandlesAcrossEvents(t *testing.T) {
	host, sock := newTestHost()

	addr := netip.MustParseAddrPort("127.0.0.1:9001")
	sock.QueueInbound(addr, marshalDatagram(
		protocol.Header{
			PeerID:   protocol.MaximumPeerID,
			Flags:    protocol.HeaderFlagSentTime,
			SentTime: 0x2222,
		},
		protocol.Connect{
			Header: protocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 1,
			},
			OutgoingPeerID:             7,
			IncomingSessionID:          0xFF,
			OutgoingSessionID:          0xFF,
			MTU:                        1400,
			WindowSize:                 32768,
			ChannelCount:               1,
			IncomingBandwidth:          60000,
			OutgoingBandwidth:          30000,
			PacketThrottleInterval:     5000,
			PacketThrottleAcceleration: 2,
			PacketThrottleDeceleration: 3,
			ConnectID:                  0x11223344,
			Data:                       0x55667788,
		},
	))

	event, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != EventNone {
		t.Fatalf("first event type = %d, want %d", event.Type, EventNone)
	}

	verifyHeader, verifyCommand := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	verify, ok := verifyCommand.(protocol.VerifyConnect)
	if !ok {
		t.Fatalf("verify command type = %T", verifyCommand)
	}

	sock.QueueInbound(addr, marshalDatagram(
		protocol.Header{
			PeerID:    verify.OutgoingPeerID,
			SessionID: verify.OutgoingSessionID,
		},
		protocol.Acknowledge{
			Header: protocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 2,
			},
			ReceivedReliableSequenceNumber: verify.Header.ReliableSequenceNumber,
			ReceivedSentTime:               verifyHeader.SentTime,
		},
	))

	connect, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if connect.Type != EventConnect {
		t.Fatalf("connect type = %d, want %d", connect.Type, EventConnect)
	}
	if connect.Peer == nil {
		t.Fatal("expected connect peer handle")
	}
	if connect.Data != 0x55667788 {
		t.Fatalf("connect data = %#x", connect.Data)
	}
	if got := connect.Peer.State(); got != PeerStateConnected {
		t.Fatalf("connect peer state = %d, want %d", got, PeerStateConnected)
	}
	raw := connect.Peer.raw

	sock.QueueInbound(addr, marshalDatagram(
		protocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     protocol.HeaderFlagSentTime,
			SentTime:  0x4567,
		},
		protocol.SendReliable{
			Header: protocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("hello"),
		},
	))

	receive, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if receive.Type != EventReceive {
		t.Fatalf("receive type = %d, want %d", receive.Type, EventReceive)
	}
	if receive.Peer != connect.Peer {
		t.Fatal("receive peer handle was not stable across events")
	}
	if receive.ChannelID != 0 {
		t.Fatalf("receive channel id = %d, want 0", receive.ChannelID)
	}
	if receive.Packet == nil {
		t.Fatal("expected receive packet")
	}
	if string(receive.Packet.Data) != "hello" {
		t.Fatalf("receive packet = %q, want %q", receive.Packet.Data, "hello")
	}
}

func TestConnectReturnsConnectingPeerAndFlushesConnectCommand(t *testing.T) {
	host, sock := newTestHost()

	peer, err := host.Connect("127.0.0.1:9001", 1, 0x55667788)
	if err != nil {
		t.Fatal(err)
	}
	if peer == nil {
		t.Fatal("expected peer handle")
	}
	if got := peer.State(); got != PeerStateConnecting {
		t.Fatalf("peer state = %d, want %d", got, PeerStateConnecting)
	}
	if sock.WriteCount() != 0 {
		t.Fatalf("writes before flush = %d", sock.WriteCount())
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	write := sock.MustWrite(t, 0)
	if got := write.Addr.String(); got != "127.0.0.1:9001" {
		t.Fatalf("write addr = %q, want %q", got, "127.0.0.1:9001")
	}

	header, command := mustSingleCommand(t, write.Payload)
	if header.PeerID != protocol.MaximumPeerID {
		t.Fatalf("header peer id = %d, want %d", header.PeerID, protocol.MaximumPeerID)
	}
	if header.Flags != protocol.HeaderFlagSentTime {
		t.Fatalf("header flags = 0x%04x, want 0x%04x", header.Flags, protocol.HeaderFlagSentTime)
	}

	connect, ok := command.(protocol.Connect)
	if !ok {
		t.Fatalf("connect command type = %T", command)
	}
	if connect.Header.ChannelID != 0xFF {
		t.Fatalf("channel id = %d, want 255", connect.Header.ChannelID)
	}
	if connect.Header.ReliableSequenceNumber != 1 {
		t.Fatalf("reliable sequence = %d, want 1", connect.Header.ReliableSequenceNumber)
	}
	if connect.OutgoingPeerID != peer.raw.IncomingPeerID {
		t.Fatalf("outgoing peer id = %d, want %d", connect.OutgoingPeerID, peer.raw.IncomingPeerID)
	}
	if connect.ChannelCount != 1 {
		t.Fatalf("channel count = %d, want 1", connect.ChannelCount)
	}
	if connect.ConnectID != peer.raw.ConnectID {
		t.Fatalf("connect id = %#x, want %#x", connect.ConnectID, peer.raw.ConnectID)
	}
	if connect.Data != 0x55667788 {
		t.Fatalf("connect data = %#x, want %#x", connect.Data, uint32(0x55667788))
	}
}

func TestConnectReusesStablePeerHandleWhenVerifyConnectArrives(t *testing.T) {
	host, sock := newTestHost()

	peer, err := host.Connect("127.0.0.1:9001", 1, 0xAABBCCDD)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), marshalDatagram(
		protocol.Header{
			PeerID:    peer.raw.IncomingPeerID,
			SessionID: 0,
			Flags:     protocol.HeaderFlagSentTime,
			SentTime:  0x3344,
		},
		protocol.VerifyConnect{
			Header: protocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 1,
			},
			OutgoingPeerID:             33,
			IncomingSessionID:          2,
			OutgoingSessionID:          3,
			MTU:                        1200,
			WindowSize:                 32000,
			ChannelCount:               1,
			IncomingBandwidth:          64000,
			OutgoingBandwidth:          32000,
			PacketThrottleInterval:     5000,
			PacketThrottleAcceleration: 2,
			PacketThrottleDeceleration: 2,
			ConnectID:                  peer.raw.ConnectID,
		},
	))

	event, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != EventConnect {
		t.Fatalf("event type = %d, want %d", event.Type, EventConnect)
	}
	if event.Peer != peer {
		t.Fatal("connect event did not reuse the peer handle returned by Connect")
	}
	if got := peer.State(); got != PeerStateConnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateConnected)
	}
}

func newTestHost() (*Host, *testsupport.FakeSocket) {
	cfg := DefaultConfig()
	cfg.PeerCount = 1
	cfg.ChannelLimit = 1

	sock := testsupport.NewFakeSocket()
	return newHostWithSocket(cfg, sock), sock
}

func marshalDatagram(header protocol.Header, commands ...protocol.PacketCommand) []byte {
	payload := header.MarshalBinary(nil)
	for _, command := range commands {
		payload = command.MarshalBinary(payload)
	}

	return payload
}

func mustSingleCommand(t *testing.T, payload []byte) (protocol.Header, protocol.PacketCommand) {
	t.Helper()

	header, err := protocol.ParseHeader(payload)
	if err != nil {
		t.Fatal(err)
	}

	offset := 2
	if header.Flags&protocol.HeaderFlagSentTime != 0 {
		offset = 4
	}

	command, _, used, err := protocol.ParseCommand(payload[offset:])
	if err != nil {
		t.Fatal(err)
	}
	if offset+used != len(payload) {
		t.Fatalf("datagram consumed %d of %d bytes", offset+used, len(payload))
	}

	return header, command
}
