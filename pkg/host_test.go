package goenet

import (
	"context"
	"errors"
	"net"
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

func TestListenExposesBoundLocalAddr(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	addr, ok := host.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr type = %T, want *net.UDPAddr", host.LocalAddr())
	}
	if addr.Port == 0 {
		t.Fatal("expected non-zero bound port")
	}
	if !addr.IP.IsLoopback() {
		t.Fatalf("local addr IP = %v, want loopback", addr.IP)
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

func TestNewHostExposesBoundLocalAddr(t *testing.T) {
	host, err := NewHost(Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	addr, ok := host.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr type = %T, want *net.UDPAddr", host.LocalAddr())
	}
	if addr.Port == 0 {
		t.Fatal("expected non-zero bound port")
	}
}

func TestLocalAddrPortMatchesBoundUDPAddr(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	addr, ok := host.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr type = %T, want *net.UDPAddr", host.LocalAddr())
	}

	got := host.LocalAddrPort()
	if !got.IsValid() {
		t.Fatal("LocalAddrPort() returned invalid address")
	}
	if got.Port() != uint16(addr.Port) {
		t.Fatalf("LocalAddrPort().Port() = %d, want %d", got.Port(), addr.Port)
	}
	if !got.Addr().IsLoopback() {
		t.Fatalf("LocalAddrPort().Addr() = %v, want loopback", got.Addr())
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
	if got := write.Addr.AddrPort().String(); got != "127.0.0.1:9001" {
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

func TestPeerSendQueuesOutboundPayloadAndFlushes(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	if err := peer.Send(0, &Packet{Data: []byte("ping"), Flags: PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != baselineWrites {
		t.Fatalf("writes before send flush = %d, want %d", got, baselineWrites)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	write := sock.MustWrite(t, baselineWrites)
	if got := write.Addr.AddrPort().String(); got != "127.0.0.1:9001" {
		t.Fatalf("write addr = %q, want %q", got, "127.0.0.1:9001")
	}

	header, command := mustSingleCommand(t, write.Payload)
	if header.Flags != protocol.HeaderFlagSentTime {
		t.Fatalf("header flags = 0x%04x, want 0x%04x", header.Flags, protocol.HeaderFlagSentTime)
	}

	payload, ok := command.(protocol.SendReliable)
	if !ok {
		t.Fatalf("payload command type = %T", command)
	}
	if payload.Header.ChannelID != 0 {
		t.Fatalf("channel id = %d, want 0", payload.Header.ChannelID)
	}
	if string(payload.Data) != "ping" {
		t.Fatalf("payload data = %q, want %q", payload.Data, "ping")
	}
}

func TestPeerSendQueuesOutboundUnsequencedPayloadAndFlushes(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	if err := peer.Send(0, &Packet{Data: []byte("ping"), Flags: PacketFlagUnsequenced}); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != baselineWrites {
		t.Fatalf("writes before send flush = %d, want %d", got, baselineWrites)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	write := sock.MustWrite(t, baselineWrites)
	header, command := mustSingleCommand(t, write.Payload)
	if header.Flags != 0 {
		t.Fatalf("header flags = 0x%04x, want 0x0000", header.Flags)
	}

	payload, ok := command.(protocol.SendUnsequenced)
	if !ok {
		t.Fatalf("payload command type = %T", command)
	}
	if payload.Header.ChannelID != 0 {
		t.Fatalf("channel id = %d, want 0", payload.Header.ChannelID)
	}
	if payload.Header.Flags != protocol.CommandFlagUnsequenced {
		t.Fatalf("payload flags = 0x%02x, want 0x%02x", payload.Header.Flags, protocol.CommandFlagUnsequenced)
	}
	if payload.UnsequencedGroup != 1 {
		t.Fatalf("unsequenced group = %d, want 1", payload.UnsequencedGroup)
	}
	if string(payload.Data) != "ping" {
		t.Fatalf("payload data = %q, want %q", payload.Data, "ping")
	}
}

func TestDisconnectOnConnectedPeerQueuesAcknowledgedDisconnect(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	if err := peer.Disconnect(9); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != PeerStateDisconnecting {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnecting)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	write := sock.MustWrite(t, baselineWrites)
	header, command := mustSingleCommand(t, write.Payload)
	if header.Flags != protocol.HeaderFlagSentTime {
		t.Fatalf("header flags = 0x%04x, want 0x%04x", header.Flags, protocol.HeaderFlagSentTime)
	}

	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Header.ChannelID != 0xFF {
		t.Fatalf("disconnect channel id = %d, want 255", disconnect.Header.ChannelID)
	}
	if disconnect.Header.Flags != protocol.CommandFlagAcknowledge {
		t.Fatalf("disconnect flags = 0x%02x, want 0x%02x", disconnect.Header.Flags, protocol.CommandFlagAcknowledge)
	}
	if disconnect.Data != 9 {
		t.Fatalf("disconnect data = %d, want 9", disconnect.Data)
	}
}

func TestPeerDisconnectLaterQueuesDisconnectAfterPendingReliableAck(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	if err := peer.Send(0, &Packet{Data: []byte("queued"), Flags: PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if err := peer.DisconnectLater(17); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != PeerStateDisconnectLater {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnectLater)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	sendHeader, sendCommand := mustSingleCommand(t, sock.MustWrite(t, baselineWrites).Payload)
	sendReliable, ok := sendCommand.(protocol.SendReliable)
	if !ok {
		t.Fatalf("queued command type = %T", sendCommand)
	}

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), marshalDatagram(
		protocol.Header{
			PeerID:    peer.raw.IncomingPeerID,
			SessionID: peer.raw.IncomingSessionID,
		},
		protocol.Acknowledge{
			Header: protocol.CommandHeader{
				ChannelID:              sendReliable.Header.ChannelID,
				ReliableSequenceNumber: 2,
			},
			ReceivedReliableSequenceNumber: sendReliable.Header.ReliableSequenceNumber,
			ReceivedSentTime:               sendHeader.SentTime,
		},
	))

	event, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != EventNone {
		t.Fatalf("event type = %d, want %d", event.Type, EventNone)
	}

	disconnectWrite := sock.MustWrite(t, baselineWrites+1)
	_, command := mustSingleCommand(t, disconnectWrite.Payload)
	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Data != 17 {
		t.Fatalf("disconnect data = %d, want 17", disconnect.Data)
	}
}

func TestDisconnectOnConnectingPeerFlushesUnsequencedDisconnectAndResets(t *testing.T) {
	host, sock := newTestHost()

	peer, err := host.Connect("127.0.0.1:9001", 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	if err := peer.Disconnect(0xDEAD); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("write count = %d, want 1", got)
	}

	write := sock.MustWrite(t, 0)
	header, command := mustSingleCommand(t, write.Payload)
	if header.PeerID != protocol.MaximumPeerID {
		t.Fatalf("header peer id = %d, want %d", header.PeerID, protocol.MaximumPeerID)
	}

	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Header.ChannelID != 0xFF {
		t.Fatalf("disconnect channel id = %d, want 255", disconnect.Header.ChannelID)
	}
	if disconnect.Header.Flags != protocol.CommandFlagUnsequenced {
		t.Fatalf("disconnect flags = 0x%02x, want 0x%02x", disconnect.Header.Flags, protocol.CommandFlagUnsequenced)
	}
	if disconnect.Data != 0xDEAD {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, uint32(0xDEAD))
	}
}

func TestDisconnectNowFlushesUnsequencedDisconnectAndResetsConnectedPeer(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	disconnectNow := mustDisconnectNowPeer(t, peer)
	if err := disconnectNow.DisconnectNow(0xBEEF); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}
	if got := sock.WriteCount(); got != baselineWrites+1 {
		t.Fatalf("write count = %d, want %d", got, baselineWrites+1)
	}

	write := sock.MustWrite(t, baselineWrites)
	_, command := mustSingleCommand(t, write.Payload)
	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Header.Flags != protocol.CommandFlagUnsequenced {
		t.Fatalf("disconnect flags = 0x%02x, want 0x%02x", disconnect.Header.Flags, protocol.CommandFlagUnsequenced)
	}
	if disconnect.Data != 0xBEEF {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, uint32(0xBEEF))
	}
}

func TestDisconnectNowOnDisconnectedPeerIsSafe(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	disconnectNow := mustDisconnectNowPeer(t, peer)
	peer.Reset()

	if err := disconnectNow.DisconnectNow(1); err != nil {
		t.Fatal(err)
	}
	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}
	if got := sock.WriteCount(); got != baselineWrites {
		t.Fatalf("write count = %d, want %d", got, baselineWrites)
	}
}

func TestPeerResetInvalidatesStateLocally(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	peer.Reset()

	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}
	if err := peer.Send(0, &Packet{Data: []byte("after reset"), Flags: PacketFlagReliable}); err == nil {
		t.Fatal("expected send after reset to fail")
	}
	if got := sock.WriteCount(); got != baselineWrites {
		t.Fatalf("writes after reset = %d, want %d", got, baselineWrites)
	}
}

func TestDisconnectedPeerHandleStaysInvalidAfterSlotReuse(t *testing.T) {
	host, sock := newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
	first := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11111111)

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), marshalDatagram(
		protocol.Header{
			PeerID:    first.raw.IncomingPeerID,
			SessionID: first.raw.IncomingSessionID,
			Flags:     protocol.HeaderFlagSentTime,
			SentTime:  0x5050,
		},
		protocol.Disconnect{
			Header: protocol.CommandHeader{
				ChannelID:              0xFF,
				Flags:                  protocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 2,
			},
			Data: 0xABCD1234,
		},
	))

	event, err := host.Service(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != EventDisconnect {
		t.Fatalf("disconnect event type = %d, want %d", event.Type, EventDisconnect)
	}
	if event.Peer != first {
		t.Fatal("disconnect event did not use original peer handle")
	}

	second := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9002", 0x22222222)
	if second == first {
		t.Fatal("reused slot should produce a fresh public peer handle")
	}

	if got := first.State(); got != PeerStateDisconnected {
		t.Fatalf("stale peer state = %d, want %d", got, PeerStateDisconnected)
	}
	if err := first.Send(0, &Packet{Data: []byte("stale"), Flags: PacketFlagReliable}); !errors.Is(err, ErrNilPeer) {
		t.Fatalf("stale peer Send() error = %v, want %v", err, ErrNilPeer)
	}
}

func TestBroadcastFansOutToConnectedPeersOnly(t *testing.T) {
	host, sock := newConfiguredTestHost(Config{PeerCount: 2, ChannelLimit: 1})
	connected := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
	connecting, err := host.Connect("127.0.0.1:9002", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	baselineWrites := sock.WriteCount()
	if got := connecting.State(); got != PeerStateConnecting {
		t.Fatalf("connecting peer state = %d, want %d", got, PeerStateConnecting)
	}

	if err := host.Broadcast(0, &Packet{Data: []byte("fanout"), Flags: PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := sock.WriteCount(); got != baselineWrites+1 {
		t.Fatalf("write count = %d, want %d", got, baselineWrites+1)
	}

	write := sock.MustWrite(t, baselineWrites)
	if got := write.Addr.AddrPort().String(); got != "127.0.0.1:9001" {
		t.Fatalf("broadcast addr = %q, want %q", got, "127.0.0.1:9001")
	}

	_, command := mustSingleCommand(t, write.Payload)
	payload, ok := command.(protocol.SendReliable)
	if !ok {
		t.Fatalf("broadcast command type = %T", command)
	}
	if string(payload.Data) != "fanout" {
		t.Fatalf("broadcast payload = %q, want %q", payload.Data, "fanout")
	}
	if connected.State() != PeerStateConnected {
		t.Fatalf("connected peer state = %d, want %d", connected.State(), PeerStateConnected)
	}
}

func newTestHost() (*Host, *testsupport.FakeSocket) {
	return newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
}

func newConfiguredTestHost(cfg Config) (*Host, *testsupport.FakeSocket) {
	sock := testsupport.NewFakeSocket()
	return newHostWithSocket(cfg, sock), sock
}

type disconnectNowPeer interface {
	DisconnectNow(data uint32) error
}

func mustDisconnectNowPeer(t *testing.T, peer *Peer) disconnectNowPeer {
	t.Helper()

	disconnectNow, ok := any(peer).(disconnectNowPeer)
	if !ok {
		t.Fatal("Peer does not implement DisconnectNow(data uint32) error")
	}

	return disconnectNow
}

func mustConnectAndVerifyPeer(t *testing.T, host *Host, sock *testsupport.FakeSocket, addr string, data uint32) *Peer {
	t.Helper()

	peer, err := host.Connect(addr, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	sock.QueueInbound(netip.MustParseAddrPort(addr), marshalDatagram(
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
		t.Fatal("connect event did not reuse the original peer handle")
	}

	return peer
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
