package goenet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/testsupport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	if host.Config().ChannelLimit != 2 {
		t.Fatalf("channel limit = %d, want 2", host.Config().ChannelLimit)
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

func TestListenNormalizesZeroChannelLimitInConfigSnapshot(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{PeerCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if got := host.Config().ChannelLimit; got == 0 {
		t.Fatal("expected normalized non-zero channel limit")
	}
}

func TestListenRejectsNegativePeerCount(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", Config{PeerCount: -1, ChannelLimit: 1}); err == nil {
		t.Fatal("expected negative peer count to be rejected")
	}
}

func TestListenRejectsPeerCountAboveProtocolLimit(t *testing.T) {
	_, err := Listen("127.0.0.1:0", Config{
		PeerCount:    int(protocol.MaximumPeerID) + 1,
		ChannelLimit: 1,
	})
	require.Error(t, err)
}

func TestListenRejectsTooSmallMTU(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", Config{PeerCount: 1, ChannelLimit: 1, MTU: 1}); err == nil {
		t.Fatal("expected too-small MTU to be rejected")
	}
}

func TestConfigRoundTripsLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	host, sock := newConfiguredTestHost(Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	})
	_ = sock

	if got := host.Config().Logger; got != logger {
		t.Fatalf("Config().Logger = %p, want %p", got, logger)
	}
}

func TestDefaultConfigKeepsLoggerNilForCallers(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Logger != nil {
		t.Fatal("DefaultConfig().Logger should be nil for callers")
	}
}

func TestListenLogsHostLifecycleWithComponentTag(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)

	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Level == slog.LevelDebug && r.Message == "host started" && r.Attrs["component"] == "host"
	}) {
		t.Fatal("missing host started log")
	}
}

func TestCloseLogsHostLifecycleWithComponentTag(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)

	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Level == slog.LevelDebug && r.Message == "host closed" && r.Attrs["component"] == "host"
	}) {
		t.Fatal("missing host closed log")
	}
}

func TestCloseReturnsSocketCloseFailureWithoutLogging(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	closeErr := errors.New("close failed")

	host, err := newHostWithSocket(Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	}, &closeErrorSocket{
		FakeSocket: testsupport.NewFakeSocket(),
		err:        closeErr,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = host.Close()
	if !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}

	// Close failure must be surfaced to the caller, not double-reported via
	// the logger. The "host closed" Debug line should also stay suppressed
	// because the close path failed.
	if handler.Contains(func(r capturedRecord) bool {
		return r.Message == "host closed" && r.Attrs["component"] == "host"
	}) {
		t.Fatal("unexpected host closed log on close failure")
	}
	if handler.Contains(func(r capturedRecord) bool {
		return r.Message == "host close failed"
	}) {
		t.Fatal("close failure should not be logged; caller observes the returned error")
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

func TestPeerRemoteAddrMatchesConnectedEndpoint(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0xCAFE)

	addr, ok := peer.RemoteAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("remote addr type = %T, want *net.UDPAddr", peer.RemoteAddr())
	}
	if got := addr.String(); got != "127.0.0.1:9001" {
		t.Fatalf("remote addr = %q, want %q", got, "127.0.0.1:9001")
	}

	addrPort := peer.RemoteAddrPort()
	if !addrPort.IsValid() {
		t.Fatal("RemoteAddrPort() returned invalid address")
	}
	if got := addrPort.String(); got != "127.0.0.1:9001" {
		t.Fatalf("remote addr port = %q, want %q", got, "127.0.0.1:9001")
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

func TestCloseWaitsForInFlightServiceCall(t *testing.T) {
	sock := newBlockingCloseSocket()
	host, err := newHostWithSocket(Config{PeerCount: 1, ChannelLimit: 1}, sock)
	if err != nil {
		t.Fatal(err)
	}

	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()

	serviceDone := make(chan error, 1)
	go func() {
		_, err := host.Service(serviceCtx, time.Hour)
		serviceDone <- err
	}()

	select {
	case <-sock.readStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Service to enter socket read")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- host.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before Service released host lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancelService()

	select {
	case err := <-serviceDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Service() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Service to return after cancellation")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Close to finish after Service returned")
	}
}

func TestServiceZeroTimeoutPollsWithoutBlocking(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{PeerCount: 1, ChannelLimit: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	event, err := host.Service(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, EventNone, event.Type)
	assert.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestServiceZeroTimeoutReceivesBufferedDatagram(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Intercept: interceptorFunc(func(netip.AddrPort, []byte) (InterceptDecision, error) {
			return InterceptDecision{
				Result: InterceptResultConsume,
				Event:  &Event{Type: EventDisconnect, Data: 0xBEEF},
			}, nil
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close() })

	conn, err := net.DialUDP("udp", nil, host.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("buffered"))
	require.NoError(t, err)

	event, err := host.Service(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, EventDisconnect, event.Type)
	assert.Equal(t, uint32(0xBEEF), event.Data)
}

func TestServiceDrainsBufferedDatagramBurst(t *testing.T) {
	const packetCount = 5
	intercepted := 0
	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Intercept: interceptorFunc(func(netip.AddrPort, []byte) (InterceptDecision, error) {
			intercepted++
			return InterceptDecision{Result: InterceptResultConsume}, nil
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close() })

	conn, err := net.DialUDP("udp", nil, host.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	for i := 0; i < packetCount; i++ {
		_, err = conn.Write([]byte{byte(i)})
		require.NoError(t, err)
	}

	_, err = host.Service(context.Background(), time.Second)
	require.NoError(t, err)
	assert.Equal(t, packetCount, intercepted)
}

func TestServiceReturnsPromptlyAfterReceivingDatagram(t *testing.T) {
	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Intercept: interceptorFunc(func(netip.AddrPort, []byte) (InterceptDecision, error) {
			return InterceptDecision{
				Result: InterceptResultConsume,
				Event:  &Event{Type: EventDisconnect, Data: 0xC0FFEE},
			}, nil
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close() })

	conn, err := net.DialUDP("udp", nil, host.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("intercept me"))
	require.NoError(t, err)

	started := time.Now()
	event, err := host.Service(context.Background(), 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, EventDisconnect, event.Type)
	assert.Equal(t, uint32(0xC0FFEE), event.Data)
	assert.Less(t, time.Since(started), 500*time.Millisecond)
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

	if err := peer.Disconnect(context.Background(), 9); err != nil {
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

	raw := peer.raw
	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), marshalDatagram(
		protocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
		},
		protocol.Acknowledge{
			Header: protocol.CommandHeader{
				ChannelID:              disconnect.Header.ChannelID,
				ReliableSequenceNumber: 3,
			},
			ReceivedReliableSequenceNumber: disconnect.Header.ReliableSequenceNumber,
			ReceivedSentTime:               header.SentTime,
		},
	))

	event, err := host.Service(context.Background(), time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, EventDisconnect, event.Type)
	assert.Same(t, peer, event.Peer)
	assert.Zero(t, event.Data)
	require.ErrorIs(t, peer.Send(0, &Packet{Data: []byte("stale")}), ErrNilPeer)
}

func TestPeerDisconnectLaterQueuesDisconnectAfterPendingReliableAck(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	if err := peer.Send(0, &Packet{Data: []byte("queued"), Flags: PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if err := peer.DisconnectLater(context.Background(), 17); err != nil {
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

	if err := peer.Disconnect(context.Background(), 0xDEAD); err != nil {
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

func TestDisconnectOnConnectingPeerHonorsCanceledContext(t *testing.T) {
	host, sock := newTestHost()

	peer, err := host.Connect("127.0.0.1:9001", 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = peer.Disconnect(ctx, 0xDEAD)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Disconnect() error = %v, want %v", err, context.Canceled)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("write count = %d, want 0", got)
	}
}

func TestDisconnectNowFlushesUnsequencedDisconnectAndResetsConnectedPeer(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	disconnectNow := mustDisconnectNowPeer(t, peer)
	if err := disconnectNow.DisconnectNow(context.Background(), 0xBEEF); err != nil {
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

func TestDisconnectNowOnResetPeerReturnsNilPeer(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 0x11223344)
	baselineWrites := sock.WriteCount()

	disconnectNow := mustDisconnectNowPeer(t, peer)
	peer.Reset()

	require.ErrorIs(t, disconnectNow.DisconnectNow(context.Background(), 1), ErrNilPeer)
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
	require.ErrorIs(t, peer.Send(0, &Packet{Data: []byte("after reset"), Flags: PacketFlagReliable}), ErrNilPeer)
	if got := sock.WriteCount(); got != baselineWrites {
		t.Fatalf("writes after reset = %d, want %d", got, baselineWrites)
	}
}

func TestLocallyResetPeerHandleStaysInvalidAfterSlotReuse(t *testing.T) {
	tests := []struct {
		name      string
		connected bool
		reset     func(*Peer) error
	}{
		{
			name:      "Reset",
			connected: true,
			reset: func(p *Peer) error {
				p.Reset()
				return nil
			},
		},
		{
			name:      "DisconnectNow",
			connected: true,
			reset: func(p *Peer) error {
				return p.DisconnectNow(context.Background(), 1)
			},
		},
		{
			name: "DisconnectDuringHandshake",
			reset: func(p *Peer) error {
				return p.Disconnect(context.Background(), 1)
			},
		},
		{
			name: "DisconnectLaterDuringHandshake",
			reset: func(p *Peer) error {
				return p.DisconnectLater(context.Background(), 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, sock := newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
			var first *Peer
			var err error
			if tt.connected {
				first = mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
			} else {
				first, err = host.Connect("127.0.0.1:9001", 1, 1)
				require.NoError(t, err)
			}

			require.NoError(t, tt.reset(first))
			assertStalePeer(t, first, sock)

			second := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9002", 2)
			require.NotSame(t, first, second)
			assertStalePeer(t, first, sock)
			require.Len(t, host.peers, 1)

			beforeSend := sock.WriteCount()
			require.NoError(t, second.Send(0, &Packet{Data: []byte("new peer"), Flags: PacketFlagReliable}))
			require.NoError(t, host.Flush(context.Background()))
			require.Equal(t, beforeSend+1, sock.WriteCount())
			assert.Equal(t, netip.MustParseAddrPort("127.0.0.1:9002"), sock.MustWrite(t, beforeSend).Addr.AddrPort())

			beforeBroadcast := sock.WriteCount()
			require.NoError(t, host.Broadcast(0, &Packet{Data: []byte("broadcast"), Flags: PacketFlagReliable}))
			require.NoError(t, host.Flush(context.Background()))
			require.Equal(t, beforeBroadcast+1, sock.WriteCount())
			assert.Equal(t, netip.MustParseAddrPort("127.0.0.1:9002"), sock.MustWrite(t, beforeBroadcast).Addr.AddrPort())
		})
	}
}

func assertStalePeer(t *testing.T, stale *Peer, sock *testsupport.FakeSocket) {
	t.Helper()
	baselineWrites := sock.WriteCount()

	assert.Equal(t, PeerStateDisconnected, stale.State())
	assert.Nil(t, stale.RemoteAddr())
	assert.False(t, stale.RemoteAddrPort().IsValid())
	require.ErrorIs(t, stale.Send(0, &Packet{Data: []byte("stale")}), ErrNilPeer)
	require.ErrorIs(t, stale.Disconnect(context.Background(), 1), ErrNilPeer)
	require.ErrorIs(t, stale.DisconnectNow(context.Background(), 1), ErrNilPeer)
	require.ErrorIs(t, stale.DisconnectLater(context.Background(), 1), ErrNilPeer)
	require.NotPanics(t, stale.Reset)
	require.Equal(t, baselineWrites, sock.WriteCount())
}

func TestEngineResetCannotRebindStalePublicPeer(t *testing.T) {
	host, sock := newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
	first := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
	raw := first.raw

	host.mu.Lock()
	host.engine.Reset(raw)
	host.mu.Unlock()

	second := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9002", 2)
	require.NotSame(t, first, second)
	assertStalePeer(t, first, sock)
}

func TestOrderedPeersRejectsStaleGeneration(t *testing.T) {
	host, sock := newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
	firstAddress, err := core.AddressFromAddrPort(netip.MustParseAddrPort("127.0.0.1:9001"))
	require.NoError(t, err)
	raw := host.engine.AddPeer(firstAddress, core.PeerStateConnected)
	first := host.wrapPeer(raw)

	host.engine.Reset(raw)
	secondAddress, err := core.AddressFromAddrPort(netip.MustParseAddrPort("127.0.0.1:9002"))
	require.NoError(t, err)
	raw = host.engine.AddPeer(secondAddress, core.PeerStateConnected)
	require.NoError(t, host.Broadcast(0, &Packet{Data: []byte("must skip"), Flags: PacketFlagReliable}))
	require.NoError(t, host.Flush(context.Background()))
	require.Zero(t, sock.WriteCount())
	require.Empty(t, host.peers)
	assert.Equal(t, PeerStateDisconnected, first.State())

	second := host.wrapPeer(raw)
	require.NotSame(t, first, second)
	require.NoError(t, host.Broadcast(0, &Packet{Data: []byte("fresh"), Flags: PacketFlagReliable}))
	require.NoError(t, host.Flush(context.Background()))
	require.Equal(t, 1, sock.WriteCount())
	assert.Equal(t, netip.MustParseAddrPort("127.0.0.1:9002"), sock.MustWrite(t, 0).Addr.AddrPort())
}

func TestPeerWrapperMapDoesNotGrowAcrossReconnectCycles(t *testing.T) {
	host, sock := newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
	current := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)

	for cycle := 0; cycle < 4; cycle++ {
		current.Reset()
		require.Empty(t, host.peers)
		current = mustConnectAndVerifyPeer(t, host, sock, fmt.Sprintf("127.0.0.1:%d", 9002+cycle), uint32(cycle+2))
		require.Len(t, host.peers, 1)
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

func TestPublicSendSurfacesExportedSentinels(t *testing.T) {
	tests := []struct {
		name    string
		act     func(t *testing.T) error
		wantErr error
	}{
		{
			name: "nil peer send",
			act: func(t *testing.T) error {
				t.Helper()
				var peer *Peer
				return peer.Send(0, &Packet{Data: []byte("abc"), Flags: PacketFlagReliable})
			},
			wantErr: ErrNilPeer,
		},
		{
			name: "nil packet",
			act: func(t *testing.T) error {
				t.Helper()
				host, sock := newTestHost()
				peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
				return peer.Send(0, nil)
			},
			wantErr: ErrNilPacket,
		},
		{
			name: "channel out of range",
			act: func(t *testing.T) error {
				t.Helper()
				host, sock := newTestHost()
				peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
				return peer.Send(1, &Packet{Data: []byte("abc")})
			},
			wantErr: ErrChannelOutOfRange,
		},
		{
			name: "packet too large",
			act: func(t *testing.T) error {
				t.Helper()
				host, sock := newConfiguredTestHost(Config{
					PeerCount:         1,
					ChannelLimit:      1,
					MaximumPacketSize: 3,
				})
				peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
				return peer.Send(0, &Packet{Data: []byte("four")})
			},
			wantErr: ErrPacketTooLarge,
		},
		{
			name: "broadcast nil packet",
			act: func(t *testing.T) error {
				t.Helper()
				host, sock := newTestHost()
				_ = mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
				return host.Broadcast(0, nil)
			},
			wantErr: ErrNilPacket,
		},
		{
			name: "broadcast nil packet without peers",
			act: func(t *testing.T) error {
				t.Helper()
				host, _ := newTestHost()
				return host.Broadcast(0, nil)
			},
			wantErr: ErrNilPacket,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.act(t)

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestInvalidPeerHandleAccessorsAndOperations(t *testing.T) {
	tests := []struct {
		name      string
		peer      *Peer
		wantState PeerState
		wantErr   error
	}{
		{
			name:      "nil peer",
			peer:      nil,
			wantState: PeerStateDisconnected,
			wantErr:   ErrNilPeer,
		},
		{
			name:      "hostless peer",
			peer:      &Peer{state: PeerStateConnected},
			wantState: PeerStateConnected,
			wantErr:   ErrNilPeer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantState, tt.peer.State())
			assert.Nil(t, tt.peer.RemoteAddr())
			assert.False(t, tt.peer.RemoteAddrPort().IsValid())

			operations := []struct {
				name string
				act  func() error
			}{
				{name: "Send", act: func() error {
					return tt.peer.Send(0, &Packet{Data: []byte("abc")})
				}},
				{name: "Disconnect", act: func() error {
					return tt.peer.Disconnect(context.Background(), 1)
				}},
				{name: "DisconnectNow", act: func() error {
					return tt.peer.DisconnectNow(context.Background(), 1)
				}},
				{name: "DisconnectLater", act: func() error {
					return tt.peer.DisconnectLater(context.Background(), 1)
				}},
			}
			for _, op := range operations {
				t.Run(op.name, func(t *testing.T) {
					require.ErrorIs(t, op.act(), tt.wantErr)
				})
			}
			require.NotPanics(t, func() { tt.peer.Reset() })
		})
	}
}

func TestClosedHostAndPeerOperationsReturnErrHostClosed(t *testing.T) {
	host, sock := newTestHost()
	peer := mustConnectAndVerifyPeer(t, host, sock, "127.0.0.1:9001", 1)
	require.NoError(t, host.Close())

	hostOperations := []struct {
		name string
		act  func() error
	}{
		{name: "Flush", act: func() error {
			return host.Flush(context.Background())
		}},
		{name: "Service", act: func() error {
			_, err := host.Service(context.Background(), 0)
			return err
		}},
		{name: "Broadcast", act: func() error {
			return host.Broadcast(0, &Packet{Data: []byte("abc")})
		}},
		{name: "BandwidthLimit", act: func() error {
			return host.BandwidthLimit(1, 2)
		}},
		{name: "Connect", act: func() error {
			_, err := host.Connect("127.0.0.1:9002", 1, 1)
			return err
		}},
	}
	for _, op := range hostOperations {
		t.Run("host "+op.name, func(t *testing.T) {
			require.ErrorIs(t, op.act(), ErrHostClosed)
		})
	}

	assert.Nil(t, peer.RemoteAddr())
	assert.False(t, peer.RemoteAddrPort().IsValid())

	peerOperations := []struct {
		name string
		act  func() error
	}{
		{name: "Send", act: func() error {
			return peer.Send(0, &Packet{Data: []byte("abc")})
		}},
		{name: "Disconnect", act: func() error {
			return peer.Disconnect(context.Background(), 1)
		}},
		{name: "DisconnectNow", act: func() error {
			return peer.DisconnectNow(context.Background(), 1)
		}},
		{name: "DisconnectLater", act: func() error {
			return peer.DisconnectLater(context.Background(), 1)
		}},
	}
	for _, op := range peerOperations {
		t.Run("peer "+op.name, func(t *testing.T) {
			require.ErrorIs(t, op.act(), ErrHostClosed)
		})
	}
}

func newTestHost() (*Host, *testsupport.FakeSocket) {
	return newConfiguredTestHost(Config{PeerCount: 1, ChannelLimit: 1})
}

func newConfiguredTestHost(cfg Config) (*Host, *testsupport.FakeSocket) {
	sock := testsupport.NewFakeSocket()
	host, err := newHostWithSocket(cfg, sock)
	if err != nil {
		panic(err)
	}
	return host, sock
}

type capturedRecord struct {
	Message string
	Level   slog.Level
	Attrs   map[string]any
}

type captureHandler struct {
	state *captureState
	attrs []slog.Attr
	group []string
}

type captureState struct {
	mu      sync.Mutex
	records []capturedRecord
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{state: &captureState{}}
}

func TestCaptureHandlerWithGroupPrefixesAttrs(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler).WithGroup("host")

	logger.Info("grouped", "status", "ok")

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "grouped" && r.Attrs["host.status"] == "ok"
	}) {
		t.Fatal("missing grouped attr")
	}
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	captured := capturedRecord{
		Message: record.Message,
		Level:   record.Level,
		Attrs:   make(map[string]any),
	}
	for _, attr := range h.attrs {
		captured.Attrs[h.groupedKey(attr.Key)] = attr.Value.Any()
	}
	record.Attrs(func(attr slog.Attr) bool {
		captured.Attrs[h.groupedKey(attr.Key)] = attr.Value.Any()
		return true
	})

	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	h.state.records = append(h.state.records, captured)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	child := &captureHandler{
		state: h.state,
		attrs: make([]slog.Attr, 0, len(h.attrs)+len(attrs)),
		group: append([]string(nil), h.group...),
	}
	child.attrs = append(child.attrs, h.attrs...)
	child.attrs = append(child.attrs, attrs...)
	return child
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return &captureHandler{
		state: h.state,
		attrs: append([]slog.Attr(nil), h.attrs...),
		group: append(append([]string(nil), h.group...), name),
	}
}

func (h *captureHandler) Contains(match func(capturedRecord) bool) bool {
	for _, record := range h.snapshot() {
		if match(record) {
			return true
		}
	}
	return false
}

func (h *captureHandler) snapshot() []capturedRecord {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()

	records := make([]capturedRecord, len(h.state.records))
	copy(records, h.state.records)
	return records
}

func (h *captureHandler) groupedKey(key string) string {
	if len(h.group) == 0 {
		return key
	}

	full := ""
	for _, group := range h.group {
		if group == "" {
			continue
		}
		if full != "" {
			full += "."
		}
		full += group
	}
	if full == "" {
		return key
	}
	return full + "." + key
}

type closeErrorSocket struct {
	*testsupport.FakeSocket
	err error
}

func (s *closeErrorSocket) Close() error {
	return s.err
}

type blockingCloseSocket struct {
	readStarted chan struct{}
	releaseRead chan struct{}
	closed      atomic.Bool
}

func newBlockingCloseSocket() *blockingCloseSocket {
	return &blockingCloseSocket{
		readStarted: make(chan struct{}),
		releaseRead: make(chan struct{}),
	}
}

func (s *blockingCloseSocket) ReadPacket(ctx context.Context, _ []byte) (int, core.Address, error) {
	select {
	case <-s.readStarted:
	default:
		close(s.readStarted)
	}

	select {
	case <-ctx.Done():
		return 0, core.Address{}, ctx.Err()
	case <-s.releaseRead:
		return 0, core.Address{}, net.ErrClosed
	}
}

func (s *blockingCloseSocket) WritePacket(context.Context, core.Address, []byte) (int, error) {
	return 0, errors.New("unexpected write")
}

func (s *blockingCloseSocket) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.releaseRead)
	}
	return nil
}

type disconnectNowPeer interface {
	DisconnectNow(ctx context.Context, data uint32) error
}

func mustDisconnectNowPeer(t *testing.T, peer *Peer) disconnectNowPeer {
	t.Helper()

	disconnectNow, ok := any(peer).(disconnectNowPeer)
	if !ok {
		t.Fatal("Peer does not implement DisconnectNow(context.Context, data uint32) error")
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
	payload, err := header.AppendBinary(nil)
	if err != nil {
		panic(fmt.Sprintf("marshalDatagram header: %v", err))
	}
	for _, command := range commands {
		payload, err = command.AppendBinary(payload)
		if err != nil {
			panic(fmt.Sprintf("marshalDatagram command: %v", err))
		}
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
