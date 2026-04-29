package goenet

import (
	"context"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestServiceUsesPublicInterceptorAndSynthesizedEvent(t *testing.T) {
	var calls int
	sock := testsupport.NewFakeSocket()
	host := newHostWithSocket(Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Intercept: interceptorFunc(func(addr netip.AddrPort, payload []byte) (InterceptDecision, error) {
			calls++
			if addr != netip.MustParseAddrPort("127.0.0.1:9001") {
				t.Fatalf("Intercept() addr = %v", addr)
			}
			if got := string(payload); got != "abc" {
				t.Fatalf("Intercept() payload = %q", got)
			}
			return InterceptDecision{
				Result: InterceptResultConsume,
				Event: &Event{
					Type:   EventDisconnect,
					Data:   0xdecafbad,
					Packet: &Packet{Data: []byte("synthetic"), Flags: PacketFlagReliable},
				},
			}, nil
		}),
	}, sock)

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), []byte("abc"))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("Intercept() calls = %d, want 1", calls)
	}
	if event.Type != EventDisconnect {
		t.Fatalf("event.Type = %d, want %d", event.Type, EventDisconnect)
	}
	if event.Data != 0xdecafbad {
		t.Fatalf("event.Data = %#x", event.Data)
	}
	if event.Packet == nil || string(event.Packet.Data) != "synthetic" {
		t.Fatalf("event.Packet = %#v", event.Packet)
	}
}

func TestFlushUsesPublicChecksummerAndCompressor(t *testing.T) {
	checksummer := &captureChecksummer{}
	compressor := &captureCompressor{}
	sock := testsupport.NewFakeSocket()
	host := newHostWithSocket(Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Checksum:     checksummer,
		Compressor:   compressor,
	}, sock)

	raw := host.engine.AddPeer(mustCoreAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	peer := host.wrapPeer(raw)

	if err := peer.Send(0, &Packet{Data: []byte("compress-me"), Flags: PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if checksummer.calls == 0 {
		t.Fatal("Checksum() was not called")
	}
	if compressor.compressCalls == 0 {
		t.Fatal("Compress() was not called")
	}

	write := sock.MustWrite(t, 0)
	header, err := protocol.ParseHeader(write.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if header.Flags&protocol.HeaderFlagCompressed == 0 {
		t.Fatalf("header.Flags = 0x%x, want compressed", header.Flags)
	}
}

type interceptorFunc func(netip.AddrPort, []byte) (InterceptDecision, error)

func (fn interceptorFunc) Intercept(addr netip.AddrPort, payload []byte) (InterceptDecision, error) {
	return fn(addr, payload)
}

type captureChecksummer struct {
	calls int
}

func (c *captureChecksummer) Checksum(buffers []Buffer) uint32 {
	c.calls++
	var sum uint32
	for _, buffer := range buffers {
		sum += uint32(len(buffer.Data))
	}
	return sum + 1
}

type captureCompressor struct {
	compressCalls int
}

func (c *captureCompressor) Compress(buffers []Buffer, inLimit int, out []byte) (int, error) {
	c.compressCalls++
	if len(out) == 0 {
		return 0, nil
	}
	out[0] = byte(len(buffers))
	return 1, nil
}

func (c *captureCompressor) Decompress(in, out []byte) (int, error) {
	n := copy(out, in)
	return n, nil
}

func mustCoreAddress(t *testing.T, value string) core.Address {
	t.Helper()

	addr, err := core.NewAddress(netip.MustParseAddrPort(value), 0)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}
