package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
)

func TestServiceInterceptConsumesBeforeProtocolDecode(t *testing.T) {
	var (
		calls   int
		capture []byte
	)

	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.Intercept = interceptFunc(func(addr netip.AddrPort, payload []byte) (core.InterceptDecision, error) {
			calls++
			if addr != netip.MustParseAddrPort("127.0.0.1:9001") {
				t.Fatalf("intercept addr = %v", addr)
			}
			capture = append([]byte(nil), payload...)
			return core.InterceptDecision{Result: core.InterceptResultConsume}, nil
		})
	})

	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	payload := []byte{0xff, 0xff, 0x00}
	sock.QueueInbound(raw.Address.AddrPort(), payload)

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if calls != 1 {
		t.Fatalf("intercept calls = %d", calls)
	}
	if !bytes.Equal(capture, payload) {
		t.Fatalf("intercept payload = %x, want %x", capture, payload)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceInterceptCanSynthesizeEvent(t *testing.T) {
	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.Intercept = interceptFunc(func(addr netip.AddrPort, payload []byte) (core.InterceptDecision, error) {
			return core.InterceptDecision{
				Result: core.InterceptResultConsume,
				Event: &core.Event{
					Type: core.EventDisconnect,
					Data: 0xdecafbad,
				},
			}, nil
		})
	})

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9001"), []byte{0x01, 0x02, 0x03})

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventDisconnect {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventDisconnect)
	}
	if event.Data != 0xdecafbad {
		t.Fatalf("event data = %#x", event.Data)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceChecksumRejectsInvalidAndAcceptsValidInboundPackets(t *testing.T) {
	checksummer := checksumFunc(testChecksum)

	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.Checksum = checksummer
	})
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1
	raw.ConnectID = 0x10203040

	valid := checksumDatagram(t, raw.ConnectID, checksummer,
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x1212,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("ok"),
		},
	)
	invalid := append([]byte(nil), valid...)
	invalid[len(invalid)-1] ^= 0xFF

	sock.QueueInbound(raw.Address.AddrPort(), invalid)
	sock.QueueInbound(raw.Address.AddrPort(), valid)

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if got := string(event.Packet.Data); got != "ok" {
		t.Fatalf("packet = %q", got)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceCompressionRoundTripOnSendAndReceive(t *testing.T) {
	compressor := &testCompressor{}

	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.Compressor = compressor
	})
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	if err := host.Send(raw, 0, &core.Packet{
		Data:  []byte("compress-me"),
		Flags: core.PacketFlagReliable,
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if compressor.compressCalls != 1 {
		t.Fatalf("compress calls = %d", compressor.compressCalls)
	}

	write := sock.MustWrite(t, 0)
	header, err := iprotocol.ParseHeader(write.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if header.Flags&iprotocol.HeaderFlagCompressed == 0 {
		t.Fatalf("header flags = 0x%x, want compressed", header.Flags)
	}

	inboundCompressed := marshalCompressedDatagram(t, compressor,
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x2222,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("round-trip"),
		},
	)
	sock.QueueInbound(raw.Address.AddrPort(), inboundCompressed)

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if got := string(event.Packet.Data); got != "round-trip" {
		t.Fatalf("packet = %q", got)
	}
	if compressor.decompressCalls != 1 {
		t.Fatalf("decompress calls = %d", compressor.decompressCalls)
	}
}

type interceptFunc func(netip.AddrPort, []byte) (core.InterceptDecision, error)

func (fn interceptFunc) Intercept(addr netip.AddrPort, payload []byte) (core.InterceptDecision, error) {
	return fn(addr, payload)
}

type checksumFunc func([]core.Buffer) uint32

func (fn checksumFunc) Checksum(buffers []core.Buffer) uint32 {
	return fn(buffers)
}

type testCompressor struct {
	compressCalls   int
	decompressCalls int
	nextID          byte
	stored          map[byte][]byte
}

func (c *testCompressor) Compress(buffers []core.Buffer, inLimit int, out []byte) (int, error) {
	c.compressCalls++
	data := flattenBuffers(buffers)
	if len(data) > inLimit {
		data = data[:inLimit]
	}
	if len(out) < 1 {
		return 0, nil
	}
	if c.stored == nil {
		c.stored = make(map[byte][]byte)
	}
	c.nextID++
	out[0] = c.nextID
	c.stored[c.nextID] = append([]byte(nil), data...)
	return 1, nil
}

func (c *testCompressor) Decompress(in, out []byte) (int, error) {
	c.decompressCalls++
	if len(in) != 1 || c.stored == nil {
		return 0, nil
	}
	data, ok := c.stored[in[0]]
	if !ok || len(data) > len(out) {
		return 0, nil
	}
	copy(out, data)
	return len(data), nil
}

func marshalCompressedDatagram(t *testing.T, compressor core.Compressor, header iprotocol.Header, commands ...iprotocol.PacketCommand) []byte {
	t.Helper()

	body := marshalDatagram(iprotocol.Header{}, commands...)[2:]
	compressed := make([]byte, len(body)+8)
	n, err := compressor.Compress([]core.Buffer{{Data: body}}, len(body), compressed)
	if err != nil {
		t.Fatal(err)
	}
	header.Flags |= iprotocol.HeaderFlagCompressed
	wire := header.MarshalBinary(nil)
	wire = append(wire, compressed[:n]...)
	return wire
}

func checksumDatagram(t *testing.T, seed uint32, checksummer core.Checksummer, header iprotocol.Header, command iprotocol.PacketCommand) []byte {
	t.Helper()

	headerBytes := header.MarshalBinary(nil)
	bodyBytes := command.MarshalBinary(nil)
	checksumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBytes, seed)
	sum := checksummer.Checksum([]core.Buffer{
		{Data: headerBytes},
		{Data: checksumBytes},
		{Data: bodyBytes},
	})
	binary.LittleEndian.PutUint32(checksumBytes, sum)

	wire := append([]byte(nil), headerBytes...)
	wire = append(wire, checksumBytes...)
	wire = append(wire, bodyBytes...)
	return wire
}

func testChecksum(buffers []core.Buffer) uint32 {
	var sum uint32
	for _, buffer := range buffers {
		for _, b := range buffer.Data {
			sum = (sum << 5) - sum + uint32(b)
		}
	}
	return sum
}

func flattenBuffers(buffers []core.Buffer) []byte {
	total := 0
	for _, buffer := range buffers {
		total += len(buffer.Data)
	}
	out := make([]byte, 0, total)
	for _, buffer := range buffers {
		out = append(out, buffer.Data...)
	}
	return out
}
