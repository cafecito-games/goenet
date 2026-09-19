package engine

import (
	"context"
	"io"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	ipeer "github.com/cafecito-games/goenet/internal/peer"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/testsupport"
	"github.com/stretchr/testify/require"
)

// flushBenchmarkPeerCount mirrors a large production host: most slots sit in
// PeerStateDisconnected and Flush still walks every one of them.
const flushBenchmarkPeerCount = 1024

func TestFlush_IdleHostDoesNotAllocate(t *testing.T) {
	host := NewHost(core.Config{PeerCount: flushBenchmarkPeerCount, ChannelLimit: 1}, discardSocket{}, 77)
	host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		if err := host.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	})

	require.Zero(t, allocs, "idle Flush over %d slots allocated", flushBenchmarkPeerCount)
}

func TestSelectOutgoingBatch_ReusesSelectionBuffer(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	require.NoError(t, host.Send(raw, 0, &core.Packet{Data: []byte("abc")}))

	allocs := testing.AllocsPerRun(100, func() {
		selected, blocked := host.selectOutgoingBatch(raw)
		if blocked != nil || len(selected) != 1 {
			t.Fatalf("selectOutgoingBatch() = %d selected, blocked %v", len(selected), blocked)
		}
	})

	require.Zero(t, allocs, "selectOutgoingBatch allocated per call")
}

func TestSelectOutgoingBatch_ReturnsNothingForIdlePeer(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	selected, blocked := host.selectOutgoingBatch(raw)

	require.Nil(t, blocked)
	require.Empty(t, selected)
}

func TestFlush_ReleasesSelectionReferencesAfterCommit(t *testing.T) {
	host, sock := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	require.NoError(t, host.Send(raw, 0, &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable}))
	queueTestAcknowledgement(raw, 4)

	require.NoError(t, host.Flush(context.Background()))
	require.Equal(t, 1, sock.WriteCount())

	for index, item := range host.selectionScratch[:cap(host.selectionScratch)] {
		require.Nil(t, item.command, "selection scratch entry %d still references a sent command", index)
		require.Nil(t, item.ack, "selection scratch entry %d still references a sent acknowledgement", index)
	}
}

func TestFlush_SendsPeerWithOnlyPendingAcknowledgements(t *testing.T) {
	host, sock := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	queueTestAcknowledgement(raw, 4)
	queueTestAcknowledgement(raw, 5)

	require.NoError(t, host.Flush(context.Background()))

	require.Equal(t, 1, sock.WriteCount())
	require.Zero(t, raw.Acknowledgements.Len())
	payload := sock.MustWrite(t, 0).Payload
	header, err := iprotocol.ParseHeader(payload)
	require.NoError(t, err)
	require.Zero(t, header.Flags&iprotocol.HeaderFlagSentTime, "ack-only datagram must not carry sent time")
	offset := iprotocol.HeaderSizeMinimal
	for _, want := range []uint16{4, 5} {
		command, _, used, err := iprotocol.ParseCommand(payload[offset:])
		require.NoError(t, err)
		ack, ok := command.(iprotocol.Acknowledge)
		require.True(t, ok, "wire command at offset %d = %T, want acknowledge", offset, command)
		require.Equal(t, want, ack.ReceivedReliableSequenceNumber)
		require.Equal(t, uint16(42), ack.ReceivedSentTime)
		offset += used
	}
	require.Equal(t, len(payload), offset, "datagram carries more than the two acknowledgements")
}

func TestFlush_SkipsIdleSlotsBetweenBusyPeers(t *testing.T) {
	sock := testsupport.NewFakeSocket()
	host := NewHost(core.Config{PeerCount: 4, ChannelLimit: 1}, sock, 77)
	first := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	idle := host.AddPeer(mustAddress(t, "127.0.0.1:9002"), core.PeerStateConnected)
	last := host.AddPeer(mustAddress(t, "127.0.0.1:9003"), core.PeerStateConnected)
	require.NoError(t, host.Send(first, 0, &core.Packet{Data: []byte("one")}))
	require.NoError(t, host.Send(last, 0, &core.Packet{Data: []byte("two"), Flags: core.PacketFlagReliable}))

	require.NoError(t, host.Flush(context.Background()))

	require.Equal(t, 2, sock.WriteCount())
	require.Equal(t, first.Address, sock.MustWrite(t, 0).Addr)
	require.Equal(t, last.Address, sock.MustWrite(t, 1).Addr)
	require.Zero(t, first.OutgoingCommands.Len())
	require.Zero(t, last.OutgoingSendReliableCommands.Len())
	require.Equal(t, 1, last.SentReliableCommands.Len())
	require.False(t, hasQueuedOutgoing(idle))
}

func TestHasQueuedOutgoing(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, host *Host, raw *ipeer.Peer)
		want    bool
	}{
		{
			name:    "idle peer",
			arrange: func(*testing.T, *Host, *ipeer.Peer) {},
			want:    false,
		},
		{
			name: "pending acknowledgement",
			arrange: func(_ *testing.T, _ *Host, raw *ipeer.Peer) {
				queueTestAcknowledgement(raw, 1)
			},
			want: true,
		},
		{
			name: "queued reliable packet",
			arrange: func(t *testing.T, host *Host, raw *ipeer.Peer) {
				require.NoError(t, host.Send(raw, 0, &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable}))
			},
			want: true,
		},
		{
			name: "queued unreliable packet",
			arrange: func(t *testing.T, host *Host, raw *ipeer.Peer) {
				require.NoError(t, host.Send(raw, 0, &core.Packet{Data: []byte("abc")}))
			},
			want: true,
		},
		{
			name: "only in-flight reliable commands",
			arrange: func(t *testing.T, host *Host, raw *ipeer.Peer) {
				require.NoError(t, host.Send(raw, 0, &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable}))
				require.NoError(t, host.Flush(context.Background()))
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, _ := newTestHost(t)
			raw := mustConnectedPeer(t, host)
			tt.arrange(t, host, raw)

			require.Equal(t, tt.want, hasQueuedOutgoing(raw))
		})
	}
}

func BenchmarkFlush_IdleHost(b *testing.B) {
	host := NewHost(core.Config{PeerCount: flushBenchmarkPeerCount, ChannelLimit: 1}, discardSocket{}, 77)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := host.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlush_OneBusyPeer(b *testing.B) {
	host := NewHost(core.Config{PeerCount: flushBenchmarkPeerCount, ChannelLimit: 1}, discardSocket{}, 77)
	address, err := core.NewAddress(netip.MustParseAddrPort("127.0.0.1:9001"), 0)
	if err != nil {
		b.Fatal(err)
	}
	raw := host.AddPeer(address, core.PeerStateConnected)
	payload := []byte("0123456789abcdef")
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		for range 8 {
			if err := host.Send(raw, 0, &core.Packet{Data: payload}); err != nil {
				b.Fatal(err)
			}
		}
		if err := host.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func queueTestAcknowledgement(p *ipeer.Peer, reliableSequenceNumber uint16) {
	p.Acknowledgements.PushBack(&ipeer.Acknowledgement{
		SentTime: 42,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: reliableSequenceNumber,
			},
		},
	})
}

// discardSocket accepts every write without retaining it so benchmarks and
// allocation tests measure only engine allocations.
type discardSocket struct{}

func (discardSocket) ReadPacket(context.Context, []byte) (int, core.Address, error) {
	return 0, core.Address{}, io.EOF
}

func (discardSocket) WritePacket(_ context.Context, _ core.Address, payload []byte) (int, error) {
	return len(payload), nil
}

func (discardSocket) Close() error {
	return nil
}
