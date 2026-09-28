package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/testsupport"
	"github.com/stretchr/testify/require"
)

func TestPeerSessionIDForIndexUsesSeededRotation(t *testing.T) {
	tests := []struct {
		index int
		seed  uint8
		want  uint8
	}{
		{index: 0, seed: 0, want: 1},
		{index: 1, seed: 0, want: 2},
		{index: 2, seed: 0, want: 3},
		{index: 0, seed: 1, want: 2},
		{index: 1, seed: 1, want: 3},
		{index: 2, seed: 1, want: 1},
		{index: 0, seed: 2, want: 3},
		{index: 1, seed: 2, want: 1},
		{index: 2, seed: 2, want: 2},
	}

	for _, tt := range tests {
		if got := peerSessionIDForIndex(tt.index, tt.seed); got != tt.want {
			t.Fatalf("peerSessionIDForIndex(%d, %d) = %d, want %d", tt.index, tt.seed, got, tt.want)
		}
	}
}

func TestPeerGenerationAdvancesAcrossResetAndSlotReuse(t *testing.T) {
	sock := testsupport.NewFakeSocket()
	host := NewHost(core.Config{PeerCount: 1, ChannelLimit: 1}, sock, 77)
	raw := host.Peers()[0]
	initialGeneration := raw.Generation

	raw = host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	configuredGeneration := raw.Generation
	require.Greater(t, configuredGeneration, initialGeneration)

	raw.ConnectID = 0x11223344
	host.Reset(raw)
	require.Greater(t, raw.Generation, configuredGeneration)
	require.Equal(t, uint32(0x11223344), raw.ConnectID)
	resetGeneration := raw.Generation

	raw = host.AddPeer(mustAddress(t, "127.0.0.1:9002"), core.PeerStateConnected)
	require.Greater(t, raw.Generation, resetGeneration)
}

func TestCloseDelegatesToSocket(t *testing.T) {
	sock := testsupport.NewFakeSocket()
	host := NewHost(core.Config{PeerCount: 1, ChannelLimit: 1}, sock, 77)

	require.NoError(t, host.Close())
}

func TestConnectLogsEngineComponent(t *testing.T) {
	handler := newCaptureHandler()
	host := NewHost(core.Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       slog.New(handler),
	}, testsupport.NewFakeSocket(), 77)

	_, err := host.Connect(mustAddress(t, "127.0.0.1:9001"), 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Attrs["component"] == "engine" &&
			r.Level == slog.LevelDebug &&
			r.Message == "peer connect queued"
	}) {
		t.Fatal("missing engine connect log")
	}
}

func TestFlushBudgetExhaustionLogsWarn(t *testing.T) {
	handler := newCaptureHandler()
	sock := testsupport.NewFakeSocket()
	host := NewHost(core.Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       slog.New(handler),
	}, sock, 77)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)

	packetSize := host.maxPacketDataLength(raw, core.PacketFlagReliable)
	for i := 0; i < maximumDatagramsPerPeerFlush+1; i++ {
		if err := host.Send(raw, 0, &core.Packet{
			Flags: core.PacketFlagReliable,
			Data:  make([]byte, packetSize),
		}); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if sock.WriteCount() != maximumDatagramsPerPeerFlush {
		t.Fatalf("WriteCount() = %d, want %d", sock.WriteCount(), maximumDatagramsPerPeerFlush)
	}

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "flush budget exhausted" &&
			r.Level == slog.LevelWarn &&
			attrUint64(r.Attrs["peer_id"]) == uint64(raw.IncomingPeerID)
	}) {
		t.Fatal("missing flush budget warn log")
	}
}

func TestNotifyConnectLogsReadablePeerStates(t *testing.T) {
	handler := newCaptureHandler()
	host := NewHost(core.Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       slog.New(handler),
	}, testsupport.NewFakeSocket(), 77)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnecting)

	host.notifyConnect(raw)

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "peer connect transition" &&
			r.Level == slog.LevelDebug &&
			attrUint64(r.Attrs["peer_id"]) == uint64(raw.IncomingPeerID) &&
			r.Attrs["from_state"] == "connecting" &&
			r.Attrs["to_state"] == "connection_succeeded"
	}) {
		t.Fatal("missing readable peer connect transition log")
	}
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

func attrUint64(value any) uint64 {
	switch v := value.(type) {
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case int:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	default:
		return 0
	}
}
