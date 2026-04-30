package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/testsupport"
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
		return r.Attrs["component"] == "engine" && r.Message == "peer connect queued"
	}) {
		t.Fatal("missing engine connect log")
	}
}

type capturedRecord struct {
	Message string
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
