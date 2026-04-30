package core

import (
	"context"
	"log/slog"
	"testing"
)

func TestNormalizeLoggerReturnsProvidedLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(testWriter{t: t}, nil))

	if got := normalizeLogger(logger); got != logger {
		t.Fatalf("normalizeLogger() = %p, want %p", got, logger)
	}
}

func TestNormalizeLoggerReturnsUsableLoggerWhenNil(t *testing.T) {
	logger := normalizeLogger(nil)
	if logger == nil {
		t.Fatal("normalizeLogger(nil) returned nil")
	}

	if logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("normalizeLogger(nil) should return a disabled logger")
	}
	if logger.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("normalizeLogger(nil) should return a disabled logger")
	}
	if logger.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("normalizeLogger(nil) should return a disabled logger")
	}
}

func TestComponentLoggerKeepsNilLoggerDisabled(t *testing.T) {
	logger := ComponentLogger(nil, "host")

	if logger.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("ComponentLogger(nil, ...) should remain disabled")
	}
}

func TestComponentLoggerAddsComponentAttr(t *testing.T) {
	records := []capturedRecord{}
	handler := &captureHandler{records: &records}
	logger := ComponentLogger(slog.New(handler), "host")

	logger.Info("hello")

	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1", len(records))
	}
	if got := records[0].attrs["component"]; got != "host" {
		t.Fatalf("component attr = %v, want %q", got, "host")
	}
}

type capturedRecord struct {
	attrs map[string]any
}

type captureHandler struct {
	records   *[]capturedRecord
	baseAttrs []slog.Attr
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]any)
	for _, attr := range h.baseAttrs {
		attrs[attr.Key] = attr.Value.Any()
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	*h.records = append(*h.records, capturedRecord{attrs: attrs})
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cloned := &captureHandler{
		records:   h.records,
		baseAttrs: append(append([]slog.Attr(nil), h.baseAttrs...), attrs...),
	}
	return cloned
}

func (h *captureHandler) WithGroup(string) slog.Handler {
	return h
}

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(p []byte) (int, error) {
	if w.t != nil {
		w.t.Helper()
	}
	return len(p), nil
}
