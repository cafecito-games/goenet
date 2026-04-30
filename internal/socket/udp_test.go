package socket

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
)

func TestReadPacketReturnsContextCanceledWithoutDeadline(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	sock := NewUDP(conn, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, _, err := sock.ReadPacket(ctx, make([]byte, 32))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadPacket() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ReadPacket() did not return promptly for canceled context")
	}
}

func TestWritePacketReturnsContextCanceledWithoutDeadline(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	sock := NewUDP(conn, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	addr, err := core.NewAddress(netip.MustParseAddrPort("127.0.0.1:9001"), 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = sock.WritePacket(ctx, addr, []byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WritePacket() error = %v, want %v", err, context.Canceled)
	}
}

func TestWritePacketLogsSocketComponentOnWriteError(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	wantErr := errors.New("write boom")
	socket := &UDP{
		logger: core.ComponentLogger(logger, "socket"),
		setWriteDeadline: func(time.Time) error {
			return nil
		},
		writeToUDP: func([]byte, *net.UDPAddr) (int, error) {
			return 0, wantErr
		},
	}

	_, err := socket.WritePacket(context.Background(), mustAddress(t, "127.0.0.1:9001"), []byte("abc"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("WritePacket() error = %v, want %v", err, wantErr)
	}
	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "socket write failed" &&
			r.Level == slog.LevelError &&
			r.Attrs["component"] == "socket" &&
			errors.Is(attrError(r.Attrs["err"]), wantErr)
	}) {
		t.Fatal("missing socket error log")
	}
}

func TestWritePacketSuppressesClosedConnErrorLog(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	socket := &UDP{
		logger: core.ComponentLogger(logger, "socket"),
		setWriteDeadline: func(time.Time) error {
			return net.ErrClosed
		},
		writeToUDP: func([]byte, *net.UDPAddr) (int, error) {
			t.Fatal("writeToUDP should not be called after closed-conn deadline error")
			return 0, nil
		},
	}

	_, err := socket.WritePacket(context.Background(), mustAddress(t, "127.0.0.1:9001"), []byte("abc"))
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("WritePacket() error = %v, want %v", err, net.ErrClosed)
	}
	if len(handler.snapshot()) != 0 {
		t.Fatal("expected closed connection errors to be silent")
	}
}

func TestReadPacketSuppressesConnRefusedWithDebugLog(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readCalls := 0
	socket := &UDP{
		logger: core.ComponentLogger(logger, "socket"),
		setReadDeadline: func(time.Time) error {
			return nil
		},
		readFromUDPAddrPort: func([]byte) (int, netip.AddrPort, error) {
			readCalls++
			if readCalls == 1 {
				return 0, netip.AddrPort{}, syscall.ECONNREFUSED
			}
			cancel()
			return 0, netip.AddrPort{}, timeoutError{}
		},
	}

	_, _, err := socket.ReadPacket(ctx, make([]byte, 32))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadPacket() error = %v, want %v", err, context.Canceled)
	}
	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "socket read ignored conn refused" &&
			r.Level == slog.LevelDebug &&
			r.Attrs["component"] == "socket" &&
			errors.Is(attrError(r.Attrs["err"]), syscall.ECONNREFUSED)
	}) {
		t.Fatal("missing debug log for suppressed conn refused")
	}
}

func TestAddressFromAddrPortPreservesNumericIPv6ScopeID(t *testing.T) {
	addr, err := AddressFromAddrPort(netip.MustParseAddrPort("[fe80::1%7]:7777"))
	if err != nil {
		t.Fatal(err)
	}

	if got := addr.AddrPort().String(); got != "[fe80::1]:7777" {
		t.Fatalf("AddrPort().String() = %q", got)
	}
	if got := addr.ScopeID(); got != 7 {
		t.Fatalf("ScopeID() = %d, want 7", got)
	}
}

func TestUDPAddrFromAddressRestoresNumericIPv6ScopeID(t *testing.T) {
	addr, err := core.NewAddress(netip.MustParseAddrPort("[fe80::1]:7777"), 7)
	if err != nil {
		t.Fatal(err)
	}

	udpAddr := UDPAddrFromAddress(addr)
	if got := udpAddr.IP.String(); got != "fe80::1" {
		t.Fatalf("UDPAddrFromAddress().IP = %q", got)
	}
	if udpAddr.Port != 7777 {
		t.Fatalf("UDPAddrFromAddress().Port = %d, want 7777", udpAddr.Port)
	}
	if udpAddr.Zone == "" {
		t.Fatal("UDPAddrFromAddress().Zone = empty, want populated scope zone")
	}
}

func mustAddress(t *testing.T, raw string) core.Address {
	t.Helper()

	addr, err := core.NewAddress(netip.MustParseAddrPort(raw), 0)
	if err != nil {
		t.Fatal(err)
	}

	return addr
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

func attrError(value any) error {
	err, _ := value.(error)
	return err
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
