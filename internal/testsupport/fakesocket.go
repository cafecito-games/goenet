package testsupport

import (
	"context"
	"io"
	"net/netip"
	"testing"
)

// SocketWrite captures one outbound datagram.
type SocketWrite struct {
	Addr    netip.AddrPort
	Payload []byte
}

// FakeSocket is a deterministic socket stub for engine tests.
type FakeSocket struct {
	writes   []SocketWrite
	writeErr error
}

func NewFakeSocket() *FakeSocket {
	return &FakeSocket{}
}

func (s *FakeSocket) ReadPacket(ctx context.Context, buf []byte) (int, netip.AddrPort, error) {
	select {
	case <-ctx.Done():
		return 0, netip.AddrPort{}, ctx.Err()
	default:
		return 0, netip.AddrPort{}, io.EOF
	}
}

func (s *FakeSocket) WritePacket(ctx context.Context, addr netip.AddrPort, payload []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if s.writeErr != nil {
		return 0, s.writeErr
	}

	copyPayload := append([]byte(nil), payload...)
	s.writes = append(s.writes, SocketWrite{
		Addr:    addr,
		Payload: copyPayload,
	})
	return len(payload), nil
}

func (s *FakeSocket) Close() error {
	return nil
}

func (s *FakeSocket) WriteCount() int {
	return len(s.writes)
}

func (s *FakeSocket) SetWriteError(err error) {
	s.writeErr = err
}

func (s *FakeSocket) MustWrite(t *testing.T, index int) SocketWrite {
	t.Helper()

	if index < 0 || index >= len(s.writes) {
		t.Fatalf("write index %d out of range", index)
	}

	return s.writes[index]
}
