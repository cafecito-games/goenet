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

// SocketRead captures one scripted inbound datagram or read error.
type SocketRead struct {
	Addr    netip.AddrPort
	Payload []byte
	Err     error
}

// FakeSocket is a deterministic socket stub for engine tests.
type FakeSocket struct {
	reads    []SocketRead
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
	}

	if len(s.reads) == 0 {
		return 0, netip.AddrPort{}, io.EOF
	}

	read := s.reads[0]
	s.reads = s.reads[1:]
	if read.Err != nil {
		return 0, netip.AddrPort{}, read.Err
	}

	n := copy(buf, read.Payload)
	if n < len(read.Payload) {
		return n, read.Addr, io.ErrShortBuffer
	}

	return n, read.Addr, nil
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

func (s *FakeSocket) QueueInbound(addr netip.AddrPort, payload []byte) {
	copyPayload := append([]byte(nil), payload...)
	s.reads = append(s.reads, SocketRead{
		Addr:    addr,
		Payload: copyPayload,
	})
}

func (s *FakeSocket) QueueReadError(err error) {
	s.reads = append(s.reads, SocketRead{Err: err})
}

func (s *FakeSocket) MustWrite(t *testing.T, index int) SocketWrite {
	t.Helper()

	if index < 0 || index >= len(s.writes) {
		t.Fatalf("write index %d out of range", index)
	}

	return s.writes[index]
}
