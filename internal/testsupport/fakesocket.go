// Package testsupport provides deterministic helpers for internal tests.
package testsupport

import (
	"context"
	"io"
	"testing"

	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/socket"
)

// SocketWrite captures one outbound datagram.
type SocketWrite struct {
	Addr    core.Address
	Payload []byte
}

// SocketRead captures one scripted inbound datagram or read error.
type SocketRead struct {
	Addr    core.Address
	Payload []byte
	Err     error
}

// FakeSocket is a deterministic socket stub for engine tests.
type FakeSocket struct {
	reads    []SocketRead
	writes   []SocketWrite
	writeErr error
}

// NewFakeSocket returns an empty scripted socket stub.
func NewFakeSocket() *FakeSocket {
	return &FakeSocket{}
}

// ReadPacket returns the next scripted inbound datagram or error.
func (s *FakeSocket) ReadPacket(ctx context.Context, buf []byte) (int, core.Address, error) {
	select {
	case <-ctx.Done():
		return 0, core.Address{}, ctx.Err()
	default:
	}

	if len(s.reads) == 0 {
		return 0, core.Address{}, io.EOF
	}

	read := s.reads[0]
	s.reads = s.reads[1:]
	if read.Err != nil {
		return 0, core.Address{}, read.Err
	}

	n := copy(buf, read.Payload)
	if n < len(read.Payload) {
		return n, read.Addr, io.ErrShortBuffer
	}

	return n, read.Addr, nil
}

// WritePacket records one outbound datagram unless a scripted write error is set.
func (s *FakeSocket) WritePacket(ctx context.Context, addr core.Address, payload []byte) (int, error) {
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

// Close satisfies the socket interface for tests.
func (s *FakeSocket) Close() error {
	return nil
}

// WriteCount reports how many outbound datagrams have been recorded.
func (s *FakeSocket) WriteCount() int {
	return len(s.writes)
}

// SetWriteError makes future writes fail with err.
func (s *FakeSocket) SetWriteError(err error) {
	s.writeErr = err
}

// QueueInbound appends one scripted inbound datagram.
func (s *FakeSocket) QueueInbound(addr netip.AddrPort, payload []byte) {
	coreAddr, err := socket.AddressFromAddrPort(addr)
	if err != nil {
		panic(err)
	}
	s.QueueInboundAddress(coreAddr, payload)
}

// QueueInboundAddress appends one scripted inbound datagram with explicit scope metadata.
func (s *FakeSocket) QueueInboundAddress(addr core.Address, payload []byte) {
	copyPayload := append([]byte(nil), payload...)
	s.reads = append(s.reads, SocketRead{
		Addr:    addr,
		Payload: copyPayload,
	})
}

// QueueReadError appends one scripted read failure.
func (s *FakeSocket) QueueReadError(err error) {
	s.reads = append(s.reads, SocketRead{Err: err})
}

// MustWrite returns the recorded write at index or fails the test.
func (s *FakeSocket) MustWrite(t *testing.T, index int) SocketWrite {
	t.Helper()

	if index < 0 || index >= len(s.writes) {
		t.Fatalf("write index %d out of range", index)
	}

	return s.writes[index]
}
