// Package socket provides the narrow datagram I/O abstraction used by the host runtime.
package socket

import (
	"context"
	"net/netip"
)

// DatagramSocket is the narrow packet I/O surface the engine needs.
type DatagramSocket interface {
	ReadPacket(ctx context.Context, buf []byte) (int, netip.AddrPort, error)
	WritePacket(ctx context.Context, addr netip.AddrPort, payload []byte) (int, error)
	Close() error
}
