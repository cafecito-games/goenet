// Package socket provides the narrow datagram I/O abstraction used by the host runtime.
package socket

import (
	"context"

	"github.com/cafecito-games/goenet/internal/core"
)

// DatagramSocket is the narrow packet I/O surface the engine needs.
type DatagramSocket interface {
	ReadPacket(ctx context.Context, buf []byte) (int, core.Address, error)
	WritePacket(ctx context.Context, addr core.Address, payload []byte) (int, error)
	Close() error
}
