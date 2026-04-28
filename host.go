package goenet

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/cafecito-games/goenet/internal/protocol"
	isocket "github.com/cafecito-games/goenet/internal/socket"
)

var errHostClosed = errors.New("goenet: host closed")

// Host is the public root for ENet-compatible peer management.
type Host struct {
	config Config
	socket isocket.DatagramSocket
	closed atomic.Bool
}

// Listen creates a public host bound to addr.
func Listen(addr string, cfg Config) (*Host, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return newHost(cfg, conn), nil
}

// NewHost creates a public host bound to an ephemeral local UDP port.
func NewHost(cfg Config) (*Host, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	return newHost(cfg, conn), nil
}

// Config returns the host configuration snapshot.
func (h *Host) Config() Config {
	return h.config
}

// Flush writes any queued outbound data.
func (h *Host) Flush(ctx context.Context) error {
	if h.closed.Load() {
		return errHostClosed
	}

	return nil
}

// Close releases the underlying UDP socket.
func (h *Host) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}

	return h.socket.Close()
}

func newHost(cfg Config, conn *net.UDPConn) *Host {
	sock := isocket.NewUDP(conn)
	return &Host{
		config: normalizeConfig(cfg),
		socket: sock,
	}
}

func normalizeConfig(cfg Config) Config {
	normalized := DefaultConfig()
	if cfg.PeerCount != 0 {
		normalized.PeerCount = cfg.PeerCount
	}
	if cfg.ChannelLimit != 0 {
		normalized.ChannelLimit = cfg.ChannelLimit
	}
	if cfg.MTU != 0 {
		normalized.MTU = cfg.MTU
	}
	if cfg.MaximumPacketSize != 0 {
		normalized.MaximumPacketSize = cfg.MaximumPacketSize
	}
	if cfg.MaximumWaitingData != 0 {
		normalized.MaximumWaitingData = cfg.MaximumWaitingData
	}
	if cfg.Checksum != nil {
		normalized.Checksum = cfg.Checksum
	}
	if cfg.Compressor != nil {
		normalized.Compressor = cfg.Compressor
	}
	if cfg.Intercept != nil {
		normalized.Intercept = cfg.Intercept
	}
	if normalized.ChannelLimit == 0 {
		normalized.ChannelLimit = uint8(protocol.MaximumPeerID >> 4)
	}

	return normalized
}
