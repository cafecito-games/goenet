package goenet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/engine"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
	isocket "github.com/cafecito-games/goenet/internal/socket"
)

var errHostClosed = errors.New("goenet: host closed")

// Host is the public root for ENet-compatible peer management.
type Host struct {
	config Config
	socket isocket.DatagramSocket
	engine *engine.Host
	peers  map[*peer.Peer]*Peer
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

// Connect initiates an outbound ENet-compatible connection.
func (h *Host) Connect(addr string, channelCount uint8, data uint32) (*Peer, error) {
	if h.closed.Load() {
		return nil, errHostClosed
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	address, err := coreAddressFromUDPAddr(udpAddr)
	if err != nil {
		return nil, err
	}

	raw, err := h.engine.Connect(address, channelCount, data)
	if err != nil {
		return nil, err
	}

	return h.wrapPeer(raw), nil
}

// Service advances the host and returns the next translated public event.
func (h *Host) Service(ctx context.Context, timeout time.Duration) (Event, error) {
	if h.closed.Load() {
		return Event{}, errHostClosed
	}

	event, err := h.engine.Service(ctx, durationMillis(timeout))
	if err != nil {
		return Event{}, err
	}

	return h.translateEvent(event), nil
}

// Flush writes any queued outbound data.
func (h *Host) Flush(ctx context.Context) error {
	if h.closed.Load() {
		return errHostClosed
	}

	return h.engine.Flush(ctx)
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
	return newHostWithSocket(cfg, sock)
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

func newHostWithSocket(cfg Config, sock isocket.DatagramSocket) *Host {
	normalized := normalizeConfig(cfg)

	return &Host{
		config: normalized,
		socket: sock,
		engine: engine.NewHost(toCoreConfig(normalized), sock, 0),
		peers:  make(map[*peer.Peer]*Peer),
	}
}

func (h *Host) wrapPeer(raw *peer.Peer) *Peer {
	if raw == nil {
		return nil
	}

	if wrapped, ok := h.peers[raw]; ok {
		wrapped.raw = raw
		wrapped.state = fromCorePeerState(raw.State)
		return wrapped
	}

	wrapped := &Peer{
		host:  h,
		raw:   raw,
		state: fromCorePeerState(raw.State),
	}
	h.peers[raw] = wrapped
	return wrapped
}

func (h *Host) translateEvent(event engine.Event) Event {
	return Event{
		Type:      EventType(event.Type),
		Peer:      h.wrapPeer(event.Peer),
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    fromCorePacket(event.Packet),
	}
}

func durationMillis(timeout time.Duration) uint32 {
	if timeout <= 0 {
		return 0
	}

	if timeout/time.Millisecond >= time.Duration(^uint32(0)) {
		return ^uint32(0)
	}

	return uint32(timeout / time.Millisecond)
}

func coreAddressFromUDPAddr(addr *net.UDPAddr) (core.Address, error) {
	addrPort := addr.AddrPort()
	addrPort = netip.AddrPortFrom(addrPort.Addr().Unmap(), addrPort.Port())
	return core.NewAddress(addrPort, 0)
}
