package goenet

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/engine"
	"github.com/cafecito-games/goenet/internal/peer"
	isocket "github.com/cafecito-games/goenet/internal/socket"
)

var errHostClosed = errors.New("goenet: host closed")

// Host is the public root for ENet-compatible peer management.
type Host struct {
	config    Config
	localAddr net.Addr
	socket    isocket.DatagramSocket
	engine    *engine.Host
	peers     map[*peer.Peer]*Peer
	closed    atomic.Bool
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

// LocalAddr returns the host's bound local network address when available.
func (h *Host) LocalAddr() net.Addr {
	if h == nil {
		return nil
	}

	return cloneNetAddr(h.localAddr)
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

// Broadcast queues a packet to all currently connected peers.
func (h *Host) Broadcast(channelID uint8, packet *Packet) error {
	if h.closed.Load() {
		return errHostClosed
	}

	corePacket := toCorePacket(packet)
	for _, wrapped := range h.peers {
		if wrapped == nil || wrapped.raw == nil {
			continue
		}
		if wrapped.raw.State != core.PeerStateConnected {
			continue
		}
		if err := h.engine.Send(wrapped.raw, channelID, corePacket); err != nil {
			return err
		}
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
	host := newHostWithSocket(cfg, sock)
	host.localAddr = cloneNetAddr(conn.LocalAddr())
	return host
}

func newHostWithSocket(cfg Config, sock isocket.DatagramSocket) *Host {
	coreCfg := toCoreConfig(cfg)
	normalized := fromCoreConfig(coreCfg)

	return &Host{
		config: normalized,
		socket: sock,
		engine: engine.NewHost(coreCfg, sock, 0),
		peers:  make(map[*peer.Peer]*Peer),
	}
}

func cloneNetAddr(addr net.Addr) net.Addr {
	if addr == nil {
		return nil
	}

	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return addr
	}

	cloned := *udpAddr
	if udpAddr.IP != nil {
		cloned.IP = append(net.IP(nil), udpAddr.IP...)
	}
	return &cloned
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
	return isocket.AddressFromUDPAddr(addr)
}
