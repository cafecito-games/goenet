package goenet

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/engine"
	"github.com/cafecito-games/goenet/internal/peer"
	isocket "github.com/cafecito-games/goenet/internal/socket"
)

// ErrHostClosed is returned by host operations after Close has been called.
var ErrHostClosed = errors.New("goenet: host closed")

// ErrNilPeer is returned when a method is invoked on a nil or zero-value peer handle.
var ErrNilPeer = errors.New("goenet: nil peer")

// Host is the public root for ENet-compatible peer management.
//
// All public methods on Host (and methods on Peer that route through Host) are
// safe for concurrent use from multiple goroutines. Mutual exclusion is provided
// by an internal mutex held for the duration of each engine call. A goroutine
// blocked in Service holds the lock; concurrent Send/Disconnect/Broadcast calls
// will wait until that Service tick returns. Run Service in one goroutine and
// other operations in others if you want them to interleave.
type Host struct {
	mu        sync.Mutex
	config    Config
	logger    *slog.Logger
	localAddr net.Addr
	engine    *engine.Host
	peers     map[*peer.Peer]*Peer
	closed    atomic.Bool
	startTime time.Time
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

	return newHost(cfg, conn)
}

// NewHost creates a public host bound to an ephemeral local UDP port.
func NewHost(cfg Config) (*Host, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	return newHost(cfg, conn)
}

// Config returns the host configuration snapshot.
func (h *Host) Config() Config {
	return h.config
}

// LocalAddr returns the host's bound local network address when available.
//
// The returned net.Addr is a defensive copy so callers cannot mutate host state.
// Prefer LocalAddrPort for new code — netip.AddrPort is immutable and avoids the
// per-call allocation.
func (h *Host) LocalAddr() net.Addr {
	if h == nil {
		return nil
	}

	return cloneNetAddr(h.localAddr)
}

// LocalAddrPort returns the host's bound local address as an immutable netip.AddrPort.
func (h *Host) LocalAddrPort() netip.AddrPort {
	if h == nil {
		return netip.AddrPort{}
	}
	udpAddr, ok := h.localAddr.(*net.UDPAddr)
	if !ok || udpAddr == nil {
		return netip.AddrPort{}
	}
	addr, ok := netip.AddrFromSlice(udpAddr.IP)
	if !ok {
		return netip.AddrPort{}
	}
	if udpAddr.Zone != "" {
		addr = addr.WithZone(udpAddr.Zone)
	}
	port := udpAddr.Port
	if port < 0 || port > 0xFFFF {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(addr.Unmap(), uint16(port))
}

// Connect initiates an outbound ENet-compatible connection.
func (h *Host) Connect(addr string, channelCount uint8, data uint32) (*Peer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	address, err := core.AddressFromUDPAddr(udpAddr)
	if err != nil {
		return nil, err
	}

	if err := h.lockOpen(); err != nil {
		return nil, err
	}
	defer h.mu.Unlock()
	h.engine.SetServiceTime(h.nowMs())

	raw, err := h.engine.Connect(address, channelCount, data)
	if err != nil {
		return nil, err
	}

	return h.wrapPeer(raw), nil
}

// Service advances the host and returns the next translated public event.
//
// timeout bounds the time spent waiting for inbound datagrams when no work is
// otherwise pending. A zero or negative timeout polls without blocking. The
// caller's ctx still cancels the call; whichever fires first wins.
func (h *Host) Service(ctx context.Context, timeout time.Duration) (Event, error) {
	tickCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		tickCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := h.lockOpen(); err != nil {
		return Event{}, err
	}
	defer h.mu.Unlock()
	h.engine.SetServiceTime(h.nowMs())

	event, err := h.engine.Service(tickCtx, durationMillis(timeout))
	if err != nil {
		// A tick-scoped deadline simply marks the end of this Service call.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return Event{}, nil
		}
		return Event{}, h.normalizeRuntimeError(err)
	}

	return h.translateEvent(event), nil
}

// Flush writes any queued outbound data.
func (h *Host) Flush(ctx context.Context) error {
	if err := h.lockOpen(); err != nil {
		return err
	}
	defer h.mu.Unlock()
	h.engine.SetServiceTime(h.nowMs())

	return h.normalizeRuntimeError(h.engine.Flush(ctx))
}

// Broadcast queues a packet for every currently connected peer. Per-peer Send errors
// are collected and returned together via errors.Join; a partial fanout still attempts
// every peer rather than aborting on the first failure.
func (h *Host) Broadcast(channelID uint8, packet *Packet) error {
	corePacket := copyPacketIn(packet)

	if err := h.lockOpen(); err != nil {
		return err
	}
	defer h.mu.Unlock()
	h.engine.SetServiceTime(h.nowMs())
	if corePacket == nil {
		return ErrNilPacket
	}

	var errs []error
	for _, wrapped := range h.orderedPeers() {
		if wrapped.raw.State != core.PeerStateConnected {
			continue
		}
		if err := h.engine.Send(wrapped.raw, channelID, corePacket); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// BandwidthLimit updates the host's incoming/outgoing bandwidth caps and triggers
// per-peer throttle recomputation on the next service tick. A value of zero on
// either argument disables the corresponding limit.
func (h *Host) BandwidthLimit(incomingBandwidth, outgoingBandwidth uint32) error {
	if err := h.lockOpen(); err != nil {
		return err
	}
	defer h.mu.Unlock()
	h.engine.BandwidthLimit(incomingBandwidth, outgoingBandwidth)
	return nil
}

// Close releases the underlying UDP socket.
func (h *Host) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed.Load() {
		return nil
	}
	h.closed.Store(true)

	if err := h.engine.Close(); err != nil {
		// Caller is responsible for surfacing close errors to its observability
		// stack; logging here would double-report. Keep the success-path Debug
		// line so an operator still sees the lifecycle event.
		return err
	}

	h.logger.Debug("host closed")
	return nil
}

func (h *Host) lockOpen() error {
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return ErrHostClosed
	}
	return nil
}

func (h *Host) normalizeRuntimeError(err error) error {
	if err == nil {
		return nil
	}
	if h.closed.Load() && errors.Is(err, net.ErrClosed) {
		return ErrHostClosed
	}
	return err
}

func newHost(cfg Config, conn *net.UDPConn) (*Host, error) {
	sock := isocket.NewUDP(conn, cfg.Logger)
	host, err := newHostWithSocket(cfg, sock)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	host.localAddr = cloneNetAddr(conn.LocalAddr())
	host.logger.Debug("host started", "addr", host.localAddr)
	return host, nil
}

func newHostWithSocket(cfg Config, sock isocket.DatagramSocket) (*Host, error) {
	normalized, coreCfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	hostLogger := core.ComponentLogger(coreCfg.Logger, "host")

	return &Host{
		config:    normalized,
		logger:    hostLogger,
		engine:    engine.NewHost(coreCfg, sock, 0),
		peers:     make(map[*peer.Peer]*Peer),
		startTime: time.Now(),
	}, nil
}

// nowMs returns wall-clock milliseconds elapsed since host construction, narrowed
// to uint32. ENet's protocol fields, RTT math, and timeout windows all use uint32
// ms with overflow-safe comparisons; anchoring on startTime keeps the value small
// for the lifetime of the host while still tracking real elapsed time. After
// ~49.7 days the counter wraps, which the timeutil overflow-safe helpers handle.
//
// time.Since uses the monotonic clock when startTime carries a monotonic reading
// (which time.Now does), so the returned duration is always non-negative.
func (h *Host) nowMs() uint32 {
	return uint32(time.Since(h.startTime) / time.Millisecond) //nolint:gosec // intentional uint32 wrap; ENet ms math is overflow-safe.
}

func cloneNetAddr(addr net.Addr) net.Addr {
	if addr == nil {
		return nil
	}

	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return addr
	}
	if udpAddr == nil {
		return nil
	}

	cloned := *udpAddr
	if udpAddr.IP != nil {
		cloned.IP = append(net.IP(nil), udpAddr.IP...)
	}
	// Zone is a string (immutable), so the value-copy above already isolates it.
	return &cloned
}

// orderedPeers returns currently-wrapped peers in engine slot order so callers see
// deterministic iteration regardless of Go map randomization.
func (h *Host) orderedPeers() []*Peer {
	raws := h.engine.Peers()
	ordered := make([]*Peer, 0, len(raws))
	for _, raw := range raws {
		if raw == nil {
			continue
		}
		if wrapped, ok := h.peers[raw]; ok {
			ordered = append(ordered, wrapped)
		}
	}
	return ordered
}

func (h *Host) wrapPeer(raw *peer.Peer) *Peer {
	if raw == nil {
		return nil
	}

	if wrapped, ok := h.peers[raw]; ok {
		wrapped.raw = raw
		wrapped.state = raw.State
		return wrapped
	}

	wrapped := &Peer{
		host:  h,
		raw:   raw,
		state: raw.State,
	}
	h.peers[raw] = wrapped
	return wrapped
}

func (h *Host) translateEvent(event engine.Event) Event {
	wrapped := h.wrapPeer(event.Peer)
	out := Event{
		Type:      event.Type,
		Peer:      wrapped,
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    event.Packet,
	}
	// After surfacing a terminal peer event, drop the wrapper from the map so a
	// future re-use of the same engine peer slot allocates a fresh public Peer
	// rather than keeping the caller's stale handle bound to a new session.
	if event.Peer != nil && (event.Type == core.EventDisconnect || event.Type == core.EventDisconnectTimeout) {
		h.detachPeer(event.Peer, wrapped)
	}
	return out
}

// detachPeer invalidates a public handle after an engine slot has been reset.
// The caller must hold h.mu.
func (h *Host) detachPeer(raw *peer.Peer, wrapped *Peer) {
	if wrapped != nil {
		wrapped.raw = nil
		wrapped.state = PeerStateDisconnected
	}
	if raw != nil {
		delete(h.peers, raw)
	}
}

func durationMillis(timeout time.Duration) uint32 {
	if timeout <= 0 {
		return 0
	}

	if timeout/time.Millisecond >= time.Duration(math.MaxUint32) {
		return math.MaxUint32
	}

	millis := timeout / time.Millisecond
	if millis == 0 {
		return 1
	}
	return uint32(millis)
}
