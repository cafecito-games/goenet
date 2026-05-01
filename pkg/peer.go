package goenet

import (
	"context"
	"net"
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
)

// PeerState mirrors ENetPeerState ordinal values.
type PeerState = core.PeerState

const (
	// PeerStateDisconnected reports that no live session exists for the peer.
	PeerStateDisconnected = core.PeerStateDisconnected
	// PeerStateConnecting reports that an outbound connect command was queued.
	PeerStateConnecting = core.PeerStateConnecting
	// PeerStateAcknowledgingConnect reports that the peer is acknowledging an inbound connect.
	PeerStateAcknowledgingConnect = core.PeerStateAcknowledgingConnect
	// PeerStateConnectionPending reports that the peer is waiting for verify-connect.
	PeerStateConnectionPending = core.PeerStateConnectionPending
	// PeerStateConnectionSucceeded reports that the connect handshake has succeeded locally.
	PeerStateConnectionSucceeded = core.PeerStateConnectionSucceeded
	// PeerStateConnected reports that the peer is fully connected.
	PeerStateConnected = core.PeerStateConnected
	// PeerStateDisconnectLater reports that disconnect is deferred until reliable queues drain.
	PeerStateDisconnectLater = core.PeerStateDisconnectLater
	// PeerStateDisconnecting reports that a graceful disconnect is in progress.
	PeerStateDisconnecting = core.PeerStateDisconnecting
	// PeerStateAcknowledgingDisconnect reports that disconnect acknowledgment is pending.
	PeerStateAcknowledgingDisconnect = core.PeerStateAcknowledgingDisconnect
	// PeerStateZombie reports that the peer is awaiting local cleanup after disconnect.
	PeerStateZombie = core.PeerStateZombie
)

// Peer is the public handle for a remote endpoint.
//
// # Lifecycle
//
// A Peer becomes visible to a caller through one of:
//   - Host.Connect, which returns the new outbound peer.
//   - Service-loop EventConnect, which surfaces an inbound peer that just
//     completed the ENet connect handshake.
//
// The handle remains valid up to and including the EventDisconnect or
// EventDisconnectTimeout event for that peer. Once that terminal event is
// returned from Service, the host detaches its internal binding from the
// handle. Subsequent operations on the handle (Send, Disconnect*, Reset)
// return ErrNilPeer; State returns PeerStateDisconnected. The handle is then
// safe to drop. Re-using the same engine peer slot for a new session
// allocates a fresh public Peer; there is no automatic re-binding of stale
// handles.
//
// # Concurrency
//
// All public methods on Peer are safe for concurrent use. Field access on
// Peer (raw, state) is synchronized via the owning Host's mutex; never read
// p.raw or p.state outside that lock.
//
// state is a cached copy of raw.State. It survives the moment translateEvent
// clears raw on a terminal event so callers holding the handle past that
// event still see PeerStateDisconnected from State() instead of a misleading
// default. While raw is non-nil, State() reads from it directly and refreshes
// the cache as a side effect.
type Peer struct {
	host  *Host
	raw   *peer.Peer
	state PeerState
}

// lockedRaw acquires host.mu and returns the peer's currently-bound raw pointer.
// On success the lock is held and the caller must `defer p.host.mu.Unlock()`.
// Returns ErrHostClosed or ErrNilPeer with the lock released on the error path.
func (p *Peer) lockedRaw() (*peer.Peer, error) {
	if p == nil || p.host == nil {
		return nil, ErrNilPeer
	}
	p.host.mu.Lock()
	if p.host.closed.Load() {
		p.host.mu.Unlock()
		return nil, ErrHostClosed
	}
	if p.raw == nil {
		p.host.mu.Unlock()
		return nil, ErrNilPeer
	}
	return p.raw, nil
}

// State returns the current peer state snapshot. Safe for concurrent use.
func (p *Peer) State() PeerState {
	if p == nil {
		return PeerStateDisconnected
	}
	if p.host == nil {
		return p.state
	}
	p.host.mu.Lock()
	defer p.host.mu.Unlock()
	if p.raw != nil {
		p.state = p.raw.State
	}
	return p.state
}

// RemoteAddr returns the peer's remote network address when available.
//
// The returned net.Addr is a defensive copy so callers cannot mutate peer state.
// Prefer RemoteAddrPort for new code when you want an immutable address value.
func (p *Peer) RemoteAddr() net.Addr {
	return cloneNetAddr(p.remoteUDPAddr())
}

// RemoteAddrPort returns the peer's remote address as an immutable netip.AddrPort.
func (p *Peer) RemoteAddrPort() netip.AddrPort {
	raw, err := p.lockedRaw()
	if err != nil {
		return netip.AddrPort{}
	}
	defer p.host.mu.Unlock()
	return raw.Address.AddrPort()
}

// Send queues a packet for transmission to the peer.
func (p *Peer) Send(channelID uint8, packet *Packet) error {
	corePacket := copyPacketIn(packet)
	raw, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer p.host.mu.Unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	return p.host.engine.Send(raw, channelID, corePacket)
}

// Disconnect queues a graceful ENet-compatible disconnect request.
func (p *Peer) Disconnect(ctx context.Context, data uint32) error {
	raw, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer p.host.mu.Unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.Disconnect(ctx, raw, data); err != nil {
		return err
	}
	p.state = raw.State
	return nil
}

// DisconnectNow forcefully notifies the remote peer, flushes immediately, and resets locally.
func (p *Peer) DisconnectNow(ctx context.Context, data uint32) error {
	raw, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer p.host.mu.Unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.DisconnectNow(ctx, raw, data); err != nil {
		return err
	}
	p.state = raw.State
	return nil
}

// DisconnectLater defers disconnect until outbound reliable work drains.
func (p *Peer) DisconnectLater(ctx context.Context, data uint32) error {
	raw, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer p.host.mu.Unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.DisconnectLater(ctx, raw, data); err != nil {
		return err
	}
	p.state = raw.State
	return nil
}

// Reset immediately drops local peer state without a wire notification.
func (p *Peer) Reset() {
	raw, err := p.lockedRaw()
	if err != nil {
		return
	}
	defer p.host.mu.Unlock()
	p.host.engine.Reset(raw)
	p.state = raw.State
}

func (p *Peer) remoteUDPAddr() *net.UDPAddr {
	raw, err := p.lockedRaw()
	if err != nil {
		return nil
	}
	defer p.host.mu.Unlock()
	return core.UDPAddrFromAddress(raw.Address)
}
