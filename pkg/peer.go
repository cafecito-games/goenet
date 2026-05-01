package goenet

import (
	"context"
	"net"
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	isocket "github.com/cafecito-games/goenet/internal/socket"
)

// PeerState mirrors ENetPeerState ordinal values.
type PeerState uint8

const (
	// PeerStateDisconnected reports that no live session exists for the peer.
	PeerStateDisconnected PeerState = iota
	// PeerStateConnecting reports that an outbound connect command was queued.
	PeerStateConnecting
	// PeerStateAcknowledgingConnect reports that the peer is acknowledging an inbound connect.
	PeerStateAcknowledgingConnect
	// PeerStateConnectionPending reports that the peer is waiting for verify-connect.
	PeerStateConnectionPending
	// PeerStateConnectionSucceeded reports that the connect handshake has succeeded locally.
	PeerStateConnectionSucceeded
	// PeerStateConnected reports that the peer is fully connected.
	PeerStateConnected
	// PeerStateDisconnectLater reports that disconnect is deferred until reliable queues drain.
	PeerStateDisconnectLater
	// PeerStateDisconnecting reports that a graceful disconnect is in progress.
	PeerStateDisconnecting
	// PeerStateAcknowledgingDisconnect reports that disconnect acknowledgment is pending.
	PeerStateAcknowledgingDisconnect
	// PeerStateZombie reports that the peer is awaiting local cleanup after disconnect.
	PeerStateZombie
)

// Peer is the public handle for a remote endpoint.
//
// Field access on Peer (raw, state) is always synchronized via the owning Host's
// mutex; never read p.raw or p.state outside that lock.
type Peer struct {
	host  *Host
	raw   *peer.Peer
	state PeerState
}

// lockedRaw acquires host.mu and returns the peer's currently-bound raw pointer.
// Returns ErrNilPeer if the peer has no host or raw binding. Callers must invoke
// the returned unlock func when done.
func (p *Peer) lockedRaw() (*peer.Peer, func(), error) {
	if p == nil || p.host == nil {
		return nil, func() {}, ErrNilPeer
	}
	p.host.mu.Lock()
	if p.host.closed.Load() {
		p.host.mu.Unlock()
		return nil, func() {}, ErrHostClosed
	}
	if p.raw == nil {
		p.host.mu.Unlock()
		return nil, func() {}, ErrNilPeer
	}
	return p.raw, p.host.mu.Unlock, nil
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
		p.state = fromCorePeerState(p.raw.State)
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
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return netip.AddrPort{}
	}
	defer unlock()
	return raw.Address.AddrPort()
}

// Send queues a packet for transmission to the peer.
func (p *Peer) Send(channelID uint8, packet *Packet) error {
	corePacket := toCorePacket(packet)
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	return p.host.engine.Send(raw, channelID, corePacket)
}

// Disconnect queues a graceful ENet-compatible disconnect request.
func (p *Peer) Disconnect(ctx context.Context, data uint32) error {
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.Disconnect(ctx, raw, data); err != nil {
		return err
	}
	p.state = fromCorePeerState(raw.State)
	return nil
}

// DisconnectNow forcefully notifies the remote peer, flushes immediately, and resets locally.
func (p *Peer) DisconnectNow(ctx context.Context, data uint32) error {
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.DisconnectNow(ctx, raw, data); err != nil {
		return err
	}
	p.state = fromCorePeerState(raw.State)
	return nil
}

// DisconnectLater defers disconnect until outbound reliable work drains.
func (p *Peer) DisconnectLater(ctx context.Context, data uint32) error {
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return err
	}
	defer unlock()
	p.host.engine.SetServiceTime(p.host.nowMs())
	if err := p.host.engine.DisconnectLater(ctx, raw, data); err != nil {
		return err
	}
	p.state = fromCorePeerState(raw.State)
	return nil
}

// Reset immediately drops local peer state without a wire notification.
func (p *Peer) Reset() {
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return
	}
	defer unlock()
	p.host.engine.Reset(raw)
	p.state = fromCorePeerState(raw.State)
}

func fromCorePeerState(state core.PeerState) PeerState {
	return PeerState(state)
}

func (p *Peer) remoteUDPAddr() *net.UDPAddr {
	raw, unlock, err := p.lockedRaw()
	if err != nil {
		return nil
	}
	defer unlock()
	return isocket.UDPAddrFromAddress(raw.Address)
}
