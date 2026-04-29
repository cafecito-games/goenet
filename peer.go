package goenet

import (
	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
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
type Peer struct {
	host  *Host
	raw   *peer.Peer
	state PeerState
}

// State returns the current peer state snapshot.
func (p *Peer) State() PeerState {
	if p != nil && p.raw != nil {
		p.state = fromCorePeerState(p.raw.State)
	}

	return p.state
}

// Send queues a packet for transmission to the peer.
func (p *Peer) Send(channelID uint8, packet *Packet) error {
	if p == nil || p.host == nil || p.raw == nil {
		return ErrNilPeer
	}
	if p.host.closed.Load() {
		return ErrHostClosed
	}

	return p.host.engine.Send(p.raw, channelID, toCorePacket(packet))
}

// Disconnect queues a graceful ENet-compatible disconnect request.
func (p *Peer) Disconnect(data uint32) error {
	if p == nil || p.host == nil || p.raw == nil {
		return ErrNilPeer
	}
	if p.host.closed.Load() {
		return ErrHostClosed
	}
	if err := p.host.engine.Disconnect(p.raw, data); err != nil {
		return err
	}

	p.state = fromCorePeerState(p.raw.State)
	return nil
}

// DisconnectNow forcefully notifies the remote peer, flushes immediately, and resets locally.
func (p *Peer) DisconnectNow(data uint32) error {
	if p == nil || p.host == nil || p.raw == nil {
		return ErrNilPeer
	}
	if p.host.closed.Load() {
		return ErrHostClosed
	}
	if err := p.host.engine.DisconnectNow(p.raw, data); err != nil {
		return err
	}

	p.state = fromCorePeerState(p.raw.State)
	return nil
}

// DisconnectLater defers disconnect until outbound reliable work drains.
func (p *Peer) DisconnectLater(data uint32) error {
	if p == nil || p.host == nil || p.raw == nil {
		return ErrNilPeer
	}
	if p.host.closed.Load() {
		return ErrHostClosed
	}
	if err := p.host.engine.DisconnectLater(p.raw, data); err != nil {
		return err
	}

	p.state = fromCorePeerState(p.raw.State)
	return nil
}

// Reset immediately drops local peer state without a wire notification.
func (p *Peer) Reset() {
	if p == nil || p.host == nil || p.raw == nil {
		return
	}

	p.host.engine.Reset(p.raw)
	p.state = fromCorePeerState(p.raw.State)
}

func fromCorePeerState(state core.PeerState) PeerState {
	return PeerState(state)
}
