package goenet

import (
	"fmt"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
)

// PeerState mirrors ENetPeerState ordinal values.
type PeerState uint8

const (
	PeerStateDisconnected PeerState = iota
	PeerStateConnecting
	PeerStateAcknowledgingConnect
	PeerStateConnectionPending
	PeerStateConnectionSucceeded
	PeerStateConnected
	PeerStateDisconnectLater
	PeerStateDisconnecting
	PeerStateAcknowledgingDisconnect
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
		return fmt.Errorf("goenet: nil peer")
	}
	if p.host.closed.Load() {
		return errHostClosed
	}

	return p.host.engine.Send(p.raw, channelID, toCorePacket(packet))
}

// Disconnect queues a graceful ENet-compatible disconnect request.
func (p *Peer) Disconnect(data uint32) error {
	if p == nil || p.host == nil || p.raw == nil {
		return fmt.Errorf("goenet: nil peer")
	}
	if p.host.closed.Load() {
		return errHostClosed
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
		return fmt.Errorf("goenet: nil peer")
	}
	if p.host.closed.Load() {
		return errHostClosed
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
		return fmt.Errorf("goenet: nil peer")
	}
	if p.host.closed.Load() {
		return errHostClosed
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

func toCorePeerState(state PeerState) core.PeerState {
	return core.PeerState(state)
}

func fromCorePeerState(state core.PeerState) PeerState {
	return PeerState(state)
}
