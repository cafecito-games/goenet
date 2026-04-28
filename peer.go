package goenet

import (
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

func toCorePeerState(state PeerState) core.PeerState {
	return core.PeerState(state)
}

func fromCorePeerState(state core.PeerState) PeerState {
	return PeerState(state)
}
