package goenet

import "github.com/cafecito-games/goenet/internal/core"

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
	state PeerState
}

// State returns the current peer state snapshot.
func (p *Peer) State() PeerState {
	return p.state
}

func toCorePeerState(state PeerState) core.PeerState {
	return core.PeerState(state)
}

func fromCorePeerState(state core.PeerState) PeerState {
	return PeerState(state)
}
