package goenet

import "github.com/cafecito-games/goenet/internal/core"

// PeerState mirrors ENetPeerState ordinal values.
type PeerState = core.PeerState

const (
	PeerStateDisconnected            = core.PeerStateDisconnected
	PeerStateConnecting              = core.PeerStateConnecting
	PeerStateAcknowledgingConnect    = core.PeerStateAcknowledgingConnect
	PeerStateConnectionPending       = core.PeerStateConnectionPending
	PeerStateConnectionSucceeded     = core.PeerStateConnectionSucceeded
	PeerStateConnected               = core.PeerStateConnected
	PeerStateDisconnectLater         = core.PeerStateDisconnectLater
	PeerStateDisconnecting           = core.PeerStateDisconnecting
	PeerStateAcknowledgingDisconnect = core.PeerStateAcknowledgingDisconnect
	PeerStateZombie                  = core.PeerStateZombie
)

// Peer is the public handle for a remote endpoint.
type Peer struct {
	state PeerState
}

// State returns the current peer state snapshot.
func (p *Peer) State() PeerState {
	return p.state
}
