package goenet

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
