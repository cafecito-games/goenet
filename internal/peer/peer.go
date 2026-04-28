package peer

import (
	"github.com/cafecito-games/goenet"
)

// Peer carries the internal ENet-oriented state for a remote endpoint.
type Peer struct {
	Address      goenet.Address
	State        goenet.PeerState
	Channels     []Channel
	Acks         acknowledgementQueue
	Outgoing     outgoingQueue
	Incoming     incomingQueue
	Dispatched   incomingQueue
	SentReliable outgoingQueue
}
