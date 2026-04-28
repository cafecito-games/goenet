package peer

import (
	"github.com/cafecito-games/goenet"
)

// Peer carries the internal ENet-oriented state for a remote endpoint.
type Peer struct {
	Address                      goenet.Address
	State                        goenet.PeerState
	Channels                     []Channel
	Acknowledgements             acknowledgementQueue
	OutgoingCommands             outgoingQueue
	OutgoingSendReliableCommands outgoingQueue
	SentReliableCommands         outgoingQueue
	SentUnreliableCommands       outgoingQueue
	DispatchedCommands           incomingQueue
}
