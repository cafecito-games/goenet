package peer

import (
	"github.com/cafecito-games/goenet"
)

// Peer carries the internal ENet-oriented state for a remote endpoint.
type Peer struct {
	OutgoingReliableSequenceNumber uint16
	MTU                            uint32
	Address                        goenet.Address
	State                          goenet.PeerState
	Channels                       []Channel
	Acknowledgements               acknowledgementQueue
	OutgoingCommands               outgoingQueue
	OutgoingSendReliableCommands   outgoingQueue
	SentReliableCommands           outgoingQueue
	DispatchedCommands             incomingQueue
}
