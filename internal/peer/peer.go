package peer

import (
	"github.com/cafecito-games/goenet"
)

// Peer carries the internal ENet-oriented state for a remote endpoint.
type Peer struct {
	OutgoingReliableSequenceNumber uint16
	OutgoingPeerID                 uint16
	IncomingPeerID                 uint16
	ConnectID                      uint32
	OutgoingSessionID              uint8
	IncomingSessionID              uint8
	MTU                            uint32
	Address                        goenet.Address
	State                          goenet.PeerState
	Channels                       []Channel
	IncomingBandwidth              uint32
	OutgoingBandwidth              uint32
	Acknowledgements               acknowledgementQueue
	OutgoingCommands               outgoingQueue
	OutgoingSendReliableCommands   outgoingQueue
	SentReliableCommands           outgoingQueue
	DispatchedCommands             incomingQueue
	TotalWaitingData               uint32
}

// QueueDispatchedCommand appends a receive command to the peer dispatch queue.
func (p *Peer) QueueDispatchedCommand(cmd *IncomingCommand) *listElement[*IncomingCommand] {
	return p.DispatchedCommands.PushBack(cmd)
}

// PopDispatchedCommand removes the next receive command scheduled for dispatch.
func (p *Peer) PopDispatchedCommand() *IncomingCommand {
	front := p.DispatchedCommands.Front()
	if front == nil {
		return nil
	}

	return p.DispatchedCommands.Remove(front)
}

// CanQueueWaitingData reports whether another packet fits under the configured waiting-data cap.
func (p *Peer) CanQueueWaitingData(length, maximumWaitingData uint32) bool {
	if length > maximumWaitingData {
		return false
	}

	return p.TotalWaitingData <= maximumWaitingData-length
}

// AddWaitingData increments the peer's queued receive-byte count.
func (p *Peer) AddWaitingData(length uint32) {
	p.TotalWaitingData += length
}

// ReleaseWaitingData decrements the peer's queued receive-byte count without underflow.
func (p *Peer) ReleaseWaitingData(length uint32) {
	if length >= p.TotalWaitingData {
		p.TotalWaitingData = 0
		return
	}

	p.TotalWaitingData -= length
}
