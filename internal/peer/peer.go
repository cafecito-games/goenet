package peer

import "github.com/cafecito-games/goenet/internal/core"

// Peer carries the internal ENet-oriented state for a remote endpoint.
type Peer struct {
	OutgoingReliableSequenceNumber uint16
	OutgoingUnsequencedGroup       uint16
	OutgoingPeerID                 uint16
	IncomingPeerID                 uint16
	ConnectID                      uint32
	OutgoingSessionID              uint8
	IncomingSessionID              uint8
	MTU                            uint32
	Address                        core.Address
	State                          core.PeerState
	Channels                       []Channel
	IncomingBandwidth              uint32
	OutgoingBandwidth              uint32
	IncomingDataTotal              uint32
	OutgoingDataTotal              uint32
	IncomingBandwidthThrottleEpoch uint32
	OutgoingBandwidthThrottleEpoch uint32
	LastSendTime                   uint32
	LastReceiveTime                uint32
	NextTimeout                    uint32
	EarliestTimeout                uint32
	PacketsLost                    uint32
	TotalPacketsLost               uint32
	PacketThrottle                 uint32
	PacketThrottleLimit            uint32
	PacketThrottleCounter          uint32
	PacketThrottleEpoch            uint32
	PacketThrottleAcceleration     uint32
	PacketThrottleDeceleration     uint32
	PacketThrottleInterval         uint32
	TimeoutLimit                   uint32
	TimeoutMinimum                 uint32
	TimeoutMaximum                 uint32
	LastRoundTripTime              uint32
	LowestRoundTripTime            uint32
	LastRoundTripTimeVariance      uint32
	HighestRoundTripTimeVariance   uint32
	RoundTripTime                  uint32
	RoundTripTimeVariance          uint32
	ReliableDataInTransit          uint32
	Acknowledgements               acknowledgementQueue
	OutgoingCommands               outgoingQueue
	OutgoingSendReliableCommands   outgoingQueue
	SentReliableCommands           outgoingQueue
	DispatchedCommands             incomingQueue
	IncomingUnsequencedGroup       uint16
	UnsequencedWindow              [32]uint32
	TotalWaitingData               uint32
}

// QueueDispatchedCommand appends a receive command to the peer dispatch queue.
func (p *Peer) QueueDispatchedCommand(cmd *IncomingCommand) *ListElement[*IncomingCommand] {
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
