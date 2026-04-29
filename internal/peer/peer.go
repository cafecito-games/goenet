package peer

import (
	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
)

// Compile-time guarantee that the peer's UnsequencedWindow bitmap matches
// ENet's documented unsequenced window size (1024 bits / 32 uint32s).
const _ = uint(len(Peer{}.UnsequencedWindow)*32) - uint(protocol.UnsequencedWindowSize)
const _ = uint(protocol.UnsequencedWindowSize) - uint(len(Peer{}.UnsequencedWindow)*32)

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
	Channels                       []*Channel
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
	sentReliableIndex              map[sentReliableKey]*ListElement[*OutgoingCommand]
	DispatchedCommands             incomingQueue
	IncomingUnsequencedGroup       uint16
	UnsequencedWindow              [32]uint32
	TotalWaitingData               uint32
}

type sentReliableKey struct {
	reliableSequenceNumber uint16
	channelID              uint8
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

// IndexSentReliableCommand records the list element for O(1) ACK removal lookups.
func (p *Peer) IndexSentReliableCommand(elem *ListElement[*OutgoingCommand]) {
	if elem == nil {
		return
	}
	if p.sentReliableIndex == nil {
		p.sentReliableIndex = make(map[sentReliableKey]*ListElement[*OutgoingCommand])
	}
	cmd := elem.Value()
	p.sentReliableIndex[sentReliableKey{
		reliableSequenceNumber: cmd.ReliableSequenceNumber,
		channelID:              cmd.Command.Header.ChannelID,
	}] = elem
}

// RemoveIndexedSentReliableCommand removes and returns an indexed in-flight reliable command entry.
func (p *Peer) RemoveIndexedSentReliableCommand(reliableSequenceNumber uint16, channelID uint8) *ListElement[*OutgoingCommand] {
	if p.sentReliableIndex == nil {
		return nil
	}
	key := sentReliableKey{reliableSequenceNumber: reliableSequenceNumber, channelID: channelID}
	elem := p.sentReliableIndex[key]
	delete(p.sentReliableIndex, key)
	return elem
}

// UnindexSentReliableCommand removes the index entry for cmd when it leaves the in-flight queue.
func (p *Peer) UnindexSentReliableCommand(cmd *OutgoingCommand) {
	if p.sentReliableIndex == nil || cmd == nil {
		return
	}
	delete(p.sentReliableIndex, sentReliableKey{
		reliableSequenceNumber: cmd.ReliableSequenceNumber,
		channelID:              cmd.Command.Header.ChannelID,
	})
}

// CanQueueWaitingData reports whether another packet fits under the configured waiting-data cap.
func (p *Peer) CanQueueWaitingData(length, maximumWaitingData uint32) bool {
	if length > maximumWaitingData {
		return false
	}

	return p.TotalWaitingData <= maximumWaitingData-length
}

// AddWaitingData increments the peer's queued receive-byte count, saturating at
// math.MaxUint32 rather than wrapping. Callers should gate on CanQueueWaitingData
// first; the saturation here defends against drift if that contract is missed.
func (p *Peer) AddWaitingData(length uint32) {
	if length > ^uint32(0)-p.TotalWaitingData {
		p.TotalWaitingData = ^uint32(0)
		return
	}
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
