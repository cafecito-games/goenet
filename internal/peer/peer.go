package peer

import (
	"math"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
)

// Compile-time guarantee that the peer's UnsequencedWindow bitmap matches
// ENet's documented unsequenced window size (1024 bits / 32 uint32s).
const _ = uint(len(Peer{}.UnsequencedWindow)*32) - uint(protocol.UnsequencedWindowSize)
const _ = uint(protocol.UnsequencedWindowSize) - uint(len(Peer{}.UnsequencedWindow)*32)

// Peer carries the internal ENet-oriented state for a remote endpoint.
//
// Peer is intentionally treated by the engine as a struct-of-fields data layer
// rather than as a fully encapsulated state machine: the engine reaches in to
// increment sequence counters and update window state directly, because every
// such mutation is paired with engine-side bookkeeping (RTT, throttle, bandwidth,
// queue order) that does not factor cleanly through narrow accessor methods.
// The methods that DO live on Peer are reserved for invariants that span more
// than a single field — queue-and-index pairing for sent reliable commands,
// waiting-data saturation arithmetic, queue resets — and the engine MUST go
// through them rather than poking the underlying maps and queues itself.
type Peer struct {
	OutgoingReliableSequenceNumber uint16
	OutgoingUnsequencedGroup       uint16
	OutgoingPeerID                 uint16
	IncomingPeerID                 uint16
	ConnectID                      uint32
	// Generation is a host-local slot incarnation. It changes every time the
	// engine reinitializes this peer storage and is never serialized on the wire.
	Generation                     uint64
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

// ResetQueues clears the per-peer command queues, channel slice, and waiting-data
// counter used during a Disconnect or DisconnectNow. Identity, address, MTU,
// timing, throttle, and bandwidth fields are left intact so the engine can still
// emit the outgoing disconnect command and continue retransmit/ack accounting
// until the peer is fully zombified. Callers must remove the peer from any
// host-level dispatch queue separately; that is engine-owned state.
func (p *Peer) ResetQueues() {
	p.Acknowledgements = acknowledgementQueue{}
	p.OutgoingCommands = outgoingQueue{}
	p.OutgoingSendReliableCommands = outgoingQueue{}
	p.SentReliableCommands = outgoingQueue{}
	p.sentReliableIndex = nil
	p.DispatchedCommands = incomingQueue{}
	p.Channels = nil
	p.TotalWaitingData = 0
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
	if length > math.MaxUint32-p.TotalWaitingData {
		p.TotalWaitingData = math.MaxUint32
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
