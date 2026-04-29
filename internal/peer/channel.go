// Package peer holds the internal ENet peer, channel, and queue state machines.
package peer

import "math"

const reliableWindowCount = 16

// Channel tracks ENet sequence numbers for one logical peer channel.
type Channel struct {
	OutgoingReliableSequenceNumber   uint16
	OutgoingUnreliableSequenceNumber uint16
	UsedReliableWindows              uint16
	ReliableWindows                  [reliableWindowCount]uint16
	IncomingReliableSequenceNumber   uint16
	IncomingUnreliableSequenceNumber uint16
	IncomingReliableCommands         incomingQueue
	IncomingUnreliableCommands       incomingQueue
}

// NewChannel returns a zero-initialized channel state holder.
//
// Channels must be addressed by pointer because the queue insertion methods
// mutate per-channel state through pointer receivers. Callers should hold
// []*Channel slices, never []Channel values.
func NewChannel() *Channel {
	return &Channel{}
}

// InsertIncomingReliableOrdered only maintains ENet receive ordering.
// Duplicate, stale, and window validation stay in the engine receive path.
func (ch *Channel) InsertIncomingReliableOrdered(cmd *IncomingCommand) *ListElement[*IncomingCommand] {
	return ch.IncomingReliableCommands.InsertOrdered(cmd, func(a, b *IncomingCommand) bool {
		return incomingReliableLess(ch.IncomingReliableSequenceNumber, a, b)
	})
}

// InsertIncomingUnreliableOrdered only maintains ENet receive ordering.
// Duplicate, stale, and window validation stay in the engine receive path.
func (ch *Channel) InsertIncomingUnreliableOrdered(cmd *IncomingCommand) *ListElement[*IncomingCommand] {
	return ch.IncomingUnreliableCommands.InsertOrdered(cmd, func(a, b *IncomingCommand) bool {
		return incomingUnreliableLess(ch.IncomingReliableSequenceNumber, a, b)
	})
}

// MarkIncomingReliableDispatched advances the receive anchor after reliable delivery.
// Fragment trains consume the full reliable sequence span once reassembly completes.
func (ch *Channel) MarkIncomingReliableDispatched(cmd *IncomingCommand) {
	if cmd == nil {
		return
	}

	ch.IncomingReliableSequenceNumber = reliableDispatchAnchor(cmd)
	ch.IncomingUnreliableSequenceNumber = 0
}

// MarkIncomingUnreliableDispatched advances the receive anchor after unreliable delivery.
func (ch *Channel) MarkIncomingUnreliableDispatched(cmd *IncomingCommand) {
	if cmd == nil {
		return
	}

	ch.IncomingReliableSequenceNumber = cmd.ReliableSequenceNumber
	ch.IncomingUnreliableSequenceNumber = cmd.UnreliableSequenceNumber
}

func incomingReliableLess(anchor uint16, a, b *IncomingCommand) bool {
	return sequenceDistance(anchor, a.ReliableSequenceNumber) < sequenceDistance(anchor, b.ReliableSequenceNumber)
}

func incomingUnreliableLess(anchor uint16, a, b *IncomingCommand) bool {
	aReliable := sequenceDistance(anchor, a.ReliableSequenceNumber)
	bReliable := sequenceDistance(anchor, b.ReliableSequenceNumber)
	if aReliable != bReliable {
		return aReliable < bReliable
	}

	// Within one reliable group the unreliable sequence is a uint16 that can
	// wrap. A naive `<` misorders pairs straddling 0xFFFF/0x0000. int16-cast
	// difference gives the standard signed-wrap comparison and treats the
	// shorter modular distance as "earlier".
	diff := a.UnreliableSequenceNumber - b.UnreliableSequenceNumber
	return int16(diff) < 0 //nolint:gosec // intentional signed-wrap compare for uint16 sequence numbers.
}

func sequenceDistance(anchor, sequence uint16) uint32 {
	if sequence >= anchor {
		return uint32(sequence - anchor)
	}

	return uint32(sequence) + (1 << 16) - uint32(anchor)
}

func reliableDispatchAnchor(cmd *IncomingCommand) uint16 {
	if cmd == nil {
		return 0
	}
	if cmd.FragmentCount <= 1 {
		return cmd.ReliableSequenceNumber
	}

	sum := uint32(cmd.ReliableSequenceNumber) + (cmd.FragmentCount - 1)
	if sum > math.MaxUint16 {
		sum %= 1 << 16
	}

	return uint16(sum)
}
