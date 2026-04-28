package peer

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
func NewChannel() Channel {
	return Channel{}
}

// QueueIncomingReliable inserts a reliable receive command in ENet sequence order.
func (ch *Channel) QueueIncomingReliable(cmd *IncomingCommand) *listElement[*IncomingCommand] {
	return ch.IncomingReliableCommands.InsertOrdered(cmd, func(a, b *IncomingCommand) bool {
		return incomingReliableLess(ch.IncomingReliableSequenceNumber, a, b)
	})
}

// QueueIncomingUnreliable inserts an unreliable receive command in ENet queue order.
func (ch *Channel) QueueIncomingUnreliable(cmd *IncomingCommand) *listElement[*IncomingCommand] {
	return ch.IncomingUnreliableCommands.InsertOrdered(cmd, func(a, b *IncomingCommand) bool {
		return incomingUnreliableLess(ch.IncomingReliableSequenceNumber, a, b)
	})
}

// MarkIncomingReliableDispatched advances the receive anchor after reliable delivery.
func (ch *Channel) MarkIncomingReliableDispatched(cmd *IncomingCommand) {
	if cmd == nil {
		return
	}

	ch.IncomingReliableSequenceNumber = cmd.ReliableSequenceNumber
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

	return a.UnreliableSequenceNumber < b.UnreliableSequenceNumber
}

func sequenceDistance(anchor, sequence uint16) uint32 {
	if sequence >= anchor {
		return uint32(sequence - anchor)
	}

	return uint32(sequence) + (1 << 16) - uint32(anchor)
}
