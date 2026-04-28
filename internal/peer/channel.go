package peer

// Channel tracks ENet sequence numbers for one logical peer channel.
type Channel struct {
	OutgoingReliableSequenceNumber   uint16
	OutgoingUnreliableSequenceNumber uint16
	IncomingReliableSequenceNumber   uint16
	IncomingUnreliableSequenceNumber uint16
}

// NewChannel returns a zero-initialized channel state holder.
func NewChannel() Channel {
	return Channel{}
}
