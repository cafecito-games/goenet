package peer

import (
	"github.com/cafecito-games/goenet"
	"github.com/cafecito-games/goenet/internal/protocol"
)

// Header preserves the common ENet protocol header fields that drive queue logic.
type Header struct {
	Command                protocol.Command
	ChannelID              uint8
	Flags                  protocol.CommandFlag
	ReliableSequenceNumber uint16
}

// Command preserves generic protocol metadata and the typed payload together.
type Command struct {
	Header  Header
	Payload protocol.PacketCommand
}

// OutgoingCommand is the queued state required before packet assembly exists.
type OutgoingCommand struct {
	ReliableSequenceNumber   uint16
	UnreliableSequenceNumber uint16
	SentTime                 uint32
	RoundTripTimeout         uint32
	QueueTime                uint32
	FragmentOffset           uint32
	FragmentLength           uint16
	SendAttempts             uint16
	Command                  Command
	Packet                   *goenet.Packet
}

// IncomingCommand is the queued receive state held before dispatch.
type IncomingCommand struct {
	ReliableSequenceNumber   uint16
	UnreliableSequenceNumber uint16
	Command                  Command
	FragmentCount            uint32
	FragmentsRemaining       uint32
	Fragments                []uint32
	Packet                   *goenet.Packet
}

// Acknowledgement tracks pending protocol acknowledgements in FIFO order.
type Acknowledgement struct {
	SentTime uint32
	Command  Command
}

type outgoingQueue struct {
	items []*OutgoingCommand
}

func (q *outgoingQueue) Push(cmd *OutgoingCommand) {
	q.items = append(q.items, cmd)
}

func (q *outgoingQueue) Pop() (*OutgoingCommand, bool) {
	if len(q.items) == 0 {
		return nil, false
	}

	cmd := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return cmd, true
}

func (q *outgoingQueue) Len() int {
	return len(q.items)
}

type incomingQueue struct {
	items []*IncomingCommand
}

func (q *incomingQueue) Push(cmd *IncomingCommand) {
	q.items = append(q.items, cmd)
}

func (q *incomingQueue) Pop() (*IncomingCommand, bool) {
	if len(q.items) == 0 {
		return nil, false
	}

	cmd := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return cmd, true
}

func (q *incomingQueue) Len() int {
	return len(q.items)
}

type acknowledgementQueue struct {
	items []*Acknowledgement
}

func (q *acknowledgementQueue) Push(ack *Acknowledgement) {
	q.items = append(q.items, ack)
}

func (q *acknowledgementQueue) Pop() (*Acknowledgement, bool) {
	if len(q.items) == 0 {
		return nil, false
	}

	ack := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return ack, true
}

func (q *acknowledgementQueue) Len() int {
	return len(q.items)
}
