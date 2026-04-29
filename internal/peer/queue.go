package peer

import (
	"github.com/cafecito-games/goenet/internal/core"
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
	Packet                   *core.Packet
}

// IncomingCommand is the queued receive state held before dispatch.
type IncomingCommand struct {
	ReliableSequenceNumber   uint16
	UnreliableSequenceNumber uint16
	Command                  Command
	FragmentCount            uint32
	FragmentsRemaining       uint32
	Fragments                []uint32
	Packet                   *core.Packet
}

// SetFragmentCount resets fragment bookkeeping for a queued inbound command.
func (c *IncomingCommand) SetFragmentCount(fragmentCount uint32) {
	c.FragmentCount = fragmentCount
	c.FragmentsRemaining = fragmentCount
	if fragmentCount == 0 {
		c.Fragments = nil
		return
	}

	c.Fragments = make([]uint32, fragmentWordCount(fragmentCount))
}

// MarkFragmentReceived marks one fragment index and reports whether it was newly observed.
func (c *IncomingCommand) MarkFragmentReceived(fragmentNumber uint32) bool {
	if fragmentNumber >= c.FragmentCount {
		return false
	}
	if len(c.Fragments) == 0 {
		c.Fragments = make([]uint32, fragmentWordCount(c.FragmentCount))
		if c.FragmentsRemaining == 0 {
			c.FragmentsRemaining = c.FragmentCount
		}
	}

	word := fragmentNumber / 32
	mask := uint32(1) << (fragmentNumber % 32)
	if c.Fragments[word]&mask != 0 {
		return false
	}

	c.Fragments[word] |= mask
	if c.FragmentsRemaining > 0 {
		c.FragmentsRemaining--
	}

	return true
}

// IsComplete reports whether all expected fragments have been observed.
func (c *IncomingCommand) IsComplete() bool {
	return c.FragmentsRemaining == 0
}

// Acknowledgement tracks pending protocol acknowledgements in FIFO order.
type Acknowledgement struct {
	SentTime uint32
	Command  Command
}

// ListElement is one node in an orderedList.
type ListElement[T any] struct {
	value T
	prev  *ListElement[T]
	next  *ListElement[T]
}

// Next returns the next element in the list or nil.
func (e *ListElement[T]) Next() *ListElement[T] {
	if e == nil {
		return nil
	}

	return e.next
}

// Prev returns the previous element in the list or nil.
func (e *ListElement[T]) Prev() *ListElement[T] {
	if e == nil {
		return nil
	}

	return e.prev
}

// Value returns the element payload.
func (e *ListElement[T]) Value() T {
	return e.value
}

type orderedList[T any] struct {
	front *ListElement[T]
	back  *ListElement[T]
	len   int
}

// Len returns the number of elements in the list.
func (l *orderedList[T]) Len() int {
	return l.len
}

// Front returns the first element in the list.
func (l *orderedList[T]) Front() *ListElement[T] {
	return l.front
}

// Back returns the last element in the list.
func (l *orderedList[T]) Back() *ListElement[T] {
	return l.back
}

// PushBack appends value to the end of the list.
func (l *orderedList[T]) PushBack(value T) *ListElement[T] {
	elem := &ListElement[T]{value: value}
	if l.back == nil {
		l.front = elem
		l.back = elem
		l.len = 1
		return elem
	}

	elem.prev = l.back
	l.back.next = elem
	l.back = elem
	l.len++
	return elem
}

// InsertBefore inserts value immediately before mark.
func (l *orderedList[T]) InsertBefore(mark *ListElement[T], value T) *ListElement[T] {
	if mark == nil {
		return l.PushBack(value)
	}

	elem := &ListElement[T]{
		value: value,
		prev:  mark.prev,
		next:  mark,
	}
	if mark.prev != nil {
		mark.prev.next = elem
	} else {
		l.front = elem
	}
	mark.prev = elem
	l.len++
	return elem
}

// InsertOrdered inserts value before the first element that sorts after it.
func (l *orderedList[T]) InsertOrdered(value T, less func(a, b T) bool) *ListElement[T] {
	for elem := l.front; elem != nil; elem = elem.next {
		if less(value, elem.value) {
			return l.InsertBefore(elem, value)
		}
	}

	return l.PushBack(value)
}

// Remove removes elem from the list and returns its payload.
func (l *orderedList[T]) Remove(elem *ListElement[T]) T {
	var zero T
	if elem == nil {
		return zero
	}

	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		l.front = elem.next
	}
	if elem.next != nil {
		elem.next.prev = elem.prev
	} else {
		l.back = elem.prev
	}

	elem.prev = nil
	elem.next = nil
	l.len--
	return elem.value
}

type outgoingCommandList = orderedList[*OutgoingCommand]
type incomingCommandList = orderedList[*IncomingCommand]
type acknowledgementList = orderedList[*Acknowledgement]

type outgoingQueue = outgoingCommandList
type incomingQueue = incomingCommandList
type acknowledgementQueue = acknowledgementList

func fragmentWordCount(fragmentCount uint32) int {
	return int((fragmentCount + 31) / 32)
}
