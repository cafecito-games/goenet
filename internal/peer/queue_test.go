package peer

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestOutgoingQueuePreservesFIFOOrder(t *testing.T) {
	var q outgoingCommandList

	first := &OutgoingCommand{
		ReliableSequenceNumber: 1,
		Packet:                 &core.Packet{Data: []byte("first")},
	}
	second := &OutgoingCommand{
		ReliableSequenceNumber: 2,
		Packet:                 &core.Packet{Data: []byte("second")},
	}

	firstElem := q.PushBack(first)
	secondElem := q.PushBack(second)

	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d", got)
	}
	if q.Front() != firstElem {
		t.Fatal("Front() did not return first element")
	}
	if q.Back() != secondElem {
		t.Fatal("Back() did not return second element")
	}
	if firstElem.Next() != secondElem {
		t.Fatal("first.Next() did not return second element")
	}
	if got := q.Remove(firstElem); got != first {
		t.Fatalf("Remove(first) = %+v, want %+v", got, first)
	}
	if q.Front() != secondElem {
		t.Fatal("Front() did not advance to second element after removal")
	}
}

func TestOutgoingCommandListSupportsOrderedInsertion(t *testing.T) {
	var q outgoingCommandList

	higher := &OutgoingCommand{ReliableSequenceNumber: 20}
	lower := &OutgoingCommand{ReliableSequenceNumber: 10}
	middle := &OutgoingCommand{ReliableSequenceNumber: 15}

	q.InsertOrdered(higher, func(a, b *OutgoingCommand) bool {
		return a.ReliableSequenceNumber < b.ReliableSequenceNumber
	})
	q.InsertOrdered(lower, func(a, b *OutgoingCommand) bool {
		return a.ReliableSequenceNumber < b.ReliableSequenceNumber
	})
	q.InsertOrdered(middle, func(a, b *OutgoingCommand) bool {
		return a.ReliableSequenceNumber < b.ReliableSequenceNumber
	})

	want := []*OutgoingCommand{lower, middle, higher}
	index := 0
	for elem := q.Front(); elem != nil; elem = elem.Next() {
		if got := elem.Value(); got != want[index] {
			t.Fatalf("element %d = %+v, want %+v", index, got, want[index])
		}
		index++
	}
	if index != len(want) {
		t.Fatalf("iterated %d elements, want %d", index, len(want))
	}
}

func TestAcknowledgementListSupportsInsertBeforeAndRemove(t *testing.T) {
	var q acknowledgementList

	first := &Acknowledgement{SentTime: 10}
	second := &Acknowledgement{SentTime: 20}
	inserted := &Acknowledgement{SentTime: 15}

	firstElem := q.PushBack(first)
	secondElem := q.PushBack(second)
	insertedElem := q.InsertBefore(secondElem, inserted)

	if firstElem.Next() != insertedElem {
		t.Fatal("first.Next() did not return inserted element")
	}
	if insertedElem.Next() != secondElem {
		t.Fatal("inserted.Next() did not return second element")
	}
	if got := q.Remove(insertedElem); got != inserted {
		t.Fatalf("Remove(inserted) = %+v, want %+v", got, inserted)
	}
	if firstElem.Next() != secondElem {
		t.Fatal("first.Next() did not reconnect to second element")
	}
}

func TestIncomingQueuePreservesFIFOOrder(t *testing.T) {
	var q incomingCommandList

	first := &IncomingCommand{
		Command: Command{
			Header: Header{
				Command:                protocol.CommandSendReliable,
				ChannelID:              1,
				ReliableSequenceNumber: 10,
			},
		},
	}
	second := &IncomingCommand{
		Command: Command{
			Header: Header{
				Command:                protocol.CommandSendUnreliable,
				ChannelID:              1,
				ReliableSequenceNumber: 11,
			},
		},
	}
	third := &IncomingCommand{
		Command: Command{
			Header: Header{
				Command:                protocol.CommandSendReliable,
				ChannelID:              1,
				ReliableSequenceNumber: 9,
			},
		},
	}

	secondElem := q.PushBack(second)
	q.InsertBefore(secondElem, first)
	q.InsertOrdered(third, func(a, b *IncomingCommand) bool {
		return a.Command.Header.ReliableSequenceNumber < b.Command.Header.ReliableSequenceNumber
	})

	if got := q.Len(); got != 3 {
		t.Fatalf("Len() = %d", got)
	}

	want := []*IncomingCommand{third, first, second}
	index := 0
	for elem := q.Front(); elem != nil; elem = elem.Next() {
		if got := elem.Value(); got != want[index] {
			t.Fatalf("element %d = %+v, want %+v", index, got, want[index])
		}
		index++
	}
	if index != len(want) {
		t.Fatalf("iterated %d elements, want %d", index, len(want))
	}
}

func TestIncomingUnreliableLessHandlesUint16Wrap(t *testing.T) {
	// Within one reliable group, two unreliable seq numbers straddling the
	// uint16 wrap should order by signed-distance: 0xFFFE < 0x0002.
	const reliable uint16 = 5
	earlier := &IncomingCommand{
		ReliableSequenceNumber:   reliable,
		UnreliableSequenceNumber: 0xFFFE,
	}
	later := &IncomingCommand{
		ReliableSequenceNumber:   reliable,
		UnreliableSequenceNumber: 0x0002,
	}

	if !incomingUnreliableLess(reliable, earlier, later) {
		t.Fatal("incomingUnreliableLess: expected 0xFFFE < 0x0002 across uint16 wrap")
	}
	if incomingUnreliableLess(reliable, later, earlier) {
		t.Fatal("incomingUnreliableLess: comparator must be antisymmetric across wrap")
	}
}
