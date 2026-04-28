package peer

import (
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestOutgoingQueuePreservesFIFOOrder(t *testing.T) {
	var q outgoingQueue

	first := &OutgoingCommand{
		ReliableSequenceNumber: 1,
		Packet:                 &goenet.Packet{Data: []byte("first")},
	}
	second := &OutgoingCommand{
		ReliableSequenceNumber: 2,
		Packet:                 &goenet.Packet{Data: []byte("second")},
	}

	q.Push(first)
	q.Push(second)

	if got := q.Len(); got != 2 {
		t.Fatalf("Len() = %d", got)
	}

	gotFirst, ok := q.Pop()
	if !ok {
		t.Fatal("first Pop() = empty")
	}
	if gotFirst != first {
		t.Fatalf("first Pop() = %+v, want %+v", gotFirst, first)
	}

	gotSecond, ok := q.Pop()
	if !ok {
		t.Fatal("second Pop() = empty")
	}
	if gotSecond != second {
		t.Fatalf("second Pop() = %+v, want %+v", gotSecond, second)
	}

	if _, ok := q.Pop(); ok {
		t.Fatal("third Pop() unexpectedly succeeded")
	}
}

func TestAcknowledgementQueuePreservesInsertionOrder(t *testing.T) {
	var q acknowledgementQueue

	first := &Acknowledgement{SentTime: 10}
	second := &Acknowledgement{SentTime: 20}

	q.Push(first)
	q.Push(second)

	gotFirst, ok := q.Pop()
	if !ok {
		t.Fatal("first Pop() = empty")
	}
	if gotFirst != first {
		t.Fatalf("first Pop() = %+v, want %+v", gotFirst, first)
	}

	gotSecond, ok := q.Pop()
	if !ok {
		t.Fatal("second Pop() = empty")
	}
	if gotSecond != second {
		t.Fatalf("second Pop() = %+v, want %+v", gotSecond, second)
	}
}
