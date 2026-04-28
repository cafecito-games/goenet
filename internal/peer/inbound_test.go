package peer

import "testing"

func TestChannelQueuesIncomingReliableAroundCurrentSequenceAnchor(t *testing.T) {
	ch := NewChannel()
	ch.IncomingReliableSequenceNumber = 0xFFF0

	wrapped := &IncomingCommand{ReliableSequenceNumber: 1}
	currentWindow := &IncomingCommand{ReliableSequenceNumber: 0xFFF2}

	ch.QueueIncomingReliable(wrapped)
	ch.QueueIncomingReliable(currentWindow)

	want := []*IncomingCommand{currentWindow, wrapped}
	index := 0
	for elem := ch.IncomingReliableCommands.Front(); elem != nil; elem = elem.Next() {
		if got := elem.Value(); got != want[index] {
			t.Fatalf("element %d = %+v, want %+v", index, got, want[index])
		}
		index++
	}
	if index != len(want) {
		t.Fatalf("iterated %d elements, want %d", index, len(want))
	}
}

func TestChannelQueuesIncomingUnreliableByReliableThenUnreliableSequence(t *testing.T) {
	ch := NewChannel()
	ch.IncomingReliableSequenceNumber = 10

	nextReliable := &IncomingCommand{ReliableSequenceNumber: 12, UnreliableSequenceNumber: 0}
	higherUnreliable := &IncomingCommand{ReliableSequenceNumber: 11, UnreliableSequenceNumber: 3}
	lowerUnreliable := &IncomingCommand{ReliableSequenceNumber: 11, UnreliableSequenceNumber: 1}

	ch.QueueIncomingUnreliable(nextReliable)
	ch.QueueIncomingUnreliable(higherUnreliable)
	ch.QueueIncomingUnreliable(lowerUnreliable)

	want := []*IncomingCommand{lowerUnreliable, higherUnreliable, nextReliable}
	index := 0
	for elem := ch.IncomingUnreliableCommands.Front(); elem != nil; elem = elem.Next() {
		if got := elem.Value(); got != want[index] {
			t.Fatalf("element %d = %+v, want %+v", index, got, want[index])
		}
		index++
	}
	if index != len(want) {
		t.Fatalf("iterated %d elements, want %d", index, len(want))
	}
}

func TestChannelDispatchSequenceHelpersAdvanceENetReceiveAnchors(t *testing.T) {
	ch := NewChannel()

	reliable := &IncomingCommand{ReliableSequenceNumber: 22}
	unreliable := &IncomingCommand{ReliableSequenceNumber: 22, UnreliableSequenceNumber: 7}

	ch.MarkIncomingReliableDispatched(reliable)
	if ch.IncomingReliableSequenceNumber != 22 {
		t.Fatalf("IncomingReliableSequenceNumber = %d", ch.IncomingReliableSequenceNumber)
	}
	if ch.IncomingUnreliableSequenceNumber != 0 {
		t.Fatalf("IncomingUnreliableSequenceNumber = %d", ch.IncomingUnreliableSequenceNumber)
	}

	ch.MarkIncomingUnreliableDispatched(unreliable)
	if ch.IncomingReliableSequenceNumber != 22 {
		t.Fatalf("IncomingReliableSequenceNumber = %d", ch.IncomingReliableSequenceNumber)
	}
	if ch.IncomingUnreliableSequenceNumber != 7 {
		t.Fatalf("IncomingUnreliableSequenceNumber = %d", ch.IncomingUnreliableSequenceNumber)
	}
}

func TestIncomingCommandFragmentTrackingUsesBitsetAndIgnoresDuplicates(t *testing.T) {
	cmd := &IncomingCommand{}
	cmd.SetFragmentCount(33)

	if cmd.FragmentCount != 33 {
		t.Fatalf("FragmentCount = %d", cmd.FragmentCount)
	}
	if cmd.FragmentsRemaining != 33 {
		t.Fatalf("FragmentsRemaining = %d", cmd.FragmentsRemaining)
	}
	if got := len(cmd.Fragments); got != 2 {
		t.Fatalf("len(Fragments) = %d", got)
	}
	if cmd.IsComplete() {
		t.Fatal("IsComplete() = true before any fragments arrive")
	}

	if !cmd.MarkFragmentReceived(0) {
		t.Fatal("MarkFragmentReceived(0) = false, want true")
	}
	if cmd.MarkFragmentReceived(0) {
		t.Fatal("MarkFragmentReceived(0) = true on duplicate")
	}
	if !cmd.MarkFragmentReceived(32) {
		t.Fatal("MarkFragmentReceived(32) = false, want true")
	}
	if cmd.MarkFragmentReceived(33) {
		t.Fatal("MarkFragmentReceived(33) = true for out-of-range fragment")
	}

	for fragment := uint32(1); fragment < 32; fragment++ {
		if !cmd.MarkFragmentReceived(fragment) {
			t.Fatalf("MarkFragmentReceived(%d) = false, want true", fragment)
		}
	}

	if cmd.FragmentsRemaining != 0 {
		t.Fatalf("FragmentsRemaining = %d", cmd.FragmentsRemaining)
	}
	if !cmd.IsComplete() {
		t.Fatal("IsComplete() = false after all fragments arrive")
	}
}

func TestPeerWaitingDataAccountingAndDispatchQueueHelpers(t *testing.T) {
	var p Peer

	first := &IncomingCommand{ReliableSequenceNumber: 1}
	second := &IncomingCommand{ReliableSequenceNumber: 2}

	p.QueueDispatchedCommand(first)
	p.QueueDispatchedCommand(second)

	if got := p.PopDispatchedCommand(); got != first {
		t.Fatalf("PopDispatchedCommand() = %+v, want %+v", got, first)
	}
	if got := p.PopDispatchedCommand(); got != second {
		t.Fatalf("PopDispatchedCommand() = %+v, want %+v", got, second)
	}
	if got := p.PopDispatchedCommand(); got != nil {
		t.Fatalf("PopDispatchedCommand() = %+v, want nil", got)
	}

	if !p.CanQueueWaitingData(6, 6) {
		t.Fatal("CanQueueWaitingData(6, 6) = false, want true")
	}

	p.AddWaitingData(6)
	if p.TotalWaitingData != 6 {
		t.Fatalf("TotalWaitingData = %d", p.TotalWaitingData)
	}
	if p.CanQueueWaitingData(1, 6) {
		t.Fatal("CanQueueWaitingData(1, 6) = true, want false")
	}

	p.ReleaseWaitingData(10)
	if p.TotalWaitingData != 0 {
		t.Fatalf("TotalWaitingData = %d after release", p.TotalWaitingData)
	}
}
