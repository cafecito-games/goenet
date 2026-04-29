package engine

import (
	"context"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	ipeer "github.com/cafecito-games/goenet/internal/peer"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestServiceCompletesServerSideConnectFlow(t *testing.T) {
	addr := netip.MustParseAddrPort("127.0.0.1:9001")
	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.PeerCount = 1
	})

	sock.QueueInbound(addr, marshalDatagram(
		iprotocol.Header{
			PeerID:   iprotocol.MaximumPeerID,
			Flags:    iprotocol.HeaderFlagSentTime,
			SentTime: 0x2222,
		},
		iprotocol.Connect{
			Header: iprotocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 1,
			},
			OutgoingPeerID:             7,
			IncomingSessionID:          0xFF,
			OutgoingSessionID:          0xFF,
			MTU:                        1400,
			WindowSize:                 32768,
			ChannelCount:               1,
			IncomingBandwidth:          60000,
			OutgoingBandwidth:          30000,
			PacketThrottleInterval:     5000,
			PacketThrottleAcceleration: 2,
			PacketThrottleDeceleration: 3,
			ConnectID:                  0x11223344,
			Data:                       0x55667788,
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("first event type = %d, want 0", event.Type)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1", got)
	}

	raw := mustPeerInState(t, host, core.PeerStateAcknowledgingConnect)
	if raw.ConnectID != 0x11223344 {
		t.Fatalf("ConnectID = %#x", raw.ConnectID)
	}
	if raw.OutgoingPeerID != 7 {
		t.Fatalf("OutgoingPeerID = %d", raw.OutgoingPeerID)
	}

	verifyWrite := sock.MustWrite(t, 0)
	verifyHeader, verifyCommand := mustSingleCommand(t, verifyWrite.Payload)
	verify, ok := verifyCommand.(iprotocol.VerifyConnect)
	if !ok {
		t.Fatalf("verify command type = %T", verifyCommand)
	}
	if verifyHeader.PeerID != raw.OutgoingPeerID {
		t.Fatalf("verify header peer id = %d, want %d", verifyHeader.PeerID, raw.OutgoingPeerID)
	}
	if verify.OutgoingPeerID != raw.IncomingPeerID {
		t.Fatalf("verify outgoing peer id = %d, want %d", verify.OutgoingPeerID, raw.IncomingPeerID)
	}

	sock.QueueInbound(addr, marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
		},
		iprotocol.Acknowledge{
			Header: iprotocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 2,
			},
			ReceivedReliableSequenceNumber: verify.Header.ReliableSequenceNumber,
			ReceivedSentTime:               verifyHeader.SentTime,
		},
	))

	event, err = host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventConnect {
		t.Fatalf("second event type = %d, want %d", event.Type, core.EventConnect)
	}
	if event.Peer != raw {
		t.Fatalf("second event peer = %p, want %p", event.Peer, raw)
	}
	if event.Data != 0x55667788 {
		t.Fatalf("second event data = %#x", event.Data)
	}
	if raw.State != core.PeerStateConnected {
		t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateConnected)
	}
}

func TestServiceDispatchesInboundReliableReceiveAndQueuesAck(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 2

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x4567,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("hello"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if event.Peer != raw {
		t.Fatalf("event peer = %p, want %p", event.Peer, raw)
	}
	if event.ChannelID != 0 {
		t.Fatalf("event channel id = %d", event.ChannelID)
	}
	if string(event.Packet.Data) != "hello" {
		t.Fatalf("event packet = %q", event.Packet.Data)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1 (ack queue len %d)", got, raw.Acknowledgements.Len())
	}

	_, ackCommand := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	ack, ok := ackCommand.(iprotocol.Acknowledge)
	if !ok {
		t.Fatalf("ack command type = %T", ackCommand)
	}
	if ack.ReceivedReliableSequenceNumber != 1 {
		t.Fatalf("ack received reliable sequence = %d", ack.ReceivedReliableSequenceNumber)
	}
	if ack.ReceivedSentTime != 0x4567 {
		t.Fatalf("ack received sent time = %#x", ack.ReceivedSentTime)
	}
}

func TestServiceReassemblesReliableFragmentsBeforeDispatch(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	queueFragment := func(sentTime uint16, fragmentNumber uint32, offset uint32, data string) {
		sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
			iprotocol.Header{
				PeerID:    raw.IncomingPeerID,
				SessionID: raw.IncomingSessionID,
				Flags:     iprotocol.HeaderFlagSentTime,
				SentTime:  sentTime,
			},
			iprotocol.SendFragment{
				Header: iprotocol.CommandHeader{
					ChannelID:              0,
					ReliableSequenceNumber: uint16(fragmentNumber + 1),
				},
				StartSequenceNumber: 1,
				FragmentCount:       2,
				FragmentNumber:      fragmentNumber,
				TotalLength:         10,
				FragmentOffset:      offset,
				Data:                []byte(data),
			},
		))
	}

	queueFragment(0x1001, 0, 0, "hello")

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("first event type = %d, want 0", event.Type)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("first WriteCount = %d, want 1", got)
	}

	queueFragment(0x1002, 1, 5, "world")

	event, err = host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("second event type = %d, want %d", event.Type, core.EventReceive)
	}
	if string(event.Packet.Data) != "helloworld" {
		t.Fatalf("packet data = %q", event.Packet.Data)
	}
	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("total WriteCount = %d, want 2", got)
	}
}

func TestServiceStopsAfterFirstAckBearingCommandWithoutSentTime(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("one"),
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 2,
			},
			Data: []byte("two"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if string(event.Packet.Data) != "one" {
		t.Fatalf("packet data = %q", event.Packet.Data)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d, want 0", got)
	}

	event, err = host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("second event type = %d, want 0", event.Type)
	}
}

func TestServiceRejectsWrongSessionWrongAddressAndInvalidUnassociatedPackets(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	invalidReliable := iprotocol.SendReliable{
		Header: iprotocol.CommandHeader{
			ChannelID:              0,
			ReliableSequenceNumber: 1,
		},
		Data: []byte("drop"),
	}

	sock.QueueInbound(netip.MustParseAddrPort("127.0.0.1:9002"), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  1,
		},
		invalidReliable,
	))
	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID + 1,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  2,
		},
		invalidReliable,
	))
	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID: iprotocol.MaximumPeerID,
		},
		invalidReliable,
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d, want 0", got)
	}
	if raw.DispatchedCommands.Len() != 0 {
		t.Fatalf("DispatchedCommands.Len() = %d", raw.DispatchedCommands.Len())
	}
	if raw.Acknowledgements.Len() != 0 {
		t.Fatalf("Acknowledgements.Len() = %d", raw.Acknowledgements.Len())
	}
}

func TestServiceAppliesVerifyConnectForConnectingPeer(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnecting)
	raw.IncomingPeerID = 0
	raw.ConnectID = 0x55667788
	raw.MTU = 1400

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x3344,
			SessionID: 0,
		},
		iprotocol.VerifyConnect{
			Header: iprotocol.CommandHeader{
				ChannelID:              0xFF,
				ReliableSequenceNumber: 1,
			},
			OutgoingPeerID:             33,
			IncomingSessionID:          2,
			OutgoingSessionID:          3,
			MTU:                        1200,
			WindowSize:                 32000,
			ChannelCount:               1,
			IncomingBandwidth:          64000,
			OutgoingBandwidth:          32000,
			PacketThrottleInterval:     defaultPacketThrottleInterval,
			PacketThrottleAcceleration: defaultPacketThrottleAcceleration,
			PacketThrottleDeceleration: defaultPacketThrottleDeceleration,
			ConnectID:                  raw.ConnectID,
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventConnect {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventConnect)
	}
	if raw.State != core.PeerStateConnected {
		t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateConnected)
	}
	if raw.OutgoingPeerID != 33 {
		t.Fatalf("OutgoingPeerID = %d", raw.OutgoingPeerID)
	}
	if raw.IncomingSessionID != 2 {
		t.Fatalf("IncomingSessionID = %d", raw.IncomingSessionID)
	}
	if raw.OutgoingSessionID != 3 {
		t.Fatalf("OutgoingSessionID = %d", raw.OutgoingSessionID)
	}
	if raw.MTU != 1200 {
		t.Fatalf("MTU = %d", raw.MTU)
	}
	if host.runtime[raw].windowSize != 32000 {
		t.Fatalf("windowSize = %d", host.runtime[raw].windowSize)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1", got)
	}

	_, ackCommand := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	if _, ok := ackCommand.(iprotocol.Acknowledge); !ok {
		t.Fatalf("ack command type = %T", ackCommand)
	}
}

func TestServiceAcknowledgesDroppedReliableDuplicate(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x2222,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 0,
			},
			Data: []byte("dup"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1", got)
	}

	_, ackCommand := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	ack, ok := ackCommand.(iprotocol.Acknowledge)
	if !ok {
		t.Fatalf("ack command type = %T", ackCommand)
	}
	if ack.ReceivedReliableSequenceNumber != 0 {
		t.Fatalf("ack received reliable sequence = %d", ack.ReceivedReliableSequenceNumber)
	}
	if ack.ReceivedSentTime != 0x2222 {
		t.Fatalf("ack received sent time = %#x", ack.ReceivedSentTime)
	}
}

func TestServiceDropsStaleUnreliableCommandBeforeDispatchingFreshOne(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1
	raw.Channels[0].IncomingReliableSequenceNumber = 1

	stale := &ipeer.IncomingCommand{
		ReliableSequenceNumber:   0,
		UnreliableSequenceNumber: 1,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendUnreliableFragment,
				ChannelID:              0,
				ReliableSequenceNumber: 0,
			},
		},
		Packet: &core.Packet{Data: []byte("stale")},
	}
	stale.SetFragmentCount(1)
	raw.AddWaitingData(uint32(len(stale.Packet.Data)))
	raw.Channels[0].InsertIncomingUnreliableOrdered(stale)

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
		},
		iprotocol.SendUnreliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			UnreliableSequenceNumber: 1,
			Data:                     []byte("fresh"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if got := string(event.Packet.Data); got != "fresh" {
		t.Fatalf("event packet = %q", got)
	}
	if got := raw.Channels[0].IncomingUnreliableCommands.Len(); got != 0 {
		t.Fatalf("IncomingUnreliableCommands.Len() = %d", got)
	}
	if got := raw.TotalWaitingData; got != 0 {
		t.Fatalf("TotalWaitingData = %d", got)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceDropsDuplicateAndFarAheadUnsequencedGroups(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	queue := func(group uint16, data string) {
		sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
			iprotocol.Header{
				PeerID:    raw.IncomingPeerID,
				SessionID: raw.IncomingSessionID,
			},
			iprotocol.SendUnsequenced{
				Header: iprotocol.CommandHeader{
					ChannelID: 0,
					Flags:     iprotocol.CommandFlagUnsequenced,
				},
				UnsequencedGroup: group,
				Data:             []byte(data),
			},
		))
	}

	queue(5, "one")
	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive || string(event.Packet.Data) != "one" {
		t.Fatalf("first event = %#v", event)
	}

	queue(5, "dup")
	event, err = host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("duplicate event type = %d, want 0", event.Type)
	}

	queue(32768, "far")
	event, err = host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("far-ahead event type = %d, want 0", event.Type)
	}
}

func TestServiceDropsPayloadCommandsWhileDisconnectLaterButStillAcknowledgesReliableOnes(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateDisconnectLater)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x1010,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("drop"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if got := raw.DispatchedCommands.Len(); got != 0 {
		t.Fatalf("DispatchedCommands.Len() = %d", got)
	}
	if got := raw.Acknowledgements.Len(); got != 0 {
		t.Fatalf("Acknowledgements.Len() = %d", got)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d", got)
	}

	_, ackCommand := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	if _, ok := ackCommand.(iprotocol.Acknowledge); !ok {
		t.Fatalf("ack command type = %T", ackCommand)
	}
}

func TestServiceRejectsInboundPayloadLargerThanMaximumPacketSize(t *testing.T) {
	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.MaximumPacketSize = 3
	})
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x3030,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("four"),
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if got := raw.DispatchedCommands.Len(); got != 0 {
		t.Fatalf("DispatchedCommands.Len() = %d", got)
	}
	if got := raw.Acknowledgements.Len(); got != 0 {
		t.Fatalf("Acknowledgements.Len() = %d", got)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceRejectsZeroLengthReliableFragment(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x4040,
		},
		iprotocol.SendFragment{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			StartSequenceNumber: 1,
			FragmentCount:       1,
			FragmentNumber:      0,
			TotalLength:         1,
			FragmentOffset:      0,
			Data:                nil,
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != 0 {
		t.Fatalf("event type = %d, want 0", event.Type)
	}
	if got := raw.DispatchedCommands.Len(); got != 0 {
		t.Fatalf("DispatchedCommands.Len() = %d", got)
	}
	if got := sock.WriteCount(); got != 0 {
		t.Fatalf("WriteCount = %d", got)
	}
}

func TestServiceHandlesRemoteDisconnectAndResetsPeerSlot(t *testing.T) {
	host, sock := newReceiveHost(t, nil)
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 1

	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x5050,
		},
		iprotocol.Disconnect{
			Header: iprotocol.CommandHeader{
				ChannelID:              0xFF,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
			Data: 0x99aabbcc,
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventDisconnect {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventDisconnect)
	}
	if event.Peer != raw {
		t.Fatalf("event peer = %p, want %p", event.Peer, raw)
	}
	if event.Data != 0x99aabbcc {
		t.Fatalf("event data = %#x", event.Data)
	}
	if raw.State != core.PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateDisconnected)
	}
	if raw.OutgoingPeerID != iprotocol.MaximumPeerID {
		t.Fatalf("OutgoingPeerID = %d, want %d", raw.OutgoingPeerID, iprotocol.MaximumPeerID)
	}
	if got := len(raw.Channels); got != 0 {
		t.Fatalf("len(raw.Channels) = %d, want 0", got)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("WriteCount = %d, want 1", got)
	}
}

func TestNewHostUsesMaximumChannelCountWhenChannelLimitUnset(t *testing.T) {
	cfg := core.DefaultConfig()
	sock := testsupport.NewFakeSocket()
	host := NewHost(cfg, sock, 0)
	peer := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)

	if got := len(peer.Channels); got != int(iprotocol.MaximumChannelCount) {
		t.Fatalf("len(peer.Channels) = %d, want %d", got, iprotocol.MaximumChannelCount)
	}
}

func TestReceiveIncomingAcceptsDatagramsLargerThanConfiguredMTU(t *testing.T) {
	host, sock := newReceiveHost(t, func(cfg *core.Config) {
		cfg.MTU = 576
	})
	raw := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateConnected)
	raw.IncomingPeerID = 0
	raw.IncomingSessionID = 2

	payload := append(make([]byte, 0, 700), []byte("oversize-reliable")...)
	payload = append(payload, make([]byte, 640)...)
	sock.QueueInbound(raw.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    raw.IncomingPeerID,
			SessionID: raw.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x1200,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: payload,
		},
	))

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if got := len(event.Packet.Data); got != len(payload) {
		t.Fatalf("payload length = %d, want %d", got, len(payload))
	}
}

func newReceiveHost(t *testing.T, configure func(*core.Config)) (*Host, *testsupport.FakeSocket) {
	t.Helper()

	cfg := core.DefaultConfig()
	cfg.ChannelLimit = 1
	if configure != nil {
		configure(&cfg)
	}

	sock := testsupport.NewFakeSocket()
	return NewHost(cfg, sock, 77), sock
}

func mustAddress(t *testing.T, value string) core.Address {
	t.Helper()

	addr, err := core.NewAddress(netip.MustParseAddrPort(value), 0)
	if err != nil {
		t.Fatal(err)
	}

	return addr
}

func marshalDatagram(header iprotocol.Header, commands ...iprotocol.PacketCommand) []byte {
	payload := header.MarshalBinary(nil)
	for _, command := range commands {
		payload = command.MarshalBinary(payload)
	}

	return payload
}

func mustPeerInState(t *testing.T, host *Host, state core.PeerState) *ipeer.Peer {
	t.Helper()

	for _, candidate := range host.peers {
		if candidate != nil && candidate.State == state {
			return candidate
		}
	}

	t.Fatalf("expected peer in state %d", state)
	return nil
}

func mustSingleCommand(t *testing.T, payload []byte) (iprotocol.Header, iprotocol.PacketCommand) {
	t.Helper()

	header, err := iprotocol.ParseHeader(payload)
	if err != nil {
		t.Fatal(err)
	}

	offset := 2
	if header.Flags&iprotocol.HeaderFlagSentTime != 0 {
		offset = 4
	}

	command, _, used, err := iprotocol.ParseCommand(payload[offset:])
	if err != nil {
		t.Fatal(err)
	}
	if offset+used != len(payload) {
		t.Fatalf("datagram consumed %d of %d bytes", offset+used, len(payload))
	}

	return header, command
}
