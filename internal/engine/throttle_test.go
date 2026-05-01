package engine

import (
	"context"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	ipeer "github.com/cafecito-games/goenet/internal/peer"
	iprotocol "github.com/cafecito-games/goenet/internal/protocol"
)

func TestAcknowledgeUpdatesRoundTripVarianceAndThrottleWindow(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	host.serviceTime = 2100

	raw.LastReceiveTime = 1
	raw.PacketThrottle = 10
	raw.PacketThrottleLimit = 32
	raw.PacketThrottleAcceleration = 2
	raw.PacketThrottleDeceleration = 2
	raw.PacketThrottleInterval = 5000
	raw.RoundTripTime = 100
	raw.RoundTripTimeVariance = 20
	raw.LastRoundTripTime = 80
	raw.LastRoundTripTimeVariance = 10
	raw.LowestRoundTripTime = 90
	raw.HighestRoundTripTimeVariance = 22

	raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1960,
		RoundTripTimeout:       500,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
	})

	ok := host.handleAcknowledge(raw, iprotocol.Acknowledge{
		Header: iprotocol.CommandHeader{
			ChannelID:              0,
			ReliableSequenceNumber: 1,
		},
		ReceivedReliableSequenceNumber: 1,
		ReceivedSentTime:               1960,
	})
	if !ok {
		t.Fatal("handleAcknowledge returned false")
	}

	if raw.PacketThrottle != 8 {
		t.Fatalf("packet throttle = %d, want 8", raw.PacketThrottle)
	}
	if raw.RoundTripTime != 105 {
		t.Fatalf("round trip time = %d, want 105", raw.RoundTripTime)
	}
	if raw.RoundTripTimeVariance != 25 {
		t.Fatalf("round trip time variance = %d, want 25", raw.RoundTripTimeVariance)
	}
	if raw.LastRoundTripTime != 90 {
		t.Fatalf("last round trip time = %d, want 90", raw.LastRoundTripTime)
	}
	if raw.LastRoundTripTimeVariance != 25 {
		t.Fatalf("last round trip time variance = %d, want 25", raw.LastRoundTripTimeVariance)
	}
	if raw.LowestRoundTripTime != 105 {
		t.Fatalf("lowest round trip time = %d, want 105", raw.LowestRoundTripTime)
	}
	if raw.HighestRoundTripTimeVariance != 25 {
		t.Fatalf("highest round trip time variance = %d, want 25", raw.HighestRoundTripTimeVariance)
	}
	if raw.PacketThrottleEpoch != 2100 {
		t.Fatalf("packet throttle epoch = %d, want 2100", raw.PacketThrottleEpoch)
	}
	if raw.LastReceiveTime != 2100 {
		t.Fatalf("last receive time = %d, want 2100", raw.LastReceiveTime)
	}
	if raw.EarliestTimeout != 0 {
		t.Fatalf("earliest timeout = %d, want 0", raw.EarliestTimeout)
	}
}

func TestAcknowledgeAdvancesNextTimeoutToNextInFlightReliable(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	host.serviceTime = 1300

	raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1000,
		RoundTripTimeout:       200,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
	})
	raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 2,
		SentTime:               1200,
		RoundTripTimeout:       300,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 2,
			},
		},
	})
	raw.NextTimeout = 1200

	ok := host.handleAcknowledge(raw, iprotocol.Acknowledge{
		Header: iprotocol.CommandHeader{
			ChannelID:              0,
			ReliableSequenceNumber: 1,
		},
		ReceivedReliableSequenceNumber: 1,
		ReceivedSentTime:               1000,
	})
	if !ok {
		t.Fatal("handleAcknowledge returned false")
	}

	if got := raw.SentReliableCommands.Len(); got != 1 {
		t.Fatalf("in-flight reliable count = %d, want 1", got)
	}
	if got := raw.NextTimeout; got != 1500 {
		t.Fatalf("next timeout = %d, want 1500", got)
	}
}

func TestServiceRetransmitsExpiredReliableButFlushDoesNot(t *testing.T) {
	host, sock := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	packet := &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable}
	if err := host.Send(raw, 0, packet); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("initial write count = %d, want 1", got)
	}

	cmd := raw.SentReliableCommands.Front().Value()
	cmd.SentTime = 1000
	cmd.RoundTripTimeout = 200
	cmd.SendAttempts = 1
	raw.RoundTripTime = 100
	raw.RoundTripTimeVariance = 25
	raw.NextTimeout = 1200
	raw.ReliableDataInTransit = uint32(cmd.FragmentLength)
	host.serviceTime = 2000

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != 1 {
		t.Fatalf("flush write count = %d, want 1", got)
	}
	if got := raw.OutgoingSendReliableCommands.Len(); got != 0 {
		t.Fatalf("queued reliable count after flush = %d, want 0", got)
	}

	if _, err := host.Service(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := sock.WriteCount(); got != 2 {
		t.Fatalf("service write count = %d, want 2", got)
	}
	if got := raw.SentReliableCommands.Len(); got != 1 {
		t.Fatalf("in-flight reliable count after service = %d, want 1", got)
	}

	retransmit := raw.SentReliableCommands.Front().Value()
	if retransmit.SendAttempts != 2 {
		t.Fatalf("send attempts = %d, want 2", retransmit.SendAttempts)
	}
	if retransmit.RoundTripTimeout != 200 {
		t.Fatalf("round trip timeout = %d, want 200", retransmit.RoundTripTimeout)
	}
}

func TestServiceDisconnectsOnTimeoutMinimumAndLimitBranch(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	host.serviceTime = 7000

	raw.RoundTripTime = 100
	raw.RoundTripTimeVariance = 25
	raw.TimeoutLimit = 32
	raw.TimeoutMinimum = 5000
	raw.TimeoutMaximum = 30000
	raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1000,
		RoundTripTimeout:       500,
		SendAttempts:           6,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
		Packet: &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable},
	})

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventDisconnectTimeout {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventDisconnectTimeout)
	}
	if raw.State != core.PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateDisconnected)
	}
}

func TestServiceDisconnectsOnTimeoutMaximumBranch(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)
	host.serviceTime = 32000

	raw.RoundTripTime = 100
	raw.RoundTripTimeVariance = 25
	raw.TimeoutLimit = 32
	raw.TimeoutMinimum = 5000
	raw.TimeoutMaximum = 30000
	raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1000,
		RoundTripTimeout:       500,
		SendAttempts:           1,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
		Packet: &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable},
	})

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventDisconnectTimeout {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventDisconnectTimeout)
	}
	if raw.State != core.PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateDisconnected)
	}
}

func TestServiceTimeoutDuringHandshakeResetsSilently(t *testing.T) {
	tests := []struct {
		name  string
		state core.PeerState
	}{
		{
			name:  "acknowledging connect",
			state: core.PeerStateAcknowledgingConnect,
		},
		{
			name:  "connection pending",
			state: core.PeerStateConnectionPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, _ := newTestHost(t)
			raw := host.AddPeer(mustConnectedPeer(t, host).Address, tt.state)
			host.serviceTime = 32000

			raw.RoundTripTime = 100
			raw.RoundTripTimeVariance = 25
			raw.TimeoutLimit = 32
			raw.TimeoutMinimum = 5000
			raw.TimeoutMaximum = 30000
			raw.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
				ReliableSequenceNumber: 1,
				SentTime:               1000,
				RoundTripTimeout:       500,
				SendAttempts:           1,
				Command: ipeer.Command{
					Header: ipeer.Header{
						Command:                iprotocol.CommandSendReliable,
						ChannelID:              0,
						Flags:                  iprotocol.CommandFlagAcknowledge,
						ReliableSequenceNumber: 1,
					},
				},
				Packet: &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable},
			})

			event, err := host.Service(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if event.Type != core.EventNone {
				t.Fatalf("event type = %d, want %d", event.Type, core.EventNone)
			}
			if raw.State != core.PeerStateDisconnected {
				t.Fatalf("peer state = %d, want %d", raw.State, core.PeerStateDisconnected)
			}
		})
	}
}

func TestServiceSilentHandshakeTimeoutContinuesServicingOtherPeers(t *testing.T) {
	host, sock := newReceiveHost(t, nil)

	timedOut := host.AddPeer(mustAddress(t, "127.0.0.1:9001"), core.PeerStateAcknowledgingConnect)
	timedOut.IncomingPeerID = 0
	timedOut.IncomingSessionID = 1
	timedOut.RoundTripTime = 100
	timedOut.RoundTripTimeVariance = 25
	timedOut.TimeoutLimit = 32
	timedOut.TimeoutMinimum = 5000
	timedOut.TimeoutMaximum = 30000
	timedOut.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1000,
		RoundTripTimeout:       500,
		SendAttempts:           1,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
		Packet: &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable},
	})

	ready := host.AddPeer(mustAddress(t, "127.0.0.1:9002"), core.PeerStateConnected)
	ready.IncomingPeerID = 1
	ready.IncomingSessionID = 1

	sock.QueueInbound(ready.Address.AddrPort(), marshalDatagram(
		iprotocol.Header{
			PeerID:    ready.IncomingPeerID,
			SessionID: ready.IncomingSessionID,
			Flags:     iprotocol.HeaderFlagSentTime,
			SentTime:  0x4242,
		},
		iprotocol.SendReliable{
			Header: iprotocol.CommandHeader{
				ChannelID:              0,
				ReliableSequenceNumber: 1,
			},
			Data: []byte("next"),
		},
	))

	host.serviceTime = 32000
	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventReceive {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventReceive)
	}
	if got := string(event.Packet.Data); got != "next" {
		t.Fatalf("packet = %q, want %q", got, "next")
	}
	if timedOut.State != core.PeerStateDisconnected {
		t.Fatalf("timedOut state = %d, want %d", timedOut.State, core.PeerStateDisconnected)
	}
}

func TestTimeoutDisconnectMarksBandwidthLimitsDirtyForLaterThrottlePass(t *testing.T) {
	host, _ := newTestHost(t)
	timedOut := mustConnectedPeer(t, host)
	survivor := host.AddPeer(timedOut.Address, core.PeerStateConnected)

	host.serviceTime = 32000
	host.throttle.incomingBudget = 800
	host.throttle.outgoingBudget = 1600
	timedOut.RoundTripTime = 100
	timedOut.RoundTripTimeVariance = 25
	timedOut.TimeoutLimit = 32
	timedOut.TimeoutMinimum = 5000
	timedOut.TimeoutMaximum = 30000
	timedOut.SentReliableCommands.PushBack(&ipeer.OutgoingCommand{
		ReliableSequenceNumber: 1,
		SentTime:               1000,
		RoundTripTimeout:       500,
		SendAttempts:           1,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandSendReliable,
				ChannelID:              0,
				Flags:                  iprotocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: 1,
			},
		},
		Packet: &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable},
	})

	event, err := host.Service(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != core.EventDisconnectTimeout {
		t.Fatalf("event type = %d, want %d", event.Type, core.EventDisconnectTimeout)
	}
	if !host.throttle.needsRecalculation {
		t.Fatal("recalculate bandwidth limits = false, want true")
	}

	host.throttle.epoch = 0
	host.serviceTime = 33000
	host.throttle.Run(host.serviceTime, host.peers, host.queueOutgoingControlCommand, host.logger)

	if got := survivor.OutgoingCommands.Len(); got != 1 {
		t.Fatalf("survivor outgoing command count = %d, want 1", got)
	}
	cmd := survivor.OutgoingCommands.Front().Value()
	if cmd.Command.Header.Command != iprotocol.CommandBandwidthLimit {
		t.Fatalf("command = %v, want %v", cmd.Command.Header.Command, iprotocol.CommandBandwidthLimit)
	}
}

func TestOutgoingDataTotalTracksQueuedCommandsNotSerializedDatagrams(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	packet := &core.Packet{Data: []byte("abc"), Flags: core.PacketFlagReliable}
	if err := host.Send(raw, 0, packet); err != nil {
		t.Fatal(err)
	}

	if got := raw.OutgoingDataTotal; got != 9 {
		t.Fatalf("outgoing data total after queue = %d, want 9", got)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := raw.OutgoingDataTotal; got != 9 {
		t.Fatalf("outgoing data total after flush = %d, want 9", got)
	}
}

func TestOutgoingDataTotalIncludesAcknowledgementCommandBytes(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	host.queueAcknowledgement(raw, iprotocol.CommandHeader{
		Command:                iprotocol.CommandPing,
		ChannelID:              0,
		ReliableSequenceNumber: 7,
	}, 123)

	expected := uint32(marshalAcknowledgement(raw.Acknowledgements.Front().Value()).WireSize())
	if got := raw.OutgoingDataTotal; got != expected {
		t.Fatalf("outgoing data total after ack queue = %d, want %d", got, expected)
	}

	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	expected = uint32(marshalAcknowledgement(&ipeer.Acknowledgement{
		SentTime: 123,
		Command: ipeer.Command{
			Header: ipeer.Header{
				Command:                iprotocol.CommandPing,
				ChannelID:              0,
				ReliableSequenceNumber: 7,
			},
		},
	}).WireSize())
	if got := raw.OutgoingDataTotal; got != expected {
		t.Fatalf("outgoing data total after ack flush = %d, want %d", got, expected)
	}
}

func TestServiceBandwidthThrottleIsNoOpBeforeEpochInterval(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	host.serviceTime = 1500
	host.throttle.epoch = 1000
	host.throttle.outgoingBudget = 4000
	host.throttle.incomingBudget = 4000
	host.throttle.limitedPeers = 1
	raw.IncomingBandwidth = 1000
	raw.OutgoingDataTotal = 900
	raw.IncomingDataTotal = 800
	raw.PacketThrottle = 20
	raw.PacketThrottleLimit = 25

	if _, err := host.Service(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	if raw.PacketThrottleLimit != 25 {
		t.Fatalf("packet throttle limit = %d, want 25", raw.PacketThrottleLimit)
	}
	if raw.OutgoingDataTotal != 900 {
		t.Fatalf("outgoing data total = %d, want 900", raw.OutgoingDataTotal)
	}
	if raw.IncomingDataTotal != 800 {
		t.Fatalf("incoming data total = %d, want 800", raw.IncomingDataTotal)
	}
}

func TestBandwidthThrottleClampsPacketThrottleLimitAndResetsDataTotals(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	host.serviceTime = 2000
	host.throttle.epoch = 0
	host.throttle.outgoingBudget = 1000
	host.throttle.limitedPeers = 1
	raw.IncomingBandwidth = 500
	raw.OutgoingDataTotal = 4000
	raw.IncomingDataTotal = 3000
	raw.PacketThrottle = 32
	raw.PacketThrottleLimit = 32

	host.throttle.Run(host.serviceTime, host.peers, host.queueOutgoingControlCommand, host.logger)

	if raw.PacketThrottleLimit != 8 {
		t.Fatalf("packet throttle limit = %d, want 8", raw.PacketThrottleLimit)
	}
	if raw.PacketThrottle != 8 {
		t.Fatalf("packet throttle = %d, want 8", raw.PacketThrottle)
	}
	if raw.OutgoingDataTotal != 0 {
		t.Fatalf("outgoing data total = %d, want 0", raw.OutgoingDataTotal)
	}
	if raw.IncomingDataTotal != 0 {
		t.Fatalf("incoming data total = %d, want 0", raw.IncomingDataTotal)
	}
}

func TestBandwidthThrottleQueuesBandwidthLimitCommandWhenRecalculationRequested(t *testing.T) {
	host, _ := newTestHost(t)
	raw := mustConnectedPeer(t, host)

	host.serviceTime = 2000
	host.throttle.epoch = 0
	host.throttle.incomingBudget = 800
	host.throttle.outgoingBudget = 1600
	host.throttle.needsRecalculation = true
	raw.OutgoingBandwidth = 300

	host.throttle.Run(host.serviceTime, host.peers, host.queueOutgoingControlCommand, host.logger)

	if got := raw.OutgoingCommands.Len(); got != 1 {
		t.Fatalf("outgoing command count = %d, want 1", got)
	}

	cmd := raw.OutgoingCommands.Front().Value()
	if cmd.Command.Header.Command != iprotocol.CommandBandwidthLimit {
		t.Fatalf("command = %v, want %v", cmd.Command.Header.Command, iprotocol.CommandBandwidthLimit)
	}

	limit, ok := cmd.Command.Payload.(*iprotocol.BandwidthLimit)
	if !ok {
		t.Fatalf("payload type = %T", cmd.Command.Payload)
	}
	if limit.IncomingBandwidth != 300 {
		t.Fatalf("incoming bandwidth = %d, want 300", limit.IncomingBandwidth)
	}
	if limit.OutgoingBandwidth != 1600 {
		t.Fatalf("outgoing bandwidth = %d, want 1600", limit.OutgoingBandwidth)
	}
}
