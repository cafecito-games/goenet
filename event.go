package goenet

import "github.com/cafecito-games/goenet/internal/core"

// EventType identifies the kind of network event that occurred.
type EventType uint8

const (
	// EventNone reports that no event was produced during a service step.
	EventNone EventType = iota
	// EventConnect reports that a peer completed the ENet connection handshake.
	EventConnect
	// EventDisconnect reports that a peer disconnected cleanly.
	EventDisconnect
	// EventReceive reports that a packet payload arrived for a peer channel.
	EventReceive
	// EventDisconnectTimeout reports that a peer timed out locally.
	EventDisconnectTimeout
)

// Event is the public event shape returned by host service loops and intercept hooks.
type Event struct {
	Type      EventType
	Peer      *Peer
	ChannelID uint8
	Data      uint32
	Packet    *Packet
}

func toCoreEvent(event *Event) *core.Event {
	if event == nil {
		return nil
	}

	return &core.Event{
		Type:      core.EventType(event.Type),
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    toCorePacket(event.Packet),
	}
}
