package goenet

import "github.com/cafecito-games/goenet/internal/core"

// EventType identifies the kind of network event that occurred.
type EventType = core.EventType

const (
	// EventNone reports that no event was produced during a service step.
	EventNone = core.EventNone
	// EventConnect reports that a peer completed the ENet connection handshake.
	EventConnect = core.EventConnect
	// EventDisconnect reports that a peer disconnected cleanly.
	EventDisconnect = core.EventDisconnect
	// EventReceive reports that a packet payload arrived for a peer channel.
	EventReceive = core.EventReceive
	// EventDisconnectTimeout reports that a peer timed out locally.
	EventDisconnectTimeout = core.EventDisconnectTimeout
)

// Event is the public event shape returned by host service loops and intercept hooks.
//
// Unlike the engine-internal core.Event, this carries a wrapped *Peer handle
// rather than the raw peer pointer; it is therefore not a pure type alias.
type Event struct {
	Type      EventType
	Peer      *Peer
	ChannelID uint8
	Data      uint32
	Packet    *Packet
}

// toCoreEvent converts a public Event back to a core.Event for use by the
// peerless intercept hook path. The wrapped Peer is dropped because intercept
// events are not associated with a specific peer slot.
func toCoreEvent(event *Event) *core.Event {
	if event == nil {
		return nil
	}
	return &core.Event{
		Type:      event.Type,
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    event.Packet,
	}
}
