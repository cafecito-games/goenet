package goenet

import "github.com/cafecito-games/goenet/internal/core"

// EventType identifies the kind of network event that occurred.
type EventType uint8

const (
	EventNone EventType = iota
	EventConnect
	EventDisconnect
	EventReceive
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

func fromCoreEvent(event *core.Event) *Event {
	if event == nil {
		return nil
	}

	return &Event{
		Type:      EventType(event.Type),
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    fromCorePacket(event.Packet),
	}
}
