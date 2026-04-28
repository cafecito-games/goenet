package goenet

import "github.com/cafecito-games/goenet/internal/core"

// EventType identifies the kind of network event that occurred.
type EventType = core.EventType

const (
	EventNone              = core.EventNone
	EventConnect           = core.EventConnect
	EventDisconnect        = core.EventDisconnect
	EventReceive           = core.EventReceive
	EventDisconnectTimeout = core.EventDisconnectTimeout
)

// Event is the public event shape returned by host service loops and intercept hooks.
type Event struct {
	Type      EventType
	Peer      *Peer
	ChannelID uint8
	Data      uint32
	Packet    *Packet
}
