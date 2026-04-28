package goenet

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
