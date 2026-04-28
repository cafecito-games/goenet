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
