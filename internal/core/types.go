package core

import "net/netip"

const (
	defaultMTU                uint32 = 1392
	defaultMaximumPacketSize  uint32 = 32 * 1024 * 1024
	defaultMaximumWaitingData uint32 = 32 * 1024 * 1024
)

// Buffer is one byte slice passed to checksum and compression hooks.
type Buffer struct {
	Data []byte
}

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag uint32

const (
	PacketFlagReliable    PacketFlag = 1 << 0
	PacketFlagUnsequenced PacketFlag = 1 << 1
)

// Packet is the shared packet payload value type.
type Packet struct {
	Data  []byte
	Flags PacketFlag
}

// EventType identifies the kind of network event that occurred.
type EventType uint8

const (
	EventNone EventType = iota
	EventConnect
	EventDisconnect
	EventReceive
	EventDisconnectTimeout
)

// PeerState mirrors ENetPeerState ordinal values.
type PeerState uint8

const (
	PeerStateDisconnected PeerState = iota
	PeerStateConnecting
	PeerStateAcknowledgingConnect
	PeerStateConnectionPending
	PeerStateConnectionSucceeded
	PeerStateConnected
	PeerStateDisconnectLater
	PeerStateDisconnecting
	PeerStateAcknowledgingDisconnect
	PeerStateZombie
)

// Checksummer computes a checksum across the provided buffers.
type Checksummer interface {
	Checksum(buffers []Buffer) uint32
}

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
type Compressor interface {
	Compress(buffers []Buffer, inLimit int, out []byte) (int, error)
	Decompress(in []byte, out []byte) (int, error)
}

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult uint8

const (
	InterceptResultContinue InterceptResult = iota
	InterceptResultConsume
)

// Event is the peerless event shape used by low-level intercept hooks.
type Event struct {
	Type      EventType
	ChannelID uint8
	Data      uint32
	Packet    *Packet
}

// InterceptDecision reports whether an interceptor consumed the packet and whether it synthesized an event.
type InterceptDecision struct {
	Result InterceptResult
	Event  *Event
}

// Interceptor can consume a received raw UDP packet before protocol decoding and optionally synthesize a service event.
type Interceptor interface {
	Intercept(addr netip.AddrPort, payload []byte) (InterceptDecision, error)
}

// Config configures host construction and ENet compatibility limits.
type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
	Checksum           Checksummer
	Compressor         Compressor
	Intercept          Interceptor
}

// DefaultConfig returns ENet-compatible host defaults.
func DefaultConfig() Config {
	return Config{
		MTU:                defaultMTU,
		MaximumPacketSize:  defaultMaximumPacketSize,
		MaximumWaitingData: defaultMaximumWaitingData,
	}
}
