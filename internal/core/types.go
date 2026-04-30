// Package core holds shared internal transport-facing value types.
package core

import (
	"log/slog"
	"net/netip"
)

const (
	defaultMTU                uint32 = 1392
	defaultMaximumPacketSize  uint32 = 32 * 1024 * 1024
	defaultMaximumWaitingData uint32 = 32 * 1024 * 1024
	defaultPeerCount                 = 1
)

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag uint32

const (
	// PacketFlagReliable requests reliable delivery semantics.
	PacketFlagReliable PacketFlag = 1 << 0
	// PacketFlagUnsequenced requests ENet's unsequenced delivery mode.
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

// PeerState mirrors ENetPeerState ordinal values.
type PeerState uint8

const (
	// PeerStateDisconnected reports that no live session exists for the peer.
	PeerStateDisconnected PeerState = iota
	// PeerStateConnecting reports that an outbound connect command was queued.
	PeerStateConnecting
	// PeerStateAcknowledgingConnect reports that the peer is acknowledging an inbound connect.
	PeerStateAcknowledgingConnect
	// PeerStateConnectionPending reports that the peer is waiting for verify-connect.
	PeerStateConnectionPending
	// PeerStateConnectionSucceeded reports that the connect handshake has succeeded locally.
	PeerStateConnectionSucceeded
	// PeerStateConnected reports that the peer is fully connected.
	PeerStateConnected
	// PeerStateDisconnectLater reports that disconnect is deferred until reliable queues drain.
	PeerStateDisconnectLater
	// PeerStateDisconnecting reports that a graceful disconnect is in progress.
	PeerStateDisconnecting
	// PeerStateAcknowledgingDisconnect reports that disconnect acknowledgment is pending.
	PeerStateAcknowledgingDisconnect
	// PeerStateZombie reports that the peer is awaiting local cleanup after disconnect.
	PeerStateZombie
)

// Checksummer computes a checksum across the provided buffer slices, treated as a
// single concatenated byte sequence.
type Checksummer interface {
	Checksum(buffers [][]byte) uint32
}

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
// buffers passed to Compress are concatenated input.
type Compressor interface {
	Compress(buffers [][]byte, inLimit int, out []byte) (int, error)
	Decompress(in []byte, out []byte) (int, error)
}

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult uint8

const (
	// InterceptResultContinue lets the packet continue through normal protocol handling.
	InterceptResultContinue InterceptResult = iota
	// InterceptResultConsume stops normal protocol handling for the packet.
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
	Logger             *slog.Logger
}

// DefaultConfig returns ENet-compatible host defaults suitable for a single-peer client.
// Servers should override PeerCount to reflect the maximum number of accepted connections.
func DefaultConfig() Config {
	return Config{
		PeerCount:          defaultPeerCount,
		MTU:                defaultMTU,
		MaximumPacketSize:  defaultMaximumPacketSize,
		MaximumWaitingData: defaultMaximumWaitingData,
	}
}
