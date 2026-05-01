package goenet

import "github.com/cafecito-games/goenet/internal/core"

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag = core.PacketFlag

const (
	// PacketFlagReliable requests reliable delivery semantics.
	PacketFlagReliable = core.PacketFlagReliable
	// PacketFlagUnsequenced requests ENet's unsequenced delivery mode.
	PacketFlagUnsequenced = core.PacketFlagUnsequenced
)

// Packet is the public packet payload value type.
type Packet = core.Packet

// copyPacketIn copies the caller's payload bytes so the engine, which retains
// the slice for retransmits, never observes mid-flight mutations from a caller
// that reuses its send buffer. This restores the normal Go expectation that
// "buffer reuse is safe after the call returns" for the public API surface.
func copyPacketIn(packet *Packet) *core.Packet {
	if packet == nil {
		return nil
	}
	return &core.Packet{
		Data:  append([]byte(nil), packet.Data...),
		Flags: packet.Flags,
	}
}
