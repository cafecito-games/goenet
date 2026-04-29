package goenet

import "github.com/cafecito-games/goenet/internal/core"

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag uint32

const (
	// PacketFlagReliable requests reliable delivery semantics.
	PacketFlagReliable PacketFlag = 1 << 0
	// PacketFlagUnsequenced requests ENet's unsequenced delivery mode.
	PacketFlagUnsequenced PacketFlag = 1 << 1
)

// Packet is the public packet payload value type.
type Packet struct {
	Data  []byte
	Flags PacketFlag
}

func toCorePacket(packet *Packet) *core.Packet {
	if packet == nil {
		return nil
	}

	// Copy Data so the engine, which retains the bytes for retransmits, never
	// observes mid-flight mutations from a caller that reuses its send buffer.
	// This matches Go's normal "buffer reuse is safe after the call returns"
	// expectation that the public API would otherwise violate silently.
	data := append([]byte(nil), packet.Data...)
	return &core.Packet{
		Data:  data,
		Flags: core.PacketFlag(packet.Flags),
	}
}

func fromCorePacket(packet *core.Packet) *Packet {
	if packet == nil {
		return nil
	}

	return &Packet{
		Data:  packet.Data,
		Flags: PacketFlag(packet.Flags),
	}
}
