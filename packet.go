package goenet

import "github.com/cafecito-games/goenet/internal/core"

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag uint32

const (
	PacketFlagReliable    PacketFlag = 1 << 0
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

	return &core.Packet{
		Data:  packet.Data,
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
