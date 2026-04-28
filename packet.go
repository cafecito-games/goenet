package goenet

import "github.com/cafecito-games/goenet/internal/core"

// PacketFlag controls how a packet is sent or interpreted.
type PacketFlag = core.PacketFlag

const (
	PacketFlagReliable    = core.PacketFlagReliable
	PacketFlagUnsequenced = core.PacketFlagUnsequenced
)

// Packet is the public packet payload value type.
type Packet = core.Packet
