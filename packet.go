package goenet

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
