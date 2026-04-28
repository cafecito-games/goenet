package protocol

import (
	"encoding/binary"
	"fmt"
)

// PacketCommand is an ENet protocol command payload.
type PacketCommand interface {
	MarshalBinary(dst []byte) []byte
}

// Acknowledge matches ENetProtocolAcknowledge.
type Acknowledge struct {
	ChannelID                      uint8
	ReliableSequenceNumber         uint16
	ReceivedReliableSequenceNumber uint16
	ReceivedSentTime               uint16
}

// Connect matches ENetProtocolConnect.
type Connect struct {
	ChannelID                  uint8
	ReliableSequenceNumber     uint16
	OutgoingPeerID             uint16
	IncomingSessionID          uint8
	OutgoingSessionID          uint8
	MTU                        uint32
	WindowSize                 uint32
	ChannelCount               uint32
	IncomingBandwidth          uint32
	OutgoingBandwidth          uint32
	PacketThrottleInterval     uint32
	PacketThrottleAcceleration uint32
	PacketThrottleDeceleration uint32
	ConnectID                  uint32
	Data                       uint32
}

func (a Acknowledge) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, acknowledgeCommandSize)...)
	dst[start] = byte(CommandAcknowledge)
	dst[start+1] = a.ChannelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], a.ReliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], a.ReceivedReliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], a.ReceivedSentTime)
	return dst
}

func (c Connect) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, connectCommandSize)...)
	dst[start] = byte(CommandConnect | Command(CommandFlagAcknowledge))
	dst[start+1] = c.ChannelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], c.ReliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], c.OutgoingPeerID)
	dst[start+6] = c.IncomingSessionID
	dst[start+7] = c.OutgoingSessionID
	binary.BigEndian.PutUint32(dst[start+8:start+12], c.MTU)
	binary.BigEndian.PutUint32(dst[start+12:start+16], c.WindowSize)
	binary.BigEndian.PutUint32(dst[start+16:start+20], c.ChannelCount)
	binary.BigEndian.PutUint32(dst[start+20:start+24], c.IncomingBandwidth)
	binary.BigEndian.PutUint32(dst[start+24:start+28], c.OutgoingBandwidth)
	binary.BigEndian.PutUint32(dst[start+28:start+32], c.PacketThrottleInterval)
	binary.BigEndian.PutUint32(dst[start+32:start+36], c.PacketThrottleAcceleration)
	binary.BigEndian.PutUint32(dst[start+36:start+40], c.PacketThrottleDeceleration)
	// The target ENet fork copies connectID straight through without host-to-net conversion.
	binary.LittleEndian.PutUint32(dst[start+40:start+44], c.ConnectID)
	binary.BigEndian.PutUint32(dst[start+44:start+48], c.Data)
	return dst
}

func ParseCommand(src []byte) (PacketCommand, int, error) {
	if len(src) < commandHeaderSize {
		return nil, 0, fmt.Errorf("protocol command too short: got %d bytes", len(src))
	}

	switch Command(src[0] & byte(CommandMask)) {
	case CommandAcknowledge:
		return parseAcknowledge(src)
	case CommandConnect:
		return parseConnect(src)
	default:
		return nil, 0, fmt.Errorf("unsupported protocol command: 0x%02x", src[0])
	}
}

func parseAcknowledge(src []byte) (Acknowledge, int, error) {
	if len(src) < acknowledgeCommandSize {
		return Acknowledge{}, 0, fmt.Errorf("acknowledge command too short: got %d bytes", len(src))
	}

	return Acknowledge{
		ChannelID:                      src[1],
		ReliableSequenceNumber:         binary.BigEndian.Uint16(src[2:4]),
		ReceivedReliableSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		ReceivedSentTime:               binary.BigEndian.Uint16(src[6:8]),
	}, acknowledgeCommandSize, nil
}

func parseConnect(src []byte) (Connect, int, error) {
	if len(src) < connectCommandSize {
		return Connect{}, 0, fmt.Errorf("connect command too short: got %d bytes", len(src))
	}

	return Connect{
		ChannelID:                  src[1],
		ReliableSequenceNumber:     binary.BigEndian.Uint16(src[2:4]),
		OutgoingPeerID:             binary.BigEndian.Uint16(src[4:6]),
		IncomingSessionID:          src[6],
		OutgoingSessionID:          src[7],
		MTU:                        binary.BigEndian.Uint32(src[8:12]),
		WindowSize:                 binary.BigEndian.Uint32(src[12:16]),
		ChannelCount:               binary.BigEndian.Uint32(src[16:20]),
		IncomingBandwidth:          binary.BigEndian.Uint32(src[20:24]),
		OutgoingBandwidth:          binary.BigEndian.Uint32(src[24:28]),
		PacketThrottleInterval:     binary.BigEndian.Uint32(src[28:32]),
		PacketThrottleAcceleration: binary.BigEndian.Uint32(src[32:36]),
		PacketThrottleDeceleration: binary.BigEndian.Uint32(src[36:40]),
		ConnectID:                  binary.LittleEndian.Uint32(src[40:44]),
		Data:                       binary.BigEndian.Uint32(src[44:48]),
	}, connectCommandSize, nil
}
