// Package protocol implements ENet packet headers and command wire formats.
package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
)

// PacketCommand is an ENet protocol command payload.
type PacketCommand interface {
	MarshalBinary(dst []byte) []byte
}

// CommandHeader preserves the common ENet command header plus packed command flags.
type CommandHeader struct {
	Command                Command
	ChannelID              uint8
	Flags                  CommandFlag
	ReliableSequenceNumber uint16
}

// Acknowledge matches ENetProtocolAcknowledge.
type Acknowledge struct {
	Header                         CommandHeader
	ReceivedReliableSequenceNumber uint16
	ReceivedSentTime               uint16
}

// Connect matches ENetProtocolConnect.
type Connect struct {
	Header                     CommandHeader
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

// VerifyConnect matches ENetProtocolVerifyConnect.
type VerifyConnect struct {
	Header                     CommandHeader
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
}

// Disconnect matches ENetProtocolDisconnect.
type Disconnect struct {
	Header CommandHeader
	Data   uint32
}

// Ping matches ENetProtocolPing.
type Ping struct {
	Header CommandHeader
}

// SendReliable matches ENetProtocolSendReliable.
type SendReliable struct {
	Header CommandHeader
	Data   []byte
}

// SendUnreliable matches ENetProtocolSendUnreliable.
type SendUnreliable struct {
	Header                   CommandHeader
	UnreliableSequenceNumber uint16
	Data                     []byte
}

// SendUnsequenced matches ENetProtocolSendUnsequenced.
type SendUnsequenced struct {
	Header           CommandHeader
	UnsequencedGroup uint16
	Data             []byte
}

// SendFragment matches ENetProtocolSendFragment and ENetProtocolSendUnreliableFragment.
type SendFragment struct {
	Header              CommandHeader
	StartSequenceNumber uint16
	FragmentCount       uint32
	FragmentNumber      uint32
	TotalLength         uint32
	FragmentOffset      uint32
	Data                []byte
}

// BandwidthLimit matches ENetProtocolBandwidthLimit.
type BandwidthLimit struct {
	Header            CommandHeader
	IncomingBandwidth uint32
	OutgoingBandwidth uint32
}

// ThrottleConfigure matches ENetProtocolThrottleConfigure.
type ThrottleConfigure struct {
	Header                     CommandHeader
	PacketThrottleInterval     uint32
	PacketThrottleAcceleration uint32
	PacketThrottleDeceleration uint32
}

// MarshalBinary appends the ENet wire encoding of the command header to dst.
func (h CommandHeader) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, commandHeaderSize)...)
	dst[start] = byte(h.Command) | byte(h.Flags)
	dst[start+1] = h.ChannelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], h.ReliableSequenceNumber)
	return dst
}

// ParseCommandHeader decodes one ENet command header from src.
func ParseCommandHeader(src []byte) (CommandHeader, error) {
	if len(src) < commandHeaderSize {
		return CommandHeader{}, fmt.Errorf("protocol command too short: got %d bytes", len(src))
	}

	return CommandHeader{
		Command:                Command(src[0] & byte(CommandMask)),
		ChannelID:              src[1],
		Flags:                  CommandFlag(src[0]) &^ CommandMask,
		ReliableSequenceNumber: binary.BigEndian.Uint16(src[2:4]),
	}, nil
}

// MarshalBinary appends the ENet wire encoding of the acknowledge command to dst.
func (a Acknowledge) MarshalBinary(dst []byte) []byte {
	header := a.Header
	header.Command = CommandAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, acknowledgeCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint16(dst[start+4:start+6], a.ReceivedReliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], a.ReceivedSentTime)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the connect command to dst.
func (c Connect) MarshalBinary(dst []byte) []byte {
	header := c.Header
	header.Command = CommandConnect
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, connectCommandSize-commandHeaderSize)...)
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

// MarshalBinary appends the ENet wire encoding of the verify-connect command to dst.
func (c VerifyConnect) MarshalBinary(dst []byte) []byte {
	header := c.Header
	header.Command = CommandVerifyConnect
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, verifyConnectCommandSize-commandHeaderSize)...)
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
	binary.LittleEndian.PutUint32(dst[start+40:start+44], c.ConnectID)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the disconnect command to dst.
func (d Disconnect) MarshalBinary(dst []byte) []byte {
	header := d.Header
	header.Command = CommandDisconnect

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, disconnectCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint32(dst[start+4:start+8], d.Data)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the ping command to dst.
func (p Ping) MarshalBinary(dst []byte) []byte {
	header := p.Header
	header.Command = CommandPing
	header.Flags |= CommandFlagAcknowledge
	return header.MarshalBinary(dst)
}

// MarshalBinary appends the ENet wire encoding of the reliable payload command to dst.
func (s SendReliable) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendReliable
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, sendReliableCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint16(dst[start+4:start+6], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the unreliable payload command to dst.
func (s SendUnreliable) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendUnreliable

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, sendUnreliableCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.UnreliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the unsequenced payload command to dst.
func (s SendUnsequenced) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendUnsequenced
	header.Flags |= CommandFlagUnsequenced

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, sendUnsequencedCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.UnsequencedGroup)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the fragment command to dst.
func (s SendFragment) MarshalBinary(dst []byte) []byte {
	header := s.Header
	if header.Command == 0 {
		header.Command = CommandSendFragment
	}
	if header.Command == CommandSendFragment {
		header.Flags |= CommandFlagAcknowledge
	}

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, sendFragmentCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.StartSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	binary.BigEndian.PutUint32(dst[start+8:start+12], s.FragmentCount)
	binary.BigEndian.PutUint32(dst[start+12:start+16], s.FragmentNumber)
	binary.BigEndian.PutUint32(dst[start+16:start+20], s.TotalLength)
	binary.BigEndian.PutUint32(dst[start+20:start+24], s.FragmentOffset)
	dst = append(dst, s.Data...)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the bandwidth-limit command to dst.
func (b BandwidthLimit) MarshalBinary(dst []byte) []byte {
	header := b.Header
	header.Command = CommandBandwidthLimit

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, bandwidthLimitCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint32(dst[start+4:start+8], b.IncomingBandwidth)
	binary.BigEndian.PutUint32(dst[start+8:start+12], b.OutgoingBandwidth)
	return dst
}

// MarshalBinary appends the ENet wire encoding of the throttle-configure command to dst.
func (t ThrottleConfigure) MarshalBinary(dst []byte) []byte {
	header := t.Header
	header.Command = CommandThrottleConfigure

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst = append(dst, make([]byte, throttleConfigureCommandSize-commandHeaderSize)...)
	binary.BigEndian.PutUint32(dst[start+4:start+8], t.PacketThrottleInterval)
	binary.BigEndian.PutUint32(dst[start+8:start+12], t.PacketThrottleAcceleration)
	binary.BigEndian.PutUint32(dst[start+12:start+16], t.PacketThrottleDeceleration)
	return dst
}

// ParseCommand decodes one ENet command from src and reports how many bytes it consumed.
func ParseCommand(src []byte) (PacketCommand, CommandFlag, int, error) {
	header, err := ParseCommandHeader(src)
	if err != nil {
		return nil, 0, 0, err
	}

	switch header.Command {
	case CommandAcknowledge:
		cmd, n, err := parseAcknowledge(header, src)
		return cmd, header.Flags, n, err
	case CommandConnect:
		cmd, n, err := parseConnect(header, src)
		return cmd, header.Flags, n, err
	case CommandVerifyConnect:
		cmd, n, err := parseVerifyConnect(header, src)
		return cmd, header.Flags, n, err
	case CommandDisconnect:
		cmd, n, err := parseDisconnect(header, src)
		return cmd, header.Flags, n, err
	case CommandPing:
		cmd, n, err := parsePing(header, src)
		return cmd, header.Flags, n, err
	case CommandSendReliable:
		cmd, n, err := parseSendReliable(header, src)
		return cmd, header.Flags, n, err
	case CommandSendUnreliable:
		cmd, n, err := parseSendUnreliable(header, src)
		return cmd, header.Flags, n, err
	case CommandSendUnsequenced:
		cmd, n, err := parseSendUnsequenced(header, src)
		return cmd, header.Flags, n, err
	case CommandSendFragment, CommandSendUnreliableFragment:
		cmd, n, err := parseSendFragment(header, src)
		return cmd, header.Flags, n, err
	case CommandBandwidthLimit:
		cmd, n, err := parseBandwidthLimit(header, src)
		return cmd, header.Flags, n, err
	case CommandThrottleConfigure:
		cmd, n, err := parseThrottleConfigure(header, src)
		return cmd, header.Flags, n, err
	default:
		return nil, 0, 0, fmt.Errorf("unsupported protocol command: 0x%02x", src[0])
	}
}

// payloadSize16 narrows a payload byte count to the 16-bit on-wire length field.
// Callers in the engine fragment outbound packets to <MTU before reaching marshal,
// so n cannot legitimately exceed math.MaxUint16; an overflow here indicates a
// programmer error in the caller (e.g. skipping fragmentation), not a wire-format
// failure, hence the panic rather than a returned error.
func payloadSize16(n int) uint16 {
	if n < 0 || n > math.MaxUint16 {
		panic(fmt.Sprintf("protocol payload too large for 16-bit length: %d", n))
	}

	return uint16(n)
}

func parseAcknowledge(header CommandHeader, src []byte) (Acknowledge, int, error) {
	if len(src) < acknowledgeCommandSize {
		return Acknowledge{}, 0, fmt.Errorf("acknowledge command too short: got %d bytes", len(src))
	}

	return Acknowledge{
		Header:                         header,
		ReceivedReliableSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		ReceivedSentTime:               binary.BigEndian.Uint16(src[6:8]),
	}, acknowledgeCommandSize, nil
}

func parseConnect(header CommandHeader, src []byte) (Connect, int, error) {
	if len(src) < connectCommandSize {
		return Connect{}, 0, fmt.Errorf("connect command too short: got %d bytes", len(src))
	}

	return Connect{
		Header:                     header,
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

func parseVerifyConnect(header CommandHeader, src []byte) (VerifyConnect, int, error) {
	if len(src) < verifyConnectCommandSize {
		return VerifyConnect{}, 0, fmt.Errorf("verify connect command too short: got %d bytes", len(src))
	}

	return VerifyConnect{
		Header:                     header,
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
	}, verifyConnectCommandSize, nil
}

func parseDisconnect(header CommandHeader, src []byte) (Disconnect, int, error) {
	if len(src) < disconnectCommandSize {
		return Disconnect{}, 0, fmt.Errorf("disconnect command too short: got %d bytes", len(src))
	}

	return Disconnect{
		Header: header,
		Data:   binary.BigEndian.Uint32(src[4:8]),
	}, disconnectCommandSize, nil
}

func parsePing(header CommandHeader, src []byte) (Ping, int, error) {
	if len(src) < pingCommandSize {
		return Ping{}, 0, fmt.Errorf("ping command too short: got %d bytes", len(src))
	}

	return Ping{Header: header}, pingCommandSize, nil
}

func parseSendReliable(header CommandHeader, src []byte) (SendReliable, int, error) {
	if len(src) < sendReliableCommandSize {
		return SendReliable{}, 0, fmt.Errorf("send reliable command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[4:6]))
	if len(src) < sendReliableCommandSize+dataLength {
		return SendReliable{}, 0, fmt.Errorf("send reliable payload too short: got %d bytes", len(src))
	}

	return SendReliable{
		Header: header,
		Data:   append([]byte(nil), src[6:6+dataLength]...),
	}, sendReliableCommandSize + dataLength, nil
}

func parseSendUnreliable(header CommandHeader, src []byte) (SendUnreliable, int, error) {
	if len(src) < sendUnreliableCommandSize {
		return SendUnreliable{}, 0, fmt.Errorf("send unreliable command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < sendUnreliableCommandSize+dataLength {
		return SendUnreliable{}, 0, fmt.Errorf("send unreliable payload too short: got %d bytes", len(src))
	}

	return SendUnreliable{
		Header:                   header,
		UnreliableSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		Data:                     append([]byte(nil), src[8:8+dataLength]...),
	}, sendUnreliableCommandSize + dataLength, nil
}

func parseSendUnsequenced(header CommandHeader, src []byte) (SendUnsequenced, int, error) {
	if len(src) < sendUnsequencedCommandSize {
		return SendUnsequenced{}, 0, fmt.Errorf("send unsequenced command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < sendUnsequencedCommandSize+dataLength {
		return SendUnsequenced{}, 0, fmt.Errorf("send unsequenced payload too short: got %d bytes", len(src))
	}

	return SendUnsequenced{
		Header:           header,
		UnsequencedGroup: binary.BigEndian.Uint16(src[4:6]),
		Data:             append([]byte(nil), src[8:8+dataLength]...),
	}, sendUnsequencedCommandSize + dataLength, nil
}

func parseSendFragment(header CommandHeader, src []byte) (SendFragment, int, error) {
	if len(src) < sendFragmentCommandSize {
		return SendFragment{}, 0, fmt.Errorf("send fragment command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < sendFragmentCommandSize+dataLength {
		return SendFragment{}, 0, fmt.Errorf("send fragment payload too short: got %d bytes", len(src))
	}

	fragmentCount := binary.BigEndian.Uint32(src[8:12])
	fragmentNumber := binary.BigEndian.Uint32(src[12:16])
	// Reject peer-controlled values that would otherwise drive a huge bitmap
	// allocation downstream (matches ENET_PROTOCOL_MAXIMUM_FRAGMENT_COUNT).
	if fragmentCount == 0 || fragmentCount > MaximumFragmentCount {
		return SendFragment{}, 0, fmt.Errorf("send fragment count %d out of range", fragmentCount)
	}
	if fragmentNumber >= fragmentCount {
		return SendFragment{}, 0, fmt.Errorf("send fragment number %d >= count %d", fragmentNumber, fragmentCount)
	}

	return SendFragment{
		Header:              header,
		StartSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		FragmentCount:       fragmentCount,
		FragmentNumber:      fragmentNumber,
		TotalLength:         binary.BigEndian.Uint32(src[16:20]),
		FragmentOffset:      binary.BigEndian.Uint32(src[20:24]),
		Data:                append([]byte(nil), src[24:24+dataLength]...),
	}, sendFragmentCommandSize + dataLength, nil
}

func parseBandwidthLimit(header CommandHeader, src []byte) (BandwidthLimit, int, error) {
	if len(src) < bandwidthLimitCommandSize {
		return BandwidthLimit{}, 0, fmt.Errorf("bandwidth limit command too short: got %d bytes", len(src))
	}

	return BandwidthLimit{
		Header:            header,
		IncomingBandwidth: binary.BigEndian.Uint32(src[4:8]),
		OutgoingBandwidth: binary.BigEndian.Uint32(src[8:12]),
	}, bandwidthLimitCommandSize, nil
}

func parseThrottleConfigure(header CommandHeader, src []byte) (ThrottleConfigure, int, error) {
	if len(src) < throttleConfigureCommandSize {
		return ThrottleConfigure{}, 0, fmt.Errorf("throttle configure command too short: got %d bytes", len(src))
	}

	return ThrottleConfigure{
		Header:                     header,
		PacketThrottleInterval:     binary.BigEndian.Uint32(src[4:8]),
		PacketThrottleAcceleration: binary.BigEndian.Uint32(src[8:12]),
		PacketThrottleDeceleration: binary.BigEndian.Uint32(src[12:16]),
	}, throttleConfigureCommandSize, nil
}
