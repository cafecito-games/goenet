// Package protocol implements ENet packet headers and command wire formats.
package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

// PacketCommand is an ENet protocol command payload.
type PacketCommand interface {
	MarshalBinary(dst []byte) []byte
}

// HeaderedCommand is implemented by every command payload that carries an
// ENet command header. The engine uses this to populate the Command/Channel/
// Flags/ReliableSequenceNumber fields after sequence-number assignment,
// without a per-type switch — adding a new command type that satisfies the
// interface keeps the engine routing automatically correct.
type HeaderedCommand interface {
	PacketCommand
	SetHeader(CommandHeader)
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
	dst, start := appendLen(dst, CommandHeaderSize)
	dst[start] = byte(h.Command) | byte(h.Flags)
	dst[start+1] = h.ChannelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], h.ReliableSequenceNumber)
	return dst
}

// WireSize reports the encoded size of the command header.
func (h CommandHeader) WireSize() int { return CommandHeaderSize }

// ParseCommandHeader decodes one ENet command header from src.
func ParseCommandHeader(src []byte) (CommandHeader, error) {
	if len(src) < CommandHeaderSize {
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
	dst, _ = appendLen(dst, AcknowledgeCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint16(dst[start+4:start+6], a.ReceivedReliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], a.ReceivedSentTime)
	return dst
}

// WireSize reports the encoded size of the acknowledge command.
func (a Acknowledge) WireSize() int { return AcknowledgeCommandSize }

// SetHeader replaces the Acknowledge command header. See HeaderedCommand.
func (a *Acknowledge) SetHeader(h CommandHeader) { a.Header = h }

// MarshalBinary appends the ENet wire encoding of the connect command to dst.
func (c Connect) MarshalBinary(dst []byte) []byte {
	header := c.Header
	header.Command = CommandConnect
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, ConnectCommandSize-CommandHeaderSize)
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

// WireSize reports the encoded size of the connect command.
func (c Connect) WireSize() int { return ConnectCommandSize }

// SetHeader replaces the Connect command header. See HeaderedCommand.
func (c *Connect) SetHeader(h CommandHeader) { c.Header = h }

// MarshalBinary appends the ENet wire encoding of the verify-connect command to dst.
func (c VerifyConnect) MarshalBinary(dst []byte) []byte {
	header := c.Header
	header.Command = CommandVerifyConnect
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, VerifyConnectCommandSize-CommandHeaderSize)
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

// WireSize reports the encoded size of the verify-connect command.
func (c VerifyConnect) WireSize() int { return VerifyConnectCommandSize }

// SetHeader replaces the VerifyConnect command header. See HeaderedCommand.
func (c *VerifyConnect) SetHeader(h CommandHeader) { c.Header = h }

// MarshalBinary appends the ENet wire encoding of the disconnect command to dst.
func (d Disconnect) MarshalBinary(dst []byte) []byte {
	header := d.Header
	header.Command = CommandDisconnect

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, DisconnectCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint32(dst[start+4:start+8], d.Data)
	return dst
}

// WireSize reports the encoded size of the disconnect command.
func (d Disconnect) WireSize() int { return DisconnectCommandSize }

// SetHeader replaces the Disconnect command header. See HeaderedCommand.
func (d *Disconnect) SetHeader(h CommandHeader) { d.Header = h }

// MarshalBinary appends the ENet wire encoding of the ping command to dst.
func (p Ping) MarshalBinary(dst []byte) []byte {
	header := p.Header
	header.Command = CommandPing
	header.Flags |= CommandFlagAcknowledge
	return header.MarshalBinary(dst)
}

// WireSize reports the encoded size of the ping command.
func (p Ping) WireSize() int { return CommandHeaderSize }

// SetHeader replaces the Ping command header. See HeaderedCommand.
func (p *Ping) SetHeader(h CommandHeader) { p.Header = h }

// MarshalBinary appends the ENet wire encoding of the reliable payload command to dst.
func (s SendReliable) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendReliable
	header.Flags |= CommandFlagAcknowledge

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, SendReliableCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint16(dst[start+4:start+6], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// WireSize reports the encoded size of the reliable payload command.
func (s SendReliable) WireSize() int { return SendReliableCommandSize + len(s.Data) }

// SetHeader replaces the SendReliable command header. See HeaderedCommand.
func (s *SendReliable) SetHeader(h CommandHeader) { s.Header = h }

// MarshalBinary appends the ENet wire encoding of the unreliable payload command to dst.
func (s SendUnreliable) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendUnreliable

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, SendUnreliableCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.UnreliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// WireSize reports the encoded size of the unreliable payload command.
func (s SendUnreliable) WireSize() int { return SendUnreliableCommandSize + len(s.Data) }

// SetHeader replaces the SendUnreliable command header. See HeaderedCommand.
func (s *SendUnreliable) SetHeader(h CommandHeader) { s.Header = h }

// MarshalBinary appends the ENet wire encoding of the unsequenced payload command to dst.
func (s SendUnsequenced) MarshalBinary(dst []byte) []byte {
	header := s.Header
	header.Command = CommandSendUnsequenced
	header.Flags |= CommandFlagUnsequenced

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, SendUnsequencedCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.UnsequencedGroup)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	dst = append(dst, s.Data...)
	return dst
}

// WireSize reports the encoded size of the unsequenced payload command.
func (s SendUnsequenced) WireSize() int { return SendUnsequencedCommandSize + len(s.Data) }

// SetHeader replaces the SendUnsequenced command header. See HeaderedCommand.
func (s *SendUnsequenced) SetHeader(h CommandHeader) { s.Header = h }

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
	dst, _ = appendLen(dst, SendFragmentCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint16(dst[start+4:start+6], s.StartSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], payloadSize16(len(s.Data)))
	binary.BigEndian.PutUint32(dst[start+8:start+12], s.FragmentCount)
	binary.BigEndian.PutUint32(dst[start+12:start+16], s.FragmentNumber)
	binary.BigEndian.PutUint32(dst[start+16:start+20], s.TotalLength)
	binary.BigEndian.PutUint32(dst[start+20:start+24], s.FragmentOffset)
	dst = append(dst, s.Data...)
	return dst
}

// WireSize reports the encoded size of the fragment command.
func (s SendFragment) WireSize() int { return SendFragmentCommandSize + len(s.Data) }

// SetHeader replaces the SendFragment command header. See HeaderedCommand.
func (s *SendFragment) SetHeader(h CommandHeader) { s.Header = h }

// MarshalBinary appends the ENet wire encoding of the bandwidth-limit command to dst.
func (b BandwidthLimit) MarshalBinary(dst []byte) []byte {
	header := b.Header
	header.Command = CommandBandwidthLimit

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, BandwidthLimitCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint32(dst[start+4:start+8], b.IncomingBandwidth)
	binary.BigEndian.PutUint32(dst[start+8:start+12], b.OutgoingBandwidth)
	return dst
}

// WireSize reports the encoded size of the bandwidth-limit command.
func (b BandwidthLimit) WireSize() int { return BandwidthLimitCommandSize }

// SetHeader replaces the BandwidthLimit command header. See HeaderedCommand.
func (b *BandwidthLimit) SetHeader(h CommandHeader) { b.Header = h }

// MarshalBinary appends the ENet wire encoding of the throttle-configure command to dst.
func (t ThrottleConfigure) MarshalBinary(dst []byte) []byte {
	header := t.Header
	header.Command = CommandThrottleConfigure

	start := len(dst)
	dst = header.MarshalBinary(dst)
	dst, _ = appendLen(dst, ThrottleConfigureCommandSize-CommandHeaderSize)
	binary.BigEndian.PutUint32(dst[start+4:start+8], t.PacketThrottleInterval)
	binary.BigEndian.PutUint32(dst[start+8:start+12], t.PacketThrottleAcceleration)
	binary.BigEndian.PutUint32(dst[start+12:start+16], t.PacketThrottleDeceleration)
	return dst
}

// WireSize reports the encoded size of the throttle-configure command.
func (t ThrottleConfigure) WireSize() int { return ThrottleConfigureCommandSize }

// SetHeader replaces the ThrottleConfigure command header. See HeaderedCommand.
func (t *ThrottleConfigure) SetHeader(h CommandHeader) { t.Header = h }

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
//
// The engine's outbound path enforces this bound before reaching MarshalBinary:
//   - Send paths run validatePacketSize, which caps the per-command body at
//     min(peerMTU, configured MTU). MaximumMTU is 4096, well below 2^16.
//   - Both reliable and unreliable fragmentation split on maxReliableFragmentDataLength
//     and maxUnreliableFragmentDataLength, both bounded by MTU - overhead.
//   - Inbound parsers reject lengths that exceed MaximumPacketSize; they do not
//     drive marshal.
//
// An overflow here therefore signals a programmer error in a caller that has
// either bypassed validatePacketSize or constructed a SendFragment/SendReliable
// with a hand-rolled oversized data slice. Crashing loudly is preferable to
// silently truncating wire bytes that the receiver cannot reconstruct.
func payloadSize16(n int) uint16 {
	if n < 0 || n > math.MaxUint16 {
		panic(fmt.Sprintf("protocol: payload too large for 16-bit length: %d", n))
	}

	return uint16(n)
}

func appendLen(dst []byte, n int) (buf []byte, start int) {
	start = len(dst)
	dst = slices.Grow(dst, n)
	dst = dst[:start+n]
	return dst, start
}

func parseAcknowledge(header CommandHeader, src []byte) (Acknowledge, int, error) {
	if len(src) < AcknowledgeCommandSize {
		return Acknowledge{}, 0, fmt.Errorf("acknowledge command too short: got %d bytes", len(src))
	}

	return Acknowledge{
		Header:                         header,
		ReceivedReliableSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		ReceivedSentTime:               binary.BigEndian.Uint16(src[6:8]),
	}, AcknowledgeCommandSize, nil
}

func parseConnect(header CommandHeader, src []byte) (Connect, int, error) {
	if len(src) < ConnectCommandSize {
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
	}, ConnectCommandSize, nil
}

func parseVerifyConnect(header CommandHeader, src []byte) (VerifyConnect, int, error) {
	if len(src) < VerifyConnectCommandSize {
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
	}, VerifyConnectCommandSize, nil
}

func parseDisconnect(header CommandHeader, src []byte) (Disconnect, int, error) {
	if len(src) < DisconnectCommandSize {
		return Disconnect{}, 0, fmt.Errorf("disconnect command too short: got %d bytes", len(src))
	}

	return Disconnect{
		Header: header,
		Data:   binary.BigEndian.Uint32(src[4:8]),
	}, DisconnectCommandSize, nil
}

func parsePing(header CommandHeader, src []byte) (Ping, int, error) {
	if len(src) < PingCommandSize {
		return Ping{}, 0, fmt.Errorf("ping command too short: got %d bytes", len(src))
	}

	return Ping{Header: header}, PingCommandSize, nil
}

func parseSendReliable(header CommandHeader, src []byte) (SendReliable, int, error) {
	if len(src) < SendReliableCommandSize {
		return SendReliable{}, 0, fmt.Errorf("send reliable command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[4:6]))
	if len(src) < SendReliableCommandSize+dataLength {
		return SendReliable{}, 0, fmt.Errorf("send reliable payload too short: got %d bytes", len(src))
	}

	return SendReliable{
		Header: header,
		Data:   append([]byte(nil), src[6:6+dataLength]...),
	}, SendReliableCommandSize + dataLength, nil
}

func parseSendUnreliable(header CommandHeader, src []byte) (SendUnreliable, int, error) {
	if len(src) < SendUnreliableCommandSize {
		return SendUnreliable{}, 0, fmt.Errorf("send unreliable command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < SendUnreliableCommandSize+dataLength {
		return SendUnreliable{}, 0, fmt.Errorf("send unreliable payload too short: got %d bytes", len(src))
	}

	return SendUnreliable{
		Header:                   header,
		UnreliableSequenceNumber: binary.BigEndian.Uint16(src[4:6]),
		Data:                     append([]byte(nil), src[8:8+dataLength]...),
	}, SendUnreliableCommandSize + dataLength, nil
}

func parseSendUnsequenced(header CommandHeader, src []byte) (SendUnsequenced, int, error) {
	if len(src) < SendUnsequencedCommandSize {
		return SendUnsequenced{}, 0, fmt.Errorf("send unsequenced command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < SendUnsequencedCommandSize+dataLength {
		return SendUnsequenced{}, 0, fmt.Errorf("send unsequenced payload too short: got %d bytes", len(src))
	}

	return SendUnsequenced{
		Header:           header,
		UnsequencedGroup: binary.BigEndian.Uint16(src[4:6]),
		Data:             append([]byte(nil), src[8:8+dataLength]...),
	}, SendUnsequencedCommandSize + dataLength, nil
}

func parseSendFragment(header CommandHeader, src []byte) (SendFragment, int, error) {
	if len(src) < SendFragmentCommandSize {
		return SendFragment{}, 0, fmt.Errorf("send fragment command too short: got %d bytes", len(src))
	}

	dataLength := int(binary.BigEndian.Uint16(src[6:8]))
	if len(src) < SendFragmentCommandSize+dataLength {
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
	}, SendFragmentCommandSize + dataLength, nil
}

func parseBandwidthLimit(header CommandHeader, src []byte) (BandwidthLimit, int, error) {
	if len(src) < BandwidthLimitCommandSize {
		return BandwidthLimit{}, 0, fmt.Errorf("bandwidth limit command too short: got %d bytes", len(src))
	}

	return BandwidthLimit{
		Header:            header,
		IncomingBandwidth: binary.BigEndian.Uint32(src[4:8]),
		OutgoingBandwidth: binary.BigEndian.Uint32(src[8:12]),
	}, BandwidthLimitCommandSize, nil
}

func parseThrottleConfigure(header CommandHeader, src []byte) (ThrottleConfigure, int, error) {
	if len(src) < ThrottleConfigureCommandSize {
		return ThrottleConfigure{}, 0, fmt.Errorf("throttle configure command too short: got %d bytes", len(src))
	}

	return ThrottleConfigure{
		Header:                     header,
		PacketThrottleInterval:     binary.BigEndian.Uint32(src[4:8]),
		PacketThrottleAcceleration: binary.BigEndian.Uint32(src[8:12]),
		PacketThrottleDeceleration: binary.BigEndian.Uint32(src[12:16]),
	}, ThrottleConfigureCommandSize, nil
}
