package protocol

import (
	"encoding/binary"
	"fmt"
)

// Header is the ENet packet header carried ahead of the command stream.
type Header struct {
	PeerID    uint16
	SentTime  uint16
	SessionID uint8
	Flags     HeaderFlag
}

// MarshalBinary appends the ENet wire encoding of the header to dst.
func (h Header) MarshalBinary(dst []byte) []byte {
	flagsAndPeerID := h.PeerID & MaximumPeerID
	flagsAndPeerID |= uint16(h.Flags & HeaderFlagMask)
	flagsAndPeerID |= (uint16(h.SessionID) << HeaderSessionShift) & HeaderSessionMask

	dst, start := appendLen(dst, h.WireSize())
	binary.BigEndian.PutUint16(dst[start:start+headerMinimalSize], flagsAndPeerID)

	if h.Flags&HeaderFlagSentTime == 0 {
		return dst
	}

	binary.BigEndian.PutUint16(dst[start+headerMinimalSize:start+headerSize], h.SentTime)
	return dst
}

// WireSize reports the encoded size of the packet header.
func (h Header) WireSize() int {
	if h.Flags&HeaderFlagSentTime != 0 {
		return headerSize
	}
	return headerMinimalSize
}

// ParseHeader decodes one ENet packet header from src.
func ParseHeader(src []byte) (Header, error) {
	if len(src) < headerMinimalSize {
		return Header{}, fmt.Errorf("protocol header too short: got %d bytes", len(src))
	}

	flagsAndPeerID := binary.BigEndian.Uint16(src[:headerMinimalSize])
	h := Header{
		PeerID:    flagsAndPeerID & MaximumPeerID,
		SessionID: uint8((flagsAndPeerID & HeaderSessionMask) >> HeaderSessionShift),
		Flags:     HeaderFlag(flagsAndPeerID) & HeaderFlagMask,
	}

	if h.Flags&HeaderFlagSentTime == 0 {
		return h, nil
	}

	if len(src) < headerSize {
		return Header{}, fmt.Errorf("protocol header missing sent time: got %d bytes", len(src))
	}

	h.SentTime = binary.BigEndian.Uint16(src[headerMinimalSize:headerSize])
	return h, nil
}
