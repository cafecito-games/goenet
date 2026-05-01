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

// AppendBinary appends the ENet wire encoding of the header to dst, matching
// the encoding.BinaryAppender convention. The header has no failure modes,
// so the returned error is always nil; the signature exists for consistency
// with command-payload appenders that may surface size-bound errors.
func (h Header) AppendBinary(dst []byte) ([]byte, error) {
	flagsAndPeerID := h.PeerID & MaximumPeerID
	flagsAndPeerID |= uint16(h.Flags & HeaderFlagMask)
	flagsAndPeerID |= (uint16(h.SessionID) << HeaderSessionShift) & HeaderSessionMask

	dst, start := appendLen(dst, h.WireSize())
	binary.BigEndian.PutUint16(dst[start:start+HeaderSizeMinimal], flagsAndPeerID)

	if h.Flags&HeaderFlagSentTime == 0 {
		return dst, nil
	}

	binary.BigEndian.PutUint16(dst[start+HeaderSizeMinimal:start+HeaderSizeWithSentTime], h.SentTime)
	return dst, nil
}

// WireSize reports the encoded size of the packet header.
func (h Header) WireSize() int {
	if h.Flags&HeaderFlagSentTime != 0 {
		return HeaderSizeWithSentTime
	}
	return HeaderSizeMinimal
}

// ParseHeader decodes one ENet packet header from src.
func ParseHeader(src []byte) (Header, error) {
	if len(src) < HeaderSizeMinimal {
		return Header{}, fmt.Errorf("protocol header too short: got %d bytes", len(src))
	}

	flagsAndPeerID := binary.BigEndian.Uint16(src[:HeaderSizeMinimal])
	h := Header{
		PeerID:    flagsAndPeerID & MaximumPeerID,
		SessionID: uint8((flagsAndPeerID & HeaderSessionMask) >> HeaderSessionShift),
		Flags:     HeaderFlag(flagsAndPeerID) & HeaderFlagMask,
	}

	if h.Flags&HeaderFlagSentTime == 0 {
		return h, nil
	}

	if len(src) < HeaderSizeWithSentTime {
		return Header{}, fmt.Errorf("protocol header missing sent time: got %d bytes", len(src))
	}

	h.SentTime = binary.BigEndian.Uint16(src[HeaderSizeMinimal:HeaderSizeWithSentTime])
	return h, nil
}
