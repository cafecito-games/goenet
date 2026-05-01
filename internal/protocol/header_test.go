package protocol_test

import (
	"bytes"
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := protocol.Header{
		PeerID:    7,
		SentTime:  1234,
		SessionID: 2,
		Flags:     protocol.HeaderFlagSentTime,
	}

	wire, err := h.AppendBinary(nil)
	if err != nil {
		t.Fatalf("AppendBinary: %v", err)
	}
	wantWire := []byte{0xa0, 0x07, 0x04, 0xd2}
	if !bytes.Equal(wire, wantWire) {
		t.Fatalf("marshal bytes = %x, want %x", wire, wantWire)
	}

	got, err := protocol.ParseHeader(wire)
	if err != nil {
		t.Fatal(err)
	}

	if got.PeerID != h.PeerID || got.SentTime != h.SentTime || got.SessionID != h.SessionID || got.Flags != h.Flags {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, h)
	}
}

func TestParseHeaderRejectsTruncatedSentTime(t *testing.T) {
	_, err := protocol.ParseHeader([]byte{0xa0, 0x07, 0x04})
	if err == nil {
		t.Fatal("ParseHeader succeeded on truncated sent-time header")
	}
}

func TestHeaderMinimalRoundTrip(t *testing.T) {
	h := protocol.Header{PeerID: 0x123, SessionID: 1}

	wire, err := h.AppendBinary(nil)
	if err != nil {
		t.Fatalf("AppendBinary: %v", err)
	}
	wantWire := []byte{0x11, 0x23}
	if !bytes.Equal(wire, wantWire) {
		t.Fatalf("marshal bytes = %x, want %x", wire, wantWire)
	}

	got, err := protocol.ParseHeader(wire)
	if err != nil {
		t.Fatal(err)
	}

	if got.PeerID != h.PeerID || got.SessionID != h.SessionID || got.Flags != 0 || got.SentTime != 0 {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, h)
	}
}
