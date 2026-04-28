package protocol_test

import (
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

	wire := h.MarshalBinary(nil)
	got, err := protocol.ParseHeader(wire)
	if err != nil {
		t.Fatal(err)
	}

	if got.PeerID != h.PeerID || got.SentTime != h.SentTime || got.SessionID != h.SessionID || got.Flags != h.Flags {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, h)
	}
}
