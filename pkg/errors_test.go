package goenet

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cafecito-games/goenet/internal/engine"
)

// TestSendErrorsAreMatchableViaErrorsIs verifies that each re-exported sentinel
// shares identity with the internal engine error it aliases, so a wrapped error
// returned from Send/Broadcast/Flush unwraps to it via errors.Is. Without the
// shared identity, callers would be forced to match on the error string.
func TestSendErrorsAreMatchableViaErrorsIs(t *testing.T) {
	cases := []struct {
		name   string
		public error
		engine error
	}{
		{"ErrPeerNotConnected", ErrPeerNotConnected, engine.ErrPeerNotConnected},
		{"ErrNilPacket", ErrNilPacket, engine.ErrNilPacket},
		{"ErrChannelOutOfRange", ErrChannelOutOfRange, engine.ErrChannelOutOfRange},
		{"ErrPacketTooLarge", ErrPacketTooLarge, engine.ErrPacketTooLarge},
		{"ErrFragmentationLimit", ErrFragmentationLimit, engine.ErrFragmentationLimit},
		{"ErrNoFragmentation", ErrNoFragmentation, engine.ErrNoFragmentation},
		{"ErrReliableSequenceExhausted", ErrReliableSequenceExhausted, engine.ErrReliableSequenceExhausted},
		{"ErrShortWrite", ErrShortWrite, engine.ErrShortWrite},
		{"ErrCommandExceedsMTU", ErrCommandExceedsMTU, engine.ErrCommandExceedsMTU},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.public, tc.engine) {
				t.Fatalf("public sentinel is not identical to engine sentinel %v", tc.engine)
			}
			// Mirror how the engine wraps the sentinel with extra context.
			wrapped := fmt.Errorf("send: %w: extra context", tc.engine)
			if !errors.Is(wrapped, tc.public) {
				t.Fatalf("errors.Is(wrapped, goenet.%s) = false, want true", tc.name)
			}
		})
	}
}
