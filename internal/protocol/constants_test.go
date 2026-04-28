package protocol_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestProtocolConstantsMatchENet(t *testing.T) {
	if protocol.MinimumMTU != 576 {
		t.Fatalf("MinimumMTU = %d", protocol.MinimumMTU)
	}
	if protocol.CommandSendReliable != 6 {
		t.Fatalf("CommandSendReliable = %d", protocol.CommandSendReliable)
	}
}
