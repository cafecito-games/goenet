package protocol_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestProtocolConstantsMatchENet(t *testing.T) {
	if protocol.MinimumMTU != 576 {
		t.Fatalf("MinimumMTU = %d", protocol.MinimumMTU)
	}
	if protocol.MaximumMTU != 4096 {
		t.Fatalf("MaximumMTU = %d", protocol.MaximumMTU)
	}
	if protocol.MaximumPacketCommands != 32 {
		t.Fatalf("MaximumPacketCommands = %d", protocol.MaximumPacketCommands)
	}
	if protocol.CommandAcknowledge != 1 {
		t.Fatalf("CommandAcknowledge = %d", protocol.CommandAcknowledge)
	}
	if protocol.CommandConnect != 2 {
		t.Fatalf("CommandConnect = %d", protocol.CommandConnect)
	}
	if protocol.CommandVerifyConnect != 3 {
		t.Fatalf("CommandVerifyConnect = %d", protocol.CommandVerifyConnect)
	}
	if protocol.CommandDisconnect != 4 {
		t.Fatalf("CommandDisconnect = %d", protocol.CommandDisconnect)
	}
	if protocol.CommandPing != 5 {
		t.Fatalf("CommandPing = %d", protocol.CommandPing)
	}
	if protocol.CommandSendReliable != 6 {
		t.Fatalf("CommandSendReliable = %d", protocol.CommandSendReliable)
	}
}
