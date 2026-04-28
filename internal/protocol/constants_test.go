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
	if protocol.CommandSendUnreliable != 7 {
		t.Fatalf("CommandSendUnreliable = %d", protocol.CommandSendUnreliable)
	}
	if protocol.CommandSendFragment != 8 {
		t.Fatalf("CommandSendFragment = %d", protocol.CommandSendFragment)
	}
	if protocol.CommandSendUnsequenced != 9 {
		t.Fatalf("CommandSendUnsequenced = %d", protocol.CommandSendUnsequenced)
	}
	if protocol.CommandBandwidthLimit != 10 {
		t.Fatalf("CommandBandwidthLimit = %d", protocol.CommandBandwidthLimit)
	}
	if protocol.CommandThrottleConfigure != 11 {
		t.Fatalf("CommandThrottleConfigure = %d", protocol.CommandThrottleConfigure)
	}
	if protocol.CommandSendUnreliableFragment != 12 {
		t.Fatalf("CommandSendUnreliableFragment = %d", protocol.CommandSendUnreliableFragment)
	}
	if protocol.CommandFlagUnsequenced != 1<<6 {
		t.Fatalf("CommandFlagUnsequenced = 0x%02x", protocol.CommandFlagUnsequenced)
	}
	if protocol.CommandFlagAcknowledge != 1<<7 {
		t.Fatalf("CommandFlagAcknowledge = 0x%02x", protocol.CommandFlagAcknowledge)
	}
}
