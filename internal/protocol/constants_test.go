package protocol_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestProtocolConstantsMatchENet(t *testing.T) {
	tests := []struct {
		name string
		got  uint64
		want uint64
	}{
		{name: "MinimumMTU", got: uint64(protocol.MinimumMTU), want: 576},
		{name: "MaximumMTU", got: uint64(protocol.MaximumMTU), want: 4096},
		{name: "MaximumPacketCommands", got: uint64(protocol.MaximumPacketCommands), want: 32},
		{name: "CommandAcknowledge", got: uint64(protocol.CommandAcknowledge), want: 1},
		{name: "CommandConnect", got: uint64(protocol.CommandConnect), want: 2},
		{name: "CommandVerifyConnect", got: uint64(protocol.CommandVerifyConnect), want: 3},
		{name: "CommandDisconnect", got: uint64(protocol.CommandDisconnect), want: 4},
		{name: "CommandPing", got: uint64(protocol.CommandPing), want: 5},
		{name: "CommandSendReliable", got: uint64(protocol.CommandSendReliable), want: 6},
		{name: "CommandSendUnreliable", got: uint64(protocol.CommandSendUnreliable), want: 7},
		{name: "CommandSendFragment", got: uint64(protocol.CommandSendFragment), want: 8},
		{name: "CommandSendUnsequenced", got: uint64(protocol.CommandSendUnsequenced), want: 9},
		{name: "CommandBandwidthLimit", got: uint64(protocol.CommandBandwidthLimit), want: 10},
		{name: "CommandThrottleConfigure", got: uint64(protocol.CommandThrottleConfigure), want: 11},
		{name: "CommandSendUnreliableFragment", got: uint64(protocol.CommandSendUnreliableFragment), want: 12},
		{name: "CommandFlagUnsequenced", got: uint64(protocol.CommandFlagUnsequenced), want: 1 << 6},
		{name: "CommandFlagAcknowledge", got: uint64(protocol.CommandFlagAcknowledge), want: 1 << 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestHeaderSize(t *testing.T) {
	tests := []struct {
		name         string
		withSentTime bool
		want         int
	}{
		{name: "minimal", withSentTime: false, want: protocol.HeaderSizeMinimal},
		{name: "with sent time", withSentTime: true, want: protocol.HeaderSizeWithSentTime},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, protocol.HeaderSize(tt.withSentTime))
		})
	}
}
