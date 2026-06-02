package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerStateString(t *testing.T) {
	tests := []struct {
		name  string
		state PeerState
		want  string
	}{
		{name: "disconnected", state: PeerStateDisconnected, want: "disconnected"},
		{name: "connecting", state: PeerStateConnecting, want: "connecting"},
		{name: "acknowledging connect", state: PeerStateAcknowledgingConnect, want: "acknowledging_connect"},
		{name: "connection pending", state: PeerStateConnectionPending, want: "connection_pending"},
		{name: "connection succeeded", state: PeerStateConnectionSucceeded, want: "connection_succeeded"},
		{name: "connected", state: PeerStateConnected, want: "connected"},
		{name: "disconnect later", state: PeerStateDisconnectLater, want: "disconnect_later"},
		{name: "disconnecting", state: PeerStateDisconnecting, want: "disconnecting"},
		{name: "acknowledging disconnect", state: PeerStateAcknowledgingDisconnect, want: "acknowledging_disconnect"},
		{name: "zombie", state: PeerStateZombie, want: "zombie"},
		{name: "unknown", state: PeerState(0xFF), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.state.String())
		})
	}
}
