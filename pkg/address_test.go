package goenet_test

import (
	"net/netip"
	"reflect"
	"testing"

	goenet "github.com/cafecito-games/goenet/pkg"
	"github.com/stretchr/testify/assert"
)

func TestNewAddressPreservesIPv6AddrPortAndScopeID(t *testing.T) {
	addr, err := goenet.NewAddress(netip.MustParseAddrPort("[fe80::1]:7777"), 1<<20)
	if err != nil {
		t.Fatalf("NewAddress() error = %v", err)
	}

	if got := addr.AddrPort().String(); got != "[fe80::1]:7777" {
		t.Fatalf("AddrPort.String() = %q", got)
	}
	if got := addr.ScopeID(); got != 1<<20 {
		t.Fatalf("ScopeID() = %d", got)
	}
}

func TestNewAddressRejectsZonedAddrPort(t *testing.T) {
	_, err := goenet.NewAddress(netip.MustParseAddrPort("[fe80::1%eth0]:7777"), 37)
	if err == nil {
		t.Fatal("NewAddress() error = nil, want error")
	}
}

func TestEventTypeValuesMatchENet(t *testing.T) {
	tests := []struct {
		name string
		got  goenet.EventType
		want uint8
	}{
		{name: "EventNone", got: goenet.EventNone, want: 0},
		{name: "EventConnect", got: goenet.EventConnect, want: 1},
		{name: "EventDisconnect", got: goenet.EventDisconnect, want: 2},
		{name: "EventReceive", got: goenet.EventReceive, want: 3},
		{name: "EventDisconnectTimeout", got: goenet.EventDisconnectTimeout, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, uint8(tt.got))
		})
	}
}

func TestPacketFlagValuesMatchENet(t *testing.T) {
	tests := []struct {
		name string
		got  goenet.PacketFlag
		want uint32
	}{
		{name: "PacketFlagReliable", got: goenet.PacketFlagReliable, want: 1},
		{name: "PacketFlagUnsequenced", got: goenet.PacketFlagUnsequenced, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, uint32(tt.got))
		})
	}
}

func TestPublicPackageStillExposesConfigAndPacketTypes(t *testing.T) {
	var cfg goenet.Config
	packet := goenet.Packet{Data: []byte("x")}

	if cfg.MTU == 0 {
		cfg = goenet.DefaultConfig()
	}
	if len(packet.Data) != 1 {
		t.Fatalf("packet length = %d, want 1", len(packet.Data))
	}
	if cfg.MTU == 0 {
		t.Fatal("default config should set MTU")
	}
}

// TestPublicTypesAreReachableThroughGoenetPackage verifies that the public
// names a goenet user imports actually resolve to non-nil types. The package
// path on the underlying type may live in internal/core (since most public
// names are type aliases collapsing the prior pkg/core duplication), so the
// older "must be owned by pkg" assertion has been retired along with the
// duplicate types it guarded.
func TestPublicTypesAreReachableThroughGoenetPackage(t *testing.T) {
	t.Parallel()

	var (
		address         goenet.Address
		packet          goenet.Packet
		eventType       goenet.EventType
		packetFlag      goenet.PacketFlag
		peerState       goenet.PeerState
		interceptResult goenet.InterceptResult
	)

	values := []struct {
		name string
		typ  reflect.Type
	}{
		{name: "Address", typ: reflect.TypeOf(address)},
		{name: "Packet", typ: reflect.TypeOf(packet)},
		{name: "EventType", typ: reflect.TypeOf(eventType)},
		{name: "PacketFlag", typ: reflect.TypeOf(packetFlag)},
		{name: "PeerState", typ: reflect.TypeOf(peerState)},
		{name: "InterceptResult", typ: reflect.TypeOf(interceptResult)},
		{name: "Checksummer", typ: reflect.TypeOf((*goenet.Checksummer)(nil)).Elem()},
		{name: "Compressor", typ: reflect.TypeOf((*goenet.Compressor)(nil)).Elem()},
	}

	for _, tc := range values {
		if tc.typ == nil {
			t.Fatalf("%s reflect.Type is nil — public name no longer resolves", tc.name)
		}
	}
}
