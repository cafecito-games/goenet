package goenet_test

import (
	"net/netip"
	"reflect"
	"testing"

	goenet "github.com/cafecito-games/goenet/pkg"
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
	if goenet.EventNone != 0 {
		t.Fatalf("EventNone = %d", goenet.EventNone)
	}
	if goenet.EventConnect != 1 {
		t.Fatalf("EventConnect = %d", goenet.EventConnect)
	}
	if goenet.EventDisconnect != 2 {
		t.Fatalf("EventDisconnect = %d", goenet.EventDisconnect)
	}
	if goenet.EventReceive != 3 {
		t.Fatalf("EventReceive = %d", goenet.EventReceive)
	}
	if goenet.EventDisconnectTimeout != 4 {
		t.Fatalf("EventDisconnectTimeout = %d", goenet.EventDisconnectTimeout)
	}
}

func TestPacketFlagValuesMatchENet(t *testing.T) {
	if goenet.PacketFlagReliable != 1 {
		t.Fatalf("PacketFlagReliable = %d", goenet.PacketFlagReliable)
	}
	if goenet.PacketFlagUnsequenced != 2 {
		t.Fatalf("PacketFlagUnsequenced = %d", goenet.PacketFlagUnsequenced)
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

func TestExportedTypesRemainOwnedByPublicPackage(t *testing.T) {
	t.Parallel()

	var (
		address         goenet.Address
		packet          goenet.Packet
		eventType       goenet.EventType
		packetFlag      goenet.PacketFlag
		peerState       goenet.PeerState
		interceptResult goenet.InterceptResult
	)

	tests := []struct {
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

	for _, tc := range tests {
		if got, want := tc.typ.PkgPath(), "github.com/cafecito-games/goenet/pkg"; got != want {
			t.Fatalf("%s package path = %q, want %q", tc.name, got, want)
		}
	}
}
