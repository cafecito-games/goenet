package core

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAddressRejectsZonedIPv6(t *testing.T) {
	addr := netip.MustParseAddr("fe80::1%eth0")
	addrPort := netip.AddrPortFrom(addr, 1234)
	if _, err := NewAddress(addrPort, 0); err == nil {
		t.Fatal("expected zoned IPv6 to be rejected")
	}
}

func TestAddressRoundTrip(t *testing.T) {
	addrPort := netip.MustParseAddrPort("[fe80::1]:1234")
	addr, err := NewAddress(addrPort, 5)
	if err != nil {
		t.Fatalf("NewAddress: %v", err)
	}
	if got := addr.AddrPort(); got != addrPort {
		t.Errorf("AddrPort: got %v want %v", got, addrPort)
	}
	if got := addr.ScopeID(); got != 5 {
		t.Errorf("ScopeID: got %d want 5", got)
	}
	if !addr.IsValid() {
		t.Error("IsValid: got false")
	}
}

func TestAddressEqual(t *testing.T) {
	a := mustAddress(t, "127.0.0.1:1", 0)
	b := mustAddress(t, "127.0.0.1:1", 0)
	c := mustAddress(t, "127.0.0.1:2", 0)
	d := mustAddress(t, "[fe80::1]:1", 1)
	e := mustAddress(t, "[fe80::1]:1", 2)

	tests := []struct {
		name  string
		left  Address
		right Address
		want  bool
	}{
		{name: "same address", left: a, right: b, want: true},
		{name: "different port", left: a, right: c, want: false},
		{name: "different scope", left: d, right: e, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.left.Equal(tt.right))
		})
	}
}

func TestAddressString(t *testing.T) {
	tests := []struct {
		name     string
		addrPort string
		scopeID  uint32
		want     string
	}{
		{name: "ipv4 without scope", addrPort: "127.0.0.1:1234", scopeID: 0, want: "127.0.0.1:1234"},
		{name: "ipv6 with numeric scope", addrPort: "[fe80::1]:1234", scopeID: 7, want: "[fe80::1%7]:1234"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := mustAddress(t, tt.addrPort, tt.scopeID)
			assert.Equal(t, tt.want, addr.String())
		})
	}
}

func TestDefaultConfigPeerCount(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PeerCount == 0 {
		t.Fatal("DefaultConfig PeerCount must be non-zero so clients can Connect")
	}
}

func TestAddressFromAddrPort(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		wantAddrPort    string
		wantScopeID     uint32
		wantIPv4        bool
		wantErrContains string
	}{
		{
			name:         "ipv4 strips zero scope",
			input:        "127.0.0.1:7777",
			wantAddrPort: "127.0.0.1:7777",
		},
		{
			name:         "preserves numeric ipv6 zone",
			input:        "[fe80::1%7]:7777",
			wantAddrPort: "[fe80::1]:7777",
			wantScopeID:  7,
		},
		{
			name:         "unmaps v4 in v6",
			input:        "[::ffff:127.0.0.1]:9000",
			wantAddrPort: "127.0.0.1:9000",
			wantIPv4:     true,
		},
		{
			name:            "rejects unknown interface zone",
			input:           "[fe80::1%this-iface-should-not-exist]:1234",
			wantErrContains: "this-iface-should-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AddressFromAddrPort(netip.MustParseAddrPort(tt.input))
			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAddrPort, got.AddrPort().String())
			assert.Equal(t, tt.wantScopeID, got.ScopeID())
			assert.Empty(t, got.AddrPort().Addr().Zone())
			if tt.wantIPv4 {
				assert.True(t, got.AddrPort().Addr().Is4())
			}
		})
	}
}

func TestAddressFromUDPAddrNilReturnsError(t *testing.T) {
	_, err := AddressFromUDPAddr(nil)
	if err == nil {
		t.Fatal("expected error for nil *net.UDPAddr")
	}
	if !strings.Contains(err.Error(), "nil UDP address") {
		t.Fatalf("error = %v", err)
	}
}

func TestAddressFromUDPAddrIPv4(t *testing.T) {
	got, err := AddressFromUDPAddr(&net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5555})
	if err != nil {
		t.Fatalf("AddressFromUDPAddr: %v", err)
	}
	if got.AddrPort().String() != "10.0.0.1:5555" {
		t.Fatalf("AddrPort = %q", got.AddrPort())
	}
	if got.ScopeID() != 0 {
		t.Fatalf("ScopeID = %d, want 0", got.ScopeID())
	}
}

func TestAddressFromUDPAddrIPv6WithNumericZone(t *testing.T) {
	got, err := AddressFromUDPAddr(&net.UDPAddr{
		IP:   net.ParseIP("fe80::1"),
		Port: 5555,
		Zone: "12",
	})
	if err != nil {
		t.Fatalf("AddressFromUDPAddr: %v", err)
	}
	if got.ScopeID() != 12 {
		t.Fatalf("ScopeID = %d, want 12", got.ScopeID())
	}
	if got.AddrPort().Addr().Zone() != "" {
		t.Fatalf("zone leaked into AddrPort: %q", got.AddrPort().Addr().Zone())
	}
}

func TestAddressFromUDPAddrPropagatesUnknownInterfaceError(t *testing.T) {
	_, err := AddressFromUDPAddr(&net.UDPAddr{
		IP:   net.ParseIP("fe80::1"),
		Port: 5555,
		Zone: "this-iface-should-not-exist",
	})
	if err == nil {
		t.Fatal("expected error for unknown interface zone")
	}
}

func TestUDPAddrFromAddressIPv4(t *testing.T) {
	addr := mustAddress(t, "10.0.0.1:5555", 0)
	udp := UDPAddrFromAddress(addr)
	if udp.IP.To4() == nil {
		t.Fatalf("IP = %v, want IPv4", udp.IP)
	}
	if udp.IP.String() != "10.0.0.1" {
		t.Fatalf("IP = %q", udp.IP)
	}
	if udp.Port != 5555 {
		t.Fatalf("Port = %d", udp.Port)
	}
	if udp.Zone != "" {
		t.Fatalf("Zone = %q, want empty for IPv4", udp.Zone)
	}
}

func TestUDPAddrFromAddressIPv6EmitsNumericZone(t *testing.T) {
	addr := mustAddress(t, "[fe80::1]:7777", 9)
	udp := UDPAddrFromAddress(addr)
	if udp.IP.String() != "fe80::1" {
		t.Fatalf("IP = %q", udp.IP)
	}
	if udp.Port != 7777 {
		t.Fatalf("Port = %d", udp.Port)
	}
	if udp.Zone != "9" {
		t.Fatalf("Zone = %q, want %q", udp.Zone, "9")
	}
}

func TestAddressUDPRoundTripPreservesScope(t *testing.T) {
	original := &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 1234, Zone: "5"}

	addr, err := AddressFromUDPAddr(original)
	if err != nil {
		t.Fatalf("AddressFromUDPAddr: %v", err)
	}

	roundTripped := UDPAddrFromAddress(addr)
	if roundTripped.IP.String() != original.IP.String() {
		t.Fatalf("round-trip IP = %q, want %q", roundTripped.IP, original.IP)
	}
	if roundTripped.Port != original.Port {
		t.Fatalf("round-trip Port = %d, want %d", roundTripped.Port, original.Port)
	}
	if roundTripped.Zone != original.Zone {
		t.Fatalf("round-trip Zone = %q, want %q", roundTripped.Zone, original.Zone)
	}
}

func TestNewAddressZonedFailureIsExportedSentinelEquivalent(t *testing.T) {
	// errAddressHasZone is unexported; assert its identity via the only path
	// that surfaces it so future refactors that swap the error don't quietly
	// regress the contract that NewAddress returns a single, comparable error.
	zoned := netip.AddrPortFrom(netip.MustParseAddr("fe80::1%eth0"), 1234)
	_, err := NewAddress(zoned, 0)
	if err == nil {
		t.Fatal("expected error for zoned input")
	}
	if !errors.Is(err, errAddressHasZone) {
		t.Fatalf("error = %v, want errAddressHasZone", err)
	}
}

func mustAddress(t *testing.T, addrPort string, scopeID uint32) Address {
	t.Helper()
	ap := netip.MustParseAddrPort(addrPort)
	addr, err := NewAddress(ap, scopeID)
	if err != nil {
		t.Fatalf("NewAddress: %v", err)
	}
	return addr
}
