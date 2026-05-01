package core

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
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
	if !a.Equal(b) {
		t.Error("a should equal b")
	}
	if a.Equal(c) {
		t.Error("a should not equal c (different port)")
	}
	if d.Equal(e) {
		t.Error("d should not equal e (different scope)")
	}
}

func TestAddressString(t *testing.T) {
	a := mustAddress(t, "127.0.0.1:1234", 0)
	if got, want := a.String(), "127.0.0.1:1234"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	b := mustAddress(t, "[fe80::1]:1234", 7)
	if got, want := b.String(), "[fe80::1%7]:1234"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestDefaultConfigPeerCount(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PeerCount == 0 {
		t.Fatal("DefaultConfig PeerCount must be non-zero so clients can Connect")
	}
}

func TestAddressFromAddrPortIPv4StripsZeroScope(t *testing.T) {
	got, err := AddressFromAddrPort(netip.MustParseAddrPort("127.0.0.1:7777"))
	if err != nil {
		t.Fatalf("AddressFromAddrPort: %v", err)
	}
	if got.AddrPort().String() != "127.0.0.1:7777" {
		t.Fatalf("AddrPort = %q", got.AddrPort())
	}
	if got.ScopeID() != 0 {
		t.Fatalf("ScopeID = %d, want 0", got.ScopeID())
	}
}

func TestAddressFromAddrPortPreservesNumericIPv6Zone(t *testing.T) {
	got, err := AddressFromAddrPort(netip.MustParseAddrPort("[fe80::1%7]:7777"))
	if err != nil {
		t.Fatalf("AddressFromAddrPort: %v", err)
	}
	if got.ScopeID() != 7 {
		t.Fatalf("ScopeID = %d, want 7", got.ScopeID())
	}
	// AddrPort returned by Address must be unzoned — the scope lives in the
	// dedicated ScopeID field so equality and lookups don't carry the zone
	// twice.
	if got.AddrPort().Addr().Zone() != "" {
		t.Fatalf("zone leaked into AddrPort: %q", got.AddrPort().Addr().Zone())
	}
}

func TestAddressFromAddrPortUnmapsV4InV6(t *testing.T) {
	// Go's resolver sometimes returns IPv4 addresses as ::ffff:0:0/96-mapped
	// v6. AddressFromAddrPort must collapse them back to v4 so equality with
	// a peer's natural v4 form works.
	got, err := AddressFromAddrPort(netip.MustParseAddrPort("[::ffff:127.0.0.1]:9000"))
	if err != nil {
		t.Fatalf("AddressFromAddrPort: %v", err)
	}
	if !got.AddrPort().Addr().Is4() {
		t.Fatalf("expected v4 address after unmap, got %v", got.AddrPort().Addr())
	}
	if got.AddrPort().String() != "127.0.0.1:9000" {
		t.Fatalf("AddrPort = %q", got.AddrPort())
	}
}

func TestAddressFromAddrPortRejectsUnknownInterfaceZone(t *testing.T) {
	// Non-numeric zones are resolved against net.InterfaceByName. A definitely-
	// nonexistent name must surface as an error rather than silently zero-ing
	// the scope.
	_, err := AddressFromAddrPort(netip.MustParseAddrPort("[fe80::1%this-iface-should-not-exist]:1234"))
	if err == nil {
		t.Fatal("expected error for unknown interface zone")
	}
	if !strings.Contains(err.Error(), "this-iface-should-not-exist") {
		t.Fatalf("error should name the bad zone: %v", err)
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
