package core

import (
	"net/netip"
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
	if got, want := b.String(), "[fe80::1]:1234%7"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestDefaultConfigPeerCount(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PeerCount == 0 {
		t.Fatal("DefaultConfig PeerCount must be non-zero so clients can Connect")
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
