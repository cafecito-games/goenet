package goenet_test

import (
	"net/netip"
	"reflect"
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestAddressScopeIDIsUint32(t *testing.T) {
	var addr goenet.Address

	if got := reflect.TypeOf(addr.ScopeID).Kind(); got != reflect.Uint32 {
		t.Fatalf("ScopeID kind = %v, want %v", got, reflect.Uint32)
	}
}

func TestAddressCarriesIPv6AddrPortAndScopeID(t *testing.T) {
	addr := goenet.Address{
		AddrPort: netip.MustParseAddrPort("[fe80::1]:7777"),
		ScopeID:  37,
	}

	if got := addr.AddrPort.String(); got != "[fe80::1]:7777" {
		t.Fatalf("AddrPort.String() = %q", got)
	}
	if addr.ScopeID != 37 {
		t.Fatalf("ScopeID = %d", addr.ScopeID)
	}
}
