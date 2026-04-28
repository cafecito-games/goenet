package goenet_test

import (
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestAddressCarriesAddrPortAndScopeID(t *testing.T) {
	addr := goenet.Address{
		AddrPort: netip.MustParseAddrPort("127.0.0.1:7777"),
		ScopeID:  0,
	}

	if got := addr.AddrPort.String(); got != "127.0.0.1:7777" {
		t.Fatalf("AddrPort.String() = %q", got)
	}
	if addr.ScopeID != 0 {
		t.Fatalf("ScopeID = %d", addr.ScopeID)
	}
}
