package goenet

import (
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// Address identifies a remote endpoint and preserves ENet's IPv6 scope ID.
type Address = core.Address

// NewAddress constructs an address with a single source of truth for IPv6 scope.
func NewAddress(addrPort netip.AddrPort, scopeID uint32) (Address, error) {
	return core.NewAddress(addrPort, scopeID)
}
