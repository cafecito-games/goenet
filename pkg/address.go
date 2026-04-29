package goenet

import (
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// Address identifies a remote endpoint and preserves ENet's IPv6 scope ID.
type Address struct {
	addrPort netip.AddrPort
	scopeID  uint32
}

// NewAddress constructs an address with a single source of truth for IPv6 scope.
func NewAddress(addrPort netip.AddrPort, scopeID uint32) (Address, error) {
	coreAddress, err := core.NewAddress(addrPort, scopeID)
	if err != nil {
		return Address{}, err
	}

	return fromCoreAddress(coreAddress), nil
}

// AddrPort returns the address without any IPv6 zone metadata.
func (a Address) AddrPort() netip.AddrPort {
	return a.addrPort
}

// ScopeID returns the IPv6 scope identifier associated with the address.
func (a Address) ScopeID() uint32 {
	return a.scopeID
}

func fromCoreAddress(address core.Address) Address {
	return Address{
		addrPort: address.AddrPort(),
		scopeID:  address.ScopeID(),
	}
}
