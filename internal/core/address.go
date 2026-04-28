package core

import (
	"errors"
	"net/netip"
)

var errAddressHasZone = errors.New("goenet: address must not include IPv6 zone")

// Address identifies a remote endpoint and preserves ENet's IPv6 scope ID.
type Address struct {
	addrPort netip.AddrPort
	scopeID  uint32
}

// NewAddress constructs an address with a single source of truth for IPv6 scope.
func NewAddress(addrPort netip.AddrPort, scopeID uint32) (Address, error) {
	if addrPort.Addr().Zone() != "" {
		return Address{}, errAddressHasZone
	}

	return Address{
		addrPort: addrPort,
		scopeID:  scopeID,
	}, nil
}

// AddrPort returns the address without any IPv6 zone metadata.
func (a Address) AddrPort() netip.AddrPort {
	return a.addrPort
}

// ScopeID returns the IPv6 scope identifier associated with the address.
func (a Address) ScopeID() uint32 {
	return a.scopeID
}
