// Package core holds shared internal transport-facing value types.
package core

import (
	"errors"
	"net/netip"
	"strconv"
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

// Equal reports whether two addresses refer to the same endpoint, including IPv6 scope.
func (a Address) Equal(other Address) bool {
	return a.addrPort == other.addrPort && a.scopeID == other.scopeID
}

// String renders the address as host:port with an optional %scope suffix for IPv6.
func (a Address) String() string {
	if a.scopeID == 0 {
		return a.addrPort.String()
	}

	addr := a.addrPort.Addr().WithZone(strconv.FormatUint(uint64(a.scopeID), 10))
	return netip.AddrPortFrom(addr, a.addrPort.Port()).String()
}

// IsValid reports whether the address has a usable underlying netip.AddrPort.
func (a Address) IsValid() bool {
	return a.addrPort.IsValid()
}
