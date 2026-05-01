// Package core holds shared internal transport-facing value types.
package core

import (
	"errors"
	"fmt"
	"net"
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

// AddressFromAddrPort converts a possibly-zoned AddrPort into the internal address form.
func AddressFromAddrPort(addr netip.AddrPort) (Address, error) {
	scopeID, err := scopeIDFromZone(addr.Addr().Zone())
	if err != nil {
		return Address{}, err
	}

	unzoned := netip.AddrPortFrom(addr.Addr().WithZone("").Unmap(), addr.Port())
	return NewAddress(unzoned, scopeID)
}

// AddressFromUDPAddr converts a UDPAddr into the internal address form.
func AddressFromUDPAddr(addr *net.UDPAddr) (Address, error) {
	if addr == nil {
		return Address{}, fmt.Errorf("goenet: nil UDP address")
	}

	scopeID, err := scopeIDFromZone(addr.Zone)
	if err != nil {
		return Address{}, err
	}

	addrPort := addr.AddrPort()
	unzoned := netip.AddrPortFrom(addrPort.Addr().WithZone("").Unmap(), addrPort.Port())
	return NewAddress(unzoned, scopeID)
}

// UDPAddrFromAddress converts an internal address into a UDPAddr suitable for socket I/O.
func UDPAddrFromAddress(addr Address) *net.UDPAddr {
	addrPort := addr.AddrPort()
	return &net.UDPAddr{
		IP:   net.IP(addrPort.Addr().AsSlice()),
		Port: int(addrPort.Port()),
		Zone: zoneFromScopeID(addr.ScopeID()),
	}
}

func scopeIDFromZone(zone string) (uint32, error) {
	if zone == "" {
		return 0, nil
	}
	if id, err := strconv.ParseUint(zone, 10, 32); err == nil {
		return uint32(id), nil
	}

	iface, err := net.InterfaceByName(zone)
	if err != nil {
		return 0, fmt.Errorf("goenet: resolve interface zone %q: %w", zone, err)
	}
	if iface.Index < 0 {
		return 0, fmt.Errorf("goenet: interface %q has negative index %d", zone, iface.Index)
	}
	id, err := strconv.ParseUint(strconv.Itoa(iface.Index), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("goenet: parse interface %q index %d: %w", zone, iface.Index, err)
	}
	return uint32(id), nil
}

func zoneFromScopeID(scopeID uint32) string {
	if scopeID == 0 {
		return ""
	}

	// Go's resolver accepts numeric IPv6 zones directly, so we can skip the
	// per-call net.InterfaceByIndex syscall that would otherwise fire on every
	// outbound write to a scoped peer.
	return strconv.FormatUint(uint64(scopeID), 10)
}
