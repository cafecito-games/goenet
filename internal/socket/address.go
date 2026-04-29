package socket

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/cafecito-games/goenet/internal/core"
)

// AddressFromAddrPort converts a possibly-zoned AddrPort into the internal address form.
func AddressFromAddrPort(addr netip.AddrPort) (core.Address, error) {
	scopeID, err := scopeIDFromZone(addr.Addr().Zone())
	if err != nil {
		return core.Address{}, err
	}

	unzoned := netip.AddrPortFrom(addr.Addr().WithZone("").Unmap(), addr.Port())
	return core.NewAddress(unzoned, scopeID)
}

// AddressFromUDPAddr converts a UDPAddr into the internal address form.
func AddressFromUDPAddr(addr *net.UDPAddr) (core.Address, error) {
	if addr == nil {
		return core.Address{}, fmt.Errorf("socket: nil UDP address")
	}

	scopeID, err := scopeIDFromZone(addr.Zone)
	if err != nil {
		return core.Address{}, err
	}

	addrPort := addr.AddrPort()
	unzoned := netip.AddrPortFrom(addrPort.Addr().WithZone("").Unmap(), addrPort.Port())
	return core.NewAddress(unzoned, scopeID)
}

// UDPAddrFromAddress converts an internal address into a UDPAddr suitable for socket I/O.
func UDPAddrFromAddress(addr core.Address) *net.UDPAddr {
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
		return 0, fmt.Errorf("socket: resolve interface zone %q: %w", zone, err)
	}
	return uint32(iface.Index), nil
}

func zoneFromScopeID(scopeID uint32) string {
	if scopeID == 0 {
		return ""
	}

	iface, err := net.InterfaceByIndex(int(scopeID))
	if err == nil && iface.Name != "" {
		return iface.Name
	}

	return strconv.FormatUint(uint64(scopeID), 10)
}
