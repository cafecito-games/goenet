package socket

import (
	"net"
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// AddressFromAddrPort is a thin re-export of core.AddressFromAddrPort kept here
// so callers that already depend on the socket package don't need a second
// import. New code should prefer core.AddressFromAddrPort directly.
func AddressFromAddrPort(addr netip.AddrPort) (core.Address, error) {
	return core.AddressFromAddrPort(addr)
}

// AddressFromUDPAddr is a thin re-export of core.AddressFromUDPAddr.
func AddressFromUDPAddr(addr *net.UDPAddr) (core.Address, error) {
	return core.AddressFromUDPAddr(addr)
}

// UDPAddrFromAddress is a thin re-export of core.UDPAddrFromAddress.
func UDPAddrFromAddress(addr core.Address) *net.UDPAddr {
	return core.UDPAddrFromAddress(addr)
}
