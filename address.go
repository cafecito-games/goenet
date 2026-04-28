package goenet

import "net/netip"

// Address identifies a remote endpoint and preserves ENet's IPv6 scope ID.
type Address struct {
	AddrPort netip.AddrPort
	ScopeID  uint32
}
