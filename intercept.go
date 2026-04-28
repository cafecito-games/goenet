package goenet

import "net/netip"

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult uint8

const (
	InterceptResultContinue InterceptResult = iota
	InterceptResultConsume
)

// InterceptDecision reports whether an interceptor consumed the packet and whether it synthesized an event.
type InterceptDecision struct {
	Result InterceptResult
	Event  *Event
}

// Interceptor can consume a received raw UDP packet before protocol decoding and optionally synthesize a service event.
type Interceptor interface {
	Intercept(addr netip.AddrPort, payload []byte) (InterceptDecision, error)
}
