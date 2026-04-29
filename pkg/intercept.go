package goenet

import (
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult uint8

const (
	// InterceptResultContinue lets the packet continue through normal protocol handling.
	InterceptResultContinue InterceptResult = iota
	// InterceptResultConsume stops normal protocol handling for the packet.
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

type interceptorAdapter struct {
	inner Interceptor
}

// Intercept adapts the public interceptor interface to the internal core form.
func (a interceptorAdapter) Intercept(addr netip.AddrPort, payload []byte) (core.InterceptDecision, error) {
	decision, err := a.inner.Intercept(addr, payload)
	if err != nil {
		return core.InterceptDecision{}, err
	}

	return core.InterceptDecision{
		Result: core.InterceptResult(decision.Result),
		Event:  toCoreEvent(decision.Event),
	}, nil
}
