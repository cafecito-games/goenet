package goenet

import (
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult = core.InterceptResult

const (
	// InterceptResultContinue lets the packet continue through normal protocol handling.
	InterceptResultContinue = core.InterceptResultContinue
	// InterceptResultConsume stops normal protocol handling for the packet.
	InterceptResultConsume = core.InterceptResultConsume
)

// InterceptDecision reports whether an interceptor consumed the packet and whether it synthesized an event.
//
// Unlike the type-aliased value types in this package, InterceptDecision wraps
// the public-facing *Event (which carries a *Peer handle), so it cannot alias
// core.InterceptDecision directly.
type InterceptDecision struct {
	Result InterceptResult
	Event  *Event
}

// Interceptor can consume a received raw UDP packet before protocol decoding and optionally synthesize a service event.
type Interceptor interface {
	Intercept(addr netip.AddrPort, payload []byte) (InterceptDecision, error)
}

// interceptorAdapter bridges the public Interceptor (whose decision carries a
// public *Event) into the core.Interceptor surface the engine consumes.
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
		Result: decision.Result,
		Event:  toCoreEvent(decision.Event),
	}, nil
}
