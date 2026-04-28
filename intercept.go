package goenet

import (
	"net/netip"

	"github.com/cafecito-games/goenet/internal/core"
)

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult = core.InterceptResult

const (
	InterceptResultContinue = core.InterceptResultContinue
	InterceptResultConsume  = core.InterceptResultConsume
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

func toCoreEvent(event *Event) *core.Event {
	if event == nil {
		return nil
	}

	return &core.Event{
		Type:      core.EventType(event.Type),
		ChannelID: event.ChannelID,
		Data:      event.Data,
		Packet:    event.Packet,
	}
}
