package goenet

// InterceptResult controls whether a raw UDP packet continues through protocol handling.
type InterceptResult uint8

const (
	InterceptResultContinue InterceptResult = iota
	InterceptResultConsume
)

// Interceptor can consume a received raw UDP packet before protocol decoding.
type Interceptor interface {
	Intercept(addr Address, payload []byte) (InterceptResult, error)
}
