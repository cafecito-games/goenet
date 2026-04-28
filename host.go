package goenet

// Host is the public root for ENet-compatible peer management.
type Host struct {
	config Config
}

// Config returns the host configuration snapshot.
func (h *Host) Config() Config {
	return h.config
}
