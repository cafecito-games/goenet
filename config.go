package goenet

const (
	defaultMTU                uint32 = 1392
	defaultMaximumPacketSize  uint32 = 32 * 1024 * 1024
	defaultMaximumWaitingData uint32 = 32 * 1024 * 1024
)

// Config configures host construction and ENet compatibility limits.
type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
}

// DefaultConfig returns ENet-compatible host defaults.
func DefaultConfig() Config {
	return Config{
		MTU:                defaultMTU,
		MaximumPacketSize:  defaultMaximumPacketSize,
		MaximumWaitingData: defaultMaximumWaitingData,
	}
}
