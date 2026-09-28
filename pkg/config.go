package goenet

import (
	"fmt"
	"log/slog"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/protocol"
)

// Config configures host construction and ENet compatibility limits.
type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
	Checksum           Checksummer
	Compressor         Compressor
	Intercept          Interceptor
	// Logger receives component-tagged records. If nil, all logging is suppressed.
	Logger *slog.Logger
}

// DefaultConfig returns ENet-compatible host defaults.
func DefaultConfig() Config {
	return fromCoreConfig(core.DefaultConfig())
}

func normalizeConfig(cfg Config) (normalized Config, coreCfg core.Config, err error) {
	if cfg.PeerCount < 0 {
		err = fmt.Errorf("goenet: peer count must be non-negative")
		return
	}
	if cfg.PeerCount > int(protocol.MaximumPeerID) {
		err = fmt.Errorf("goenet: peer count must not exceed %d", protocol.MaximumPeerID)
		return
	}

	coreCfg = core.DefaultConfig()
	if cfg.PeerCount != 0 {
		coreCfg.PeerCount = cfg.PeerCount
	}
	if cfg.ChannelLimit != 0 {
		coreCfg.ChannelLimit = cfg.ChannelLimit
	}
	if cfg.MTU != 0 {
		coreCfg.MTU = cfg.MTU
	}
	if cfg.MaximumPacketSize != 0 {
		coreCfg.MaximumPacketSize = cfg.MaximumPacketSize
	}
	if cfg.MaximumWaitingData != 0 {
		coreCfg.MaximumWaitingData = cfg.MaximumWaitingData
	}
	// Checksummer and Compressor are type aliases for core.* equivalents, so
	// the public interfaces flow straight through without an adapter shim.
	// Interceptor still needs a small bridge because public InterceptDecision
	// carries a public *Event with a *Peer handle.
	if cfg.Checksum != nil {
		coreCfg.Checksum = cfg.Checksum
	}
	if cfg.Compressor != nil {
		coreCfg.Compressor = cfg.Compressor
	}
	if cfg.Intercept != nil {
		coreCfg.Intercept = interceptorAdapter{inner: cfg.Intercept}
	}
	if cfg.Logger != nil {
		coreCfg.Logger = cfg.Logger
	}
	if coreCfg.ChannelLimit == 0 {
		coreCfg.ChannelLimit = uint8(protocol.MaximumPeerID >> 4)
	}
	if coreCfg.MTU < protocol.MinimumMTU || coreCfg.MTU > protocol.MaximumMTU {
		err = fmt.Errorf(
			"goenet: mtu must be between %d and %d bytes",
			protocol.MinimumMTU,
			protocol.MaximumMTU,
		)
		return
	}

	normalized = fromCoreConfig(coreCfg)
	normalized.Checksum = cfg.Checksum
	normalized.Compressor = cfg.Compressor
	normalized.Intercept = cfg.Intercept
	normalized.Logger = cfg.Logger

	return
}

func fromCoreConfig(cfg core.Config) Config {
	return Config{
		PeerCount:          cfg.PeerCount,
		ChannelLimit:       cfg.ChannelLimit,
		MTU:                cfg.MTU,
		MaximumPacketSize:  cfg.MaximumPacketSize,
		MaximumWaitingData: cfg.MaximumWaitingData,
		Logger:             cfg.Logger,
	}
}
