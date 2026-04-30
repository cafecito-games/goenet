package goenet

import (
	"log/slog"

	"github.com/cafecito-games/goenet/internal/core"
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
	Logger             *slog.Logger
}

// DefaultConfig returns ENet-compatible host defaults.
func DefaultConfig() Config {
	return fromCoreConfig(core.DefaultConfig())
}

func toCoreConfig(cfg Config) core.Config {
	coreCfg := core.DefaultConfig()
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
	if cfg.Checksum != nil {
		coreCfg.Checksum = checksummerAdapter{inner: cfg.Checksum}
	}
	if cfg.Compressor != nil {
		coreCfg.Compressor = compressorAdapter{inner: cfg.Compressor}
	}
	if cfg.Intercept != nil {
		coreCfg.Intercept = interceptorAdapter{inner: cfg.Intercept}
	}
	if cfg.Logger != nil {
		coreCfg.Logger = cfg.Logger
	}

	return coreCfg
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
