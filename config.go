package goenet

import "github.com/cafecito-games/goenet/internal/core"

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
}

// DefaultConfig returns ENet-compatible host defaults.
func DefaultConfig() Config {
	coreCfg := core.DefaultConfig()
	return Config{
		PeerCount:          coreCfg.PeerCount,
		ChannelLimit:       coreCfg.ChannelLimit,
		MTU:                coreCfg.MTU,
		MaximumPacketSize:  coreCfg.MaximumPacketSize,
		MaximumWaitingData: coreCfg.MaximumWaitingData,
		Checksum:           coreCfg.Checksum,
		Compressor:         coreCfg.Compressor,
	}
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
		coreCfg.Checksum = cfg.Checksum
	}
	if cfg.Compressor != nil {
		coreCfg.Compressor = cfg.Compressor
	}
	if cfg.Intercept != nil {
		coreCfg.Intercept = interceptorAdapter{inner: cfg.Intercept}
	}

	return coreCfg
}
