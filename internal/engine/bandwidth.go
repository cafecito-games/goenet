// Package engine implements the ENet-compatible host runtime: peer state
// machine, send/receive paths, bandwidth throttling, and timeout management.
package engine

import (
	"fmt"
	"math"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
)

// bandwidthThrottle implements the periodic per-peer throttle/limit
// recomputation that mirrors enet_host_bandwidth_throttle. Two passes:
// the first re-clamps each peer's PacketThrottleLimit using the negotiated
// outgoing bandwidth budget; the second, when recalculateBandwidthLimits is
// set, broadcasts updated BandwidthLimit commands on the host's incoming
// budget.
func (h *Host) bandwidthThrottle() error {
	elapsedTime := h.serviceTime - h.bandwidthThrottleEpoch
	if elapsedTime < defaultBandwidthThrottleInterval {
		return nil
	}
	if h.outgoingBandwidth == 0 && h.incomingBandwidth == 0 {
		return nil
	}

	h.bandwidthThrottleEpoch = h.serviceTime

	peersRemaining := h.connectedPeerCount()
	if peersRemaining == 0 {
		return nil
	}

	dataTotal := uint32(math.MaxUint32)
	bandwidth := uint32(math.MaxUint32)
	throttle := uint32(0)
	bandwidthLimit := uint32(0)
	h.bandwidthLimitedPeers = h.bandwidthLimitedPeerCount()
	needsAdjustment := h.bandwidthLimitedPeers > 0

	if h.outgoingBandwidth != 0 {
		dataTotal = 0
		bandwidth = (h.outgoingBandwidth * elapsedTime) / 1000
		for _, p := range h.peers {
			if !isBandwidthThrottlePeer(p) {
				continue
			}
			dataTotal += p.OutgoingDataTotal
		}
	}

	for peersRemaining > 0 && needsAdjustment {
		needsAdjustment = false
		if dataTotal <= bandwidth {
			throttle = packetThrottleScale
		} else {
			throttle = (bandwidth * packetThrottleScale) / dataTotal
		}

		for _, p := range h.peers {
			if !isBandwidthThrottlePeer(p) || p.IncomingBandwidth == 0 || p.OutgoingBandwidthThrottleEpoch == h.serviceTime {
				continue
			}

			peerBandwidth := (p.IncomingBandwidth * elapsedTime) / 1000
			if (throttle*p.OutgoingDataTotal)/packetThrottleScale <= peerBandwidth {
				continue
			}

			p.PacketThrottleLimit = (peerBandwidth * packetThrottleScale) / p.OutgoingDataTotal
			if p.PacketThrottleLimit == 0 {
				p.PacketThrottleLimit = 1
			}
			if p.PacketThrottle > p.PacketThrottleLimit {
				p.PacketThrottle = p.PacketThrottleLimit
			}
			h.logger.Debug(
				"peer throttle limited",
				"peer_id", p.IncomingPeerID,
				"packet_throttle_limit", p.PacketThrottleLimit,
				"incoming_bandwidth", p.IncomingBandwidth,
			)

			p.OutgoingBandwidthThrottleEpoch = h.serviceTime
			p.IncomingDataTotal = 0
			p.OutgoingDataTotal = 0

			needsAdjustment = true
			peersRemaining--
			bandwidth -= peerBandwidth
			dataTotal -= peerBandwidth
		}
	}

	if peersRemaining > 0 {
		if dataTotal <= bandwidth {
			throttle = packetThrottleScale
		} else {
			throttle = (bandwidth * packetThrottleScale) / dataTotal
		}

		for _, p := range h.peers {
			if !isBandwidthThrottlePeer(p) || p.OutgoingBandwidthThrottleEpoch == h.serviceTime {
				continue
			}
			p.PacketThrottleLimit = throttle
			if p.PacketThrottle > p.PacketThrottleLimit {
				p.PacketThrottle = p.PacketThrottleLimit
			}
			p.IncomingDataTotal = 0
			p.OutgoingDataTotal = 0
		}
	}

	if !h.recalculateBandwidthLimits {
		return nil
	}

	h.recalculateBandwidthLimits = false
	peersRemaining = h.connectedPeerCount()
	bandwidth = h.incomingBandwidth
	needsAdjustment = true
	if bandwidth == 0 {
		bandwidthLimit = 0
	} else {
		for peersRemaining > 0 && needsAdjustment {
			needsAdjustment = false
			bandwidthLimit = bandwidth / peersRemaining

			for _, p := range h.peers {
				if !isBandwidthThrottlePeer(p) || p.IncomingBandwidthThrottleEpoch == h.serviceTime {
					continue
				}
				if p.OutgoingBandwidth > 0 && p.OutgoingBandwidth >= bandwidthLimit {
					continue
				}

				p.IncomingBandwidthThrottleEpoch = h.serviceTime
				needsAdjustment = true
				peersRemaining--
				bandwidth -= p.OutgoingBandwidth
			}
		}
	}

	for _, p := range h.peers {
		if !isBandwidthThrottlePeer(p) {
			continue
		}

		incomingLimit := bandwidthLimit
		if p.IncomingBandwidthThrottleEpoch == h.serviceTime {
			incomingLimit = p.OutgoingBandwidth
		}

		if err := h.queueOutgoingControlCommand(p, peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandBandwidthLimit,
				ChannelID: 0xFF,
				Flags:     protocol.CommandFlagAcknowledge,
			},
			Payload: &protocol.BandwidthLimit{
				IncomingBandwidth: incomingLimit,
				OutgoingBandwidth: h.outgoingBandwidth,
			},
		}); err != nil {
			return fmt.Errorf("engine: bandwidth limit broadcast: %w", err)
		}
	}

	return nil
}

// BandwidthLimit updates the host bandwidth caps and schedules peer recomputation.
func (h *Host) BandwidthLimit(incomingBandwidth, outgoingBandwidth uint32) {
	h.incomingBandwidth = incomingBandwidth
	h.outgoingBandwidth = outgoingBandwidth
	h.recalculateBandwidthLimits = true
}

func (h *Host) connectedPeerCount() uint32 {
	var count uint32
	for _, p := range h.peers {
		if isBandwidthThrottlePeer(p) {
			count++
		}
	}
	return count
}

func (h *Host) bandwidthLimitedPeerCount() uint32 {
	var count uint32
	for _, p := range h.peers {
		if isBandwidthThrottlePeer(p) && p.IncomingBandwidth > 0 {
			count++
		}
	}
	return count
}

func isBandwidthThrottlePeer(p *peer.Peer) bool {
	return p != nil && (p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater)
}
