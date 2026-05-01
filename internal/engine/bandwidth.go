// Package engine implements the ENet-compatible host runtime: peer state
// machine, send/receive paths, bandwidth throttling, and timeout management.
package engine

import (
	"fmt"
	"log/slog"
	"math"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/timeutil"
)

// bandwidthThrottler owns the host-level bandwidth budgets and the periodic
// throttle pass that translates them into per-peer PacketThrottleLimit values
// and broadcast BandwidthLimit commands. It is stored as a field on *Host;
// every read or write of bandwidth state goes through this struct so the
// throttle subsystem can be reasoned about (and tested) in isolation.
//
// The actual throttle math is faithful to enet_host_bandwidth_throttle —
// see the inline references inside Run.
type bandwidthThrottler struct {
	incomingBudget     uint32
	outgoingBudget     uint32
	epoch              uint32
	limitedPeers       uint32
	needsRecalculation bool
}

// SetBudgets updates the host's incoming/outgoing caps and schedules a
// per-peer recomputation on the next throttle pass. A value of zero on either
// argument disables the corresponding limit.
func (t *bandwidthThrottler) SetBudgets(incoming, outgoing uint32) {
	t.incomingBudget = incoming
	t.outgoingBudget = outgoing
	t.needsRecalculation = true
}

// IncomingBudget returns the host-wide incoming bandwidth cap, used during
// handshake to derive the peer's advertised window size.
func (t *bandwidthThrottler) IncomingBudget() uint32 { return t.incomingBudget }

// OutgoingBudget returns the host-wide outgoing bandwidth cap, used during
// handshake to derive the peer's negotiated window size.
func (t *bandwidthThrottler) OutgoingBudget() uint32 { return t.outgoingBudget }

// MarkRecalculate flags the next throttle pass to re-broadcast BandwidthLimit
// commands. Called from receive paths when a peer's bandwidth profile
// changes (handshake completion, BandwidthLimit command, peer timeout/reset).
func (t *bandwidthThrottler) MarkRecalculate() { t.needsRecalculation = true }

// IncrementLimitedPeers grows the count of peers with a non-zero
// IncomingBandwidth advertisement. Called from the receive path when a
// peer sets a new positive IncomingBandwidth value.
func (t *bandwidthThrottler) IncrementLimitedPeers() {
	t.limitedPeers++
}

// DecrementLimitedPeers shrinks the count of peers with a non-zero
// IncomingBandwidth advertisement. Underflow-saturated to zero.
func (t *bandwidthThrottler) DecrementLimitedPeers() {
	if t.limitedPeers > 0 {
		t.limitedPeers--
	}
}

// dueAt reports whether enough wall time has elapsed since the previous
// throttle pass to warrant another. Pulled out of the Service loop so the
// inline comparison stays readable.
func (t *bandwidthThrottler) dueAt(serviceTime, interval uint32) bool {
	return timeutil.Difference(serviceTime, t.epoch) >= interval
}

// Run executes one throttle pass: re-clamps each peer's PacketThrottleLimit
// using the negotiated outgoing budget, then (when needsRecalculation is
// set) broadcasts updated BandwidthLimit commands on the host's incoming
// budget. queueCommand is the engine's outbound-control-command queueing
// callback; logger is the engine logger to inherit.
func (t *bandwidthThrottler) Run(
	serviceTime uint32,
	peers []*peer.Peer,
	queueCommand func(*peer.Peer, peer.Command) error,
	logger *slog.Logger,
) error {
	elapsedTime := serviceTime - t.epoch
	if elapsedTime < defaultBandwidthThrottleInterval {
		return nil
	}
	if t.outgoingBudget == 0 && t.incomingBudget == 0 {
		return nil
	}

	t.epoch = serviceTime

	peersRemaining := connectedPeerCount(peers)
	if peersRemaining == 0 {
		return nil
	}

	dataTotal := uint32(math.MaxUint32)
	bandwidth := uint32(math.MaxUint32)
	throttle := uint32(0)
	bandwidthLimit := uint32(0)
	t.limitedPeers = bandwidthLimitedPeerCount(peers)
	needsAdjustment := t.limitedPeers > 0

	if t.outgoingBudget != 0 {
		dataTotal = 0
		bandwidth = (t.outgoingBudget * elapsedTime) / 1000
		for _, p := range peers {
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

		for _, p := range peers {
			if !isBandwidthThrottlePeer(p) || p.IncomingBandwidth == 0 || p.OutgoingBandwidthThrottleEpoch == serviceTime {
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
			logger.Debug(
				"peer throttle limited",
				"peer_id", p.IncomingPeerID,
				"packet_throttle_limit", p.PacketThrottleLimit,
				"incoming_bandwidth", p.IncomingBandwidth,
			)

			p.OutgoingBandwidthThrottleEpoch = serviceTime
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

		for _, p := range peers {
			if !isBandwidthThrottlePeer(p) || p.OutgoingBandwidthThrottleEpoch == serviceTime {
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

	if !t.needsRecalculation {
		return nil
	}

	t.needsRecalculation = false
	peersRemaining = connectedPeerCount(peers)
	bandwidth = t.incomingBudget
	needsAdjustment = true
	if bandwidth == 0 {
		bandwidthLimit = 0
	} else {
		for peersRemaining > 0 && needsAdjustment {
			needsAdjustment = false
			bandwidthLimit = bandwidth / peersRemaining

			for _, p := range peers {
				if !isBandwidthThrottlePeer(p) || p.IncomingBandwidthThrottleEpoch == serviceTime {
					continue
				}
				if p.OutgoingBandwidth > 0 && p.OutgoingBandwidth >= bandwidthLimit {
					continue
				}

				p.IncomingBandwidthThrottleEpoch = serviceTime
				needsAdjustment = true
				peersRemaining--
				bandwidth -= p.OutgoingBandwidth
			}
		}
	}

	for _, p := range peers {
		if !isBandwidthThrottlePeer(p) {
			continue
		}

		incomingLimit := bandwidthLimit
		if p.IncomingBandwidthThrottleEpoch == serviceTime {
			incomingLimit = p.OutgoingBandwidth
		}

		if err := queueCommand(p, peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandBandwidthLimit,
				ChannelID: 0xFF,
				Flags:     protocol.CommandFlagAcknowledge,
			},
			Payload: &protocol.BandwidthLimit{
				IncomingBandwidth: incomingLimit,
				OutgoingBandwidth: t.outgoingBudget,
			},
		}); err != nil {
			return fmt.Errorf("engine: bandwidth limit broadcast: %w", err)
		}
	}

	return nil
}

// BandwidthLimit updates the host bandwidth caps and schedules peer recomputation.
func (h *Host) BandwidthLimit(incomingBandwidth, outgoingBandwidth uint32) {
	h.throttle.SetBudgets(incomingBandwidth, outgoingBandwidth)
}

func connectedPeerCount(peers []*peer.Peer) uint32 {
	var count uint32
	for _, p := range peers {
		if isBandwidthThrottlePeer(p) {
			count++
		}
	}
	return count
}

func bandwidthLimitedPeerCount(peers []*peer.Peer) uint32 {
	var count uint32
	for _, p := range peers {
		if isBandwidthThrottlePeer(p) && p.IncomingBandwidth > 0 {
			count++
		}
	}
	return count
}

func isBandwidthThrottlePeer(p *peer.Peer) bool {
	return p != nil && (p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater)
}
