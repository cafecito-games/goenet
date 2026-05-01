package engine

import (
	"context"
	"log/slog"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/timeutil"
)

// updateNextTimeout refreshes a peer's NextTimeout marker based on the head of
// its in-flight reliable queue. Called whenever the queue head changes.
func (h *Host) updateNextTimeout(p *peer.Peer) {
	front := p.SentReliableCommands.Front()
	if front == nil {
		p.NextTimeout = 0
		return
	}

	cmd := front.Value()
	p.NextTimeout = cmd.SentTime + cmd.RoundTripTimeout
}

// checkTimeouts walks each peer's in-flight reliable queue, requeues commands
// whose retransmit window has elapsed, and surfaces a disconnect-timeout event
// when retransmits exceed the per-peer timeout limits.
func (h *Host) checkTimeouts() (Event, bool) {
	for _, p := range h.peers {
		if p == nil || p.State == core.PeerStateDisconnected || p.State == core.PeerStateZombie {
			continue
		}

		current := p.SentReliableCommands.Front()
		for current != nil {
			elem := current
			cmd := elem.Value()
			current = current.Next()

			if timeutil.Difference(h.serviceTime, cmd.SentTime) < cmd.RoundTripTimeout {
				continue
			}
			if p.EarliestTimeout == 0 || timeutil.Less(cmd.SentTime, p.EarliestTimeout) {
				p.EarliestTimeout = cmd.SentTime
			}

			attemptLimit := uint32(0)
			if cmd.SendAttempts > 0 {
				attemptLimit = uint32(1) << (cmd.SendAttempts - 1)
			}
			if p.EarliestTimeout != 0 &&
				(timeutil.Difference(h.serviceTime, p.EarliestTimeout) >= p.TimeoutMaximum ||
					(attemptLimit >= p.TimeoutLimit &&
						timeutil.Difference(h.serviceTime, p.EarliestTimeout) >= p.TimeoutMinimum)) {
				h.logger.Info(
					"peer disconnect timeout",
					"peer_id", p.IncomingPeerID,
					"state", p.State,
					"send_attempts", cmd.SendAttempts,
					"timeout_limit", p.TimeoutLimit,
				)
				return h.notifyDisconnectTimeout(p)
			}

			p.PacketsLost++
			p.TotalPacketsLost++
			cmd.RoundTripTimeout = p.RoundTripTime + 4*p.RoundTripTimeVariance

			p.UnindexSentReliableCommand(cmd)
			p.SentReliableCommands.Remove(elem)
			if h.logger.Enabled(context.Background(), slog.LevelDebug) {
				h.logger.Debug(
					"requeue timed out command",
					"peer_id", p.IncomingPeerID,
					"command", cmd.Command.Header.Command,
					"send_attempts", cmd.SendAttempts,
					"reliable_sequence_number", cmd.ReliableSequenceNumber,
				)
			}
			if cmd.Packet != nil {
				if uint32(cmd.FragmentLength) >= p.ReliableDataInTransit {
					p.ReliableDataInTransit = 0
				} else {
					p.ReliableDataInTransit -= uint32(cmd.FragmentLength)
				}
				p.OutgoingSendReliableCommands.InsertOrdered(cmd, lessOutgoingCommand)
			} else {
				p.OutgoingCommands.InsertOrdered(cmd, lessOutgoingCommand)
			}
		}

		h.updateNextTimeout(p)
	}

	return Event{}, false
}

func (h *Host) notifyDisconnectTimeout(p *peer.Peer) (Event, bool) {
	if p.State >= core.PeerStateConnectionPending {
		h.recalculateBandwidthLimits = true
	}

	if p.State != core.PeerStateConnecting && p.State < core.PeerStateConnectionSucceeded {
		h.resetPeer(p)
		return Event{}, false
	}

	event := Event{
		Type: core.EventDisconnectTimeout,
		Peer: p,
	}
	h.resetPeer(p)
	return event, true
}
