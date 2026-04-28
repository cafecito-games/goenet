package engine

import (
	"fmt"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/socket"
	"github.com/cafecito-games/goenet/internal/timeutil"
)

const (
	defaultBandwidthThrottleInterval  uint32 = 1000
	defaultRoundTripTimeout           uint32 = 500
	defaultPacketThrottle             uint32 = 32
	peerWindowSizeScale               uint32 = 64 * 1024
	packetThrottleScale               uint32 = 32
	packetThrottleCounter             uint32 = 7
	defaultPacketThrottleAcceleration uint32 = 2
	defaultPacketThrottleDeceleration uint32 = 2
	defaultPacketThrottleInterval     uint32 = 5000
	defaultTimeoutLimit               uint32 = 32
	defaultTimeoutMinimum             uint32 = 5000
	defaultTimeoutMaximum             uint32 = 30000
)

// Host carries the minimal outbound engine state for queueing and flush tests.
type Host struct {
	config                     core.Config
	socket                     socket.DatagramSocket
	peers                      []*peer.Peer
	serviceTime                uint32
	totalQueued                uint32
	incomingBandwidth          uint32
	outgoingBandwidth          uint32
	bandwidthThrottleEpoch     uint32
	bandwidthLimitedPeers      uint32
	recalculateBandwidthLimits bool
	dispatchSet                map[*peer.Peer]struct{}
	dispatchQ                  []*peer.Peer
	runtime                    map[*peer.Peer]*peerRuntime
	intercepted                *Event
	nextConnectID              uint32
}

func NewHost(config core.Config, sock socket.DatagramSocket, serviceTime uint32) *Host {
	cfg := core.DefaultConfig()
	if config.PeerCount != 0 {
		cfg.PeerCount = config.PeerCount
	}
	if config.ChannelLimit != 0 {
		cfg.ChannelLimit = config.ChannelLimit
	}
	if config.MTU != 0 {
		cfg.MTU = config.MTU
	}
	if config.MaximumPacketSize != 0 {
		cfg.MaximumPacketSize = config.MaximumPacketSize
	}
	if config.MaximumWaitingData != 0 {
		cfg.MaximumWaitingData = config.MaximumWaitingData
	}
	if config.Checksum != nil {
		cfg.Checksum = config.Checksum
	}
	if config.Compressor != nil {
		cfg.Compressor = config.Compressor
	}
	if config.Intercept != nil {
		cfg.Intercept = config.Intercept
	}
	if cfg.ChannelLimit == 0 {
		cfg.ChannelLimit = uint8(protocolMaximumPeerID >> 4)
	}

	host := &Host{
		config:      cfg,
		socket:      sock,
		serviceTime: serviceTime,
		dispatchSet: make(map[*peer.Peer]struct{}),
		runtime:     make(map[*peer.Peer]*peerRuntime),
	}
	for index := 0; index < cfg.PeerCount; index++ {
		host.peers = append(host.peers, host.newPeerSlot(index))
	}

	return host
}

func (h *Host) AddPeer(addr core.Address, state core.PeerState) *peer.Peer {
	for index, candidate := range h.peers {
		if candidate != nil && candidate.State == core.PeerStateDisconnected {
			return h.configurePeer(candidate, index, addr, state)
		}
	}

	index := len(h.peers)
	p := h.newPeerSlot(index)
	p = h.configurePeer(p, index, addr, state)
	h.peers = append(h.peers, p)
	return p
}

func (h *Host) Send(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if packet == nil {
		return fmt.Errorf("engine: nil packet")
	}
	if p.State != core.PeerStateConnected && p.State != core.PeerStateDisconnectLater {
		return fmt.Errorf("engine: peer not connected")
	}
	if int(channelID) >= len(p.Channels) {
		return fmt.Errorf("engine: channel %d out of range", channelID)
	}
	if len(packet.Data) > int(h.config.MaximumPacketSize) {
		return fmt.Errorf("engine: packet too large: %d", len(packet.Data))
	}

	return h.queueOutgoingCommand(p, channelID, packet)
}

func (h *Host) Disconnect(p *peer.Peer, data uint32) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if p.State == core.PeerStateDisconnecting ||
		p.State == core.PeerStateDisconnected ||
		p.State == core.PeerStateAcknowledgingDisconnect ||
		p.State == core.PeerStateZombie {
		return nil
	}
	if p.State != core.PeerStateConnected && p.State != core.PeerStateDisconnectLater {
		return fmt.Errorf("engine: peer not connected")
	}

	h.clearPeerQueues(p)
	if err := h.queueOutgoingControlCommand(p, peer.Command{
		Header: peer.Header{
			Command:   protocol.CommandDisconnect,
			ChannelID: 0xFF,
			Flags:     protocol.CommandFlagAcknowledge,
		},
		Payload: &protocol.Disconnect{
			Data: data,
		},
	}); err != nil {
		return err
	}

	h.runtime[p].eventData = data
	h.runtime[p].disconnectLater = false
	p.State = core.PeerStateDisconnecting
	return nil
}

func (h *Host) DisconnectLater(p *peer.Peer, data uint32) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if (p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater) && h.hasOutgoingCommands(p) {
		h.runtime[p].eventData = data
		h.runtime[p].disconnectLater = true
		p.State = core.PeerStateDisconnectLater
		return nil
	}

	return h.Disconnect(p, data)
}

func (h *Host) Reset(p *peer.Peer) {
	if p == nil {
		return
	}

	h.resetPeer(p)
}

func (h *Host) Connect(addr core.Address, channelCount uint8, data uint32) (*peer.Peer, error) {
	requestedChannels := clampUint32(uint32(channelCount), protocol.MinimumChannelCount, protocol.MaximumChannelCount)
	if h.config.ChannelLimit != 0 && requestedChannels > uint32(h.config.ChannelLimit) {
		requestedChannels = uint32(h.config.ChannelLimit)
	}

	var (
		p     *peer.Peer
		index int
	)
	for i, candidate := range h.peers {
		if candidate != nil && candidate.State == core.PeerStateDisconnected {
			p = candidate
			index = i
			break
		}
	}
	if p == nil {
		return nil, fmt.Errorf("engine: no disconnected peers available")
	}

	p = h.configurePeer(p, index, addr, core.PeerStateConnecting)
	p.Channels = make([]peer.Channel, requestedChannels)
	for i := range p.Channels {
		p.Channels[i] = peer.NewChannel()
	}
	p.Address = addr
	p.ConnectID = h.nextPeerConnectID()

	runtime := h.runtime[p]
	runtime.windowSize = h.outboundWindowSize()

	connect := &protocol.Connect{
		OutgoingPeerID:             p.IncomingPeerID,
		IncomingSessionID:          p.IncomingSessionID,
		OutgoingSessionID:          p.OutgoingSessionID,
		MTU:                        p.MTU,
		WindowSize:                 runtime.windowSize,
		ChannelCount:               requestedChannels,
		IncomingBandwidth:          h.incomingBandwidth,
		OutgoingBandwidth:          h.outgoingBandwidth,
		PacketThrottleInterval:     p.PacketThrottleInterval,
		PacketThrottleAcceleration: p.PacketThrottleAcceleration,
		PacketThrottleDeceleration: p.PacketThrottleDeceleration,
		ConnectID:                  p.ConnectID,
		Data:                       data,
	}

	if err := h.queueOutgoingControlCommand(p, peer.Command{
		Header: peer.Header{
			Command:   protocol.CommandConnect,
			ChannelID: 0xFF,
			Flags:     protocol.CommandFlagAcknowledge,
		},
		Payload: connect,
	}); err != nil {
		return nil, err
	}

	return p, nil
}

func (h *Host) newPeerSlot(index int) *peer.Peer {
	p := &peer.Peer{}
	h.initializePeer(p, index, core.Address{}, core.PeerStateDisconnected, protocolMaximumPeerID, 0xFF, 0xFF)
	h.runtime[p] = defaultPeerRuntime()
	return p
}

func (h *Host) configurePeer(p *peer.Peer, index int, addr core.Address, state core.PeerState) *peer.Peer {
	channels := make([]peer.Channel, h.config.ChannelLimit)
	for i := range channels {
		channels[i] = peer.NewChannel()
	}

	outgoingPeerID := uint16(index + 1)
	if state == core.PeerStateConnecting {
		outgoingPeerID = protocolMaximumPeerID
	}

	h.initializePeer(p, index, addr, state, outgoingPeerID, 0xFF, uint8((index%3)+1))
	p.Channels = channels
	h.runtime[p] = defaultPeerRuntime()
	return p
}

func (h *Host) initializePeer(
	p *peer.Peer,
	index int,
	addr core.Address,
	state core.PeerState,
	outgoingPeerID uint16,
	incomingSessionID uint8,
	outgoingSessionID uint8,
) {
	*p = peer.Peer{
		OutgoingPeerID:               outgoingPeerID,
		IncomingPeerID:               uint16(index),
		IncomingSessionID:            incomingSessionID,
		OutgoingSessionID:            outgoingSessionID,
		MTU:                          h.config.MTU,
		Address:                      addr,
		State:                        state,
		PacketThrottle:               defaultPacketThrottle,
		PacketThrottleLimit:          packetThrottleScale,
		PacketThrottleCounter:        packetThrottleCounter,
		PacketThrottleAcceleration:   defaultPacketThrottleAcceleration,
		PacketThrottleDeceleration:   defaultPacketThrottleDeceleration,
		PacketThrottleInterval:       defaultPacketThrottleInterval,
		TimeoutLimit:                 defaultTimeoutLimit,
		TimeoutMinimum:               defaultTimeoutMinimum,
		TimeoutMaximum:               defaultTimeoutMaximum,
		LastRoundTripTime:            defaultRoundTripTimeout,
		LowestRoundTripTime:          defaultRoundTripTimeout,
		RoundTripTime:                defaultRoundTripTimeout,
		RoundTripTimeVariance:        0,
		LastRoundTripTimeVariance:    0,
		HighestRoundTripTimeVariance: 0,
	}
}

func (h *Host) nextPeerConnectID() uint32 {
	h.nextConnectID++
	if h.nextConnectID == 0 {
		h.nextConnectID = 1
	}

	return h.nextConnectID
}

func (h *Host) outboundWindowSize() uint32 {
	if h.outgoingBandwidth == 0 {
		return protocol.MaximumWindowSize
	}

	windowSize := (h.outgoingBandwidth / peerWindowSizeScale) * protocol.MinimumWindowSize
	return clampUint32(windowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize)
}

func (h *Host) hasOutgoingCommands(p *peer.Peer) bool {
	return p.OutgoingCommands.Len() > 0 || p.OutgoingSendReliableCommands.Len() > 0 || p.SentReliableCommands.Len() > 0
}

func (h *Host) BandwidthLimit(incomingBandwidth, outgoingBandwidth uint32) {
	h.incomingBandwidth = incomingBandwidth
	h.outgoingBandwidth = outgoingBandwidth
	h.recalculateBandwidthLimits = true
}

func (h *Host) updateNextTimeout(p *peer.Peer) {
	front := p.SentReliableCommands.Front()
	if front == nil {
		p.NextTimeout = 0
		return
	}

	cmd := front.Value()
	p.NextTimeout = cmd.SentTime + cmd.RoundTripTimeout
}

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
				return h.notifyDisconnectTimeout(p)
			}

			p.PacketsLost++
			p.TotalPacketsLost++
			cmd.RoundTripTimeout = p.RoundTripTime + 4*p.RoundTripTimeVariance

			p.SentReliableCommands.Remove(elem)
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

func (h *Host) bandwidthThrottle() {
	elapsedTime := h.serviceTime - h.bandwidthThrottleEpoch
	if elapsedTime < defaultBandwidthThrottleInterval {
		return
	}
	if h.outgoingBandwidth == 0 && h.incomingBandwidth == 0 {
		return
	}

	h.bandwidthThrottleEpoch = h.serviceTime

	peersRemaining := h.connectedPeerCount()
	if peersRemaining == 0 {
		return
	}

	dataTotal := ^uint32(0)
	bandwidth := ^uint32(0)
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
		return
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

		_ = h.queueOutgoingControlCommand(p, peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandBandwidthLimit,
				ChannelID: 0xFF,
				Flags:     protocol.CommandFlagAcknowledge,
			},
			Payload: &protocol.BandwidthLimit{
				IncomingBandwidth: incomingLimit,
				OutgoingBandwidth: h.outgoingBandwidth,
			},
		})
	}
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

func lessOutgoingCommand(a, b *peer.OutgoingCommand) bool {
	return a.QueueTime < b.QueueTime
}
