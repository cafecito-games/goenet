package engine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/socket"
	"github.com/cafecito-games/goenet/internal/timeutil"
)

// Sentinel errors returned by the engine. Public callers can match against
// these via errors.Is, even when the engine wraps them with %w for context.
var (
	// ErrNilPeer is returned when a method requires a peer but receives nil.
	ErrNilPeer = errors.New("engine: nil peer")
	// ErrNilPacket is returned by Send when the caller passes a nil packet.
	ErrNilPacket = errors.New("engine: nil packet")
	// ErrPeerNotConnected is returned when a Send-style operation is invoked
	// on a peer that is not in PeerStateConnected or PeerStateDisconnectLater.
	ErrPeerNotConnected = errors.New("engine: peer not connected")
	// ErrChannelOutOfRange is returned for channel IDs outside the peer's
	// per-peer channel allocation.
	ErrChannelOutOfRange = errors.New("engine: channel out of range")
	// ErrPacketTooLarge is returned when a packet exceeds the host's
	// configured MaximumPacketSize.
	ErrPacketTooLarge = errors.New("engine: packet too large")
	// ErrNoFreePeerSlot is returned by Connect when every peer slot is in use.
	ErrNoFreePeerSlot = errors.New("engine: no disconnected peers available")
	// ErrShortWrite is returned when the underlying socket reports a partial
	// write of an outbound datagram.
	ErrShortWrite = errors.New("engine: short write")
	// ErrFragmentationLimit is returned when a reliable send would exceed the
	// MaximumFragmentCount cap.
	ErrFragmentationLimit = errors.New("engine: packet exceeds fragmentation limit")
	// ErrNoFragmentation is returned when a packet exceeds the per-fragment
	// data limit but cannot be fragmented (unsequenced flag, or peer MTU is
	// too small to fit any per-fragment overhead).
	ErrNoFragmentation = errors.New("engine: packet cannot be fragmented")
	// ErrReliableSequenceExhausted is returned when the channel's outgoing
	// reliable sequence space has insufficient room for the requested fragment
	// train.
	ErrReliableSequenceExhausted = errors.New("engine: reliable sequence space exhausted")
	// ErrCommandExceedsMTU is returned by Flush when a queued command's wire
	// size cannot fit into a single datagram given the negotiated peer MTU.
	ErrCommandExceedsMTU = errors.New("engine: queued command exceeds peer mtu")
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
	logger                     *slog.Logger
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
	sessionIDSeed              uint8
}

// NewHost constructs an engine host around the provided socket and config snapshot.
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
	if config.Logger != nil {
		cfg.Logger = config.Logger
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
		config:        cfg,
		logger:        core.ComponentLogger(cfg.Logger, "engine"),
		socket:        sock,
		serviceTime:   serviceTime,
		dispatchSet:   make(map[*peer.Peer]struct{}),
		runtime:       make(map[*peer.Peer]*peerRuntime),
		nextConnectID: randomConnectIDSeed(cfg.Logger),
		sessionIDSeed: randomSessionIDSeed(cfg.Logger),
	}
	for index := 0; index < cfg.PeerCount; index++ {
		host.peers = append(host.peers, host.newPeerSlot(index))
	}

	return host
}

// Peers returns the internal peer slot slice in stable index order.
// The returned slice aliases internal state and must be treated as read-only.
func (h *Host) Peers() []*peer.Peer {
	return h.peers
}

// Close releases the engine's underlying datagram socket. After Close the host
// must not be used; subsequent Service/Flush calls will surface a closed-socket
// error from the underlying I/O.
func (h *Host) Close() error {
	return h.socket.Close()
}

// SetServiceTime overrides the engine's millisecond clock to t. The public host
// calls this at the start of every Service/Flush from a wall clock so RTT,
// retransmit, and throttle math observe real elapsed time. Tests use it to drive
// a virtual clock deterministically.
func (h *Host) SetServiceTime(t uint32) {
	h.serviceTime = t
}

// AddPeer reserves or extends a peer slot with the provided address and state.
//
// AddPeer is exposed only as a test-construction helper so harnesses can bypass
// the connect handshake. Production code paths must use Connect or accept inbound
// connections; do not call AddPeer from non-test callers.
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

// Send queues one outbound packet for a connected peer channel.
func (h *Host) Send(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	if p == nil {
		return ErrNilPeer
	}
	if packet == nil {
		return ErrNilPacket
	}
	if p.State != core.PeerStateConnected && p.State != core.PeerStateDisconnectLater {
		return fmt.Errorf("%w: state %s", ErrPeerNotConnected, p.State)
	}
	if int(channelID) >= len(p.Channels) {
		return fmt.Errorf("%w: %d", ErrChannelOutOfRange, channelID)
	}
	if len(packet.Data) > int(h.config.MaximumPacketSize) {
		return fmt.Errorf("%w: %d bytes", ErrPacketTooLarge, len(packet.Data))
	}

	return h.queueOutgoingCommand(p, channelID, packet)
}

// Disconnect follows ENet's graceful or handshake-state disconnect path for p.
// ctx scopes any synchronous flush triggered by an unsequenced disconnect.
func (h *Host) Disconnect(ctx context.Context, p *peer.Peer, data uint32) error {
	if p == nil {
		return ErrNilPeer
	}
	if p.State == core.PeerStateDisconnecting ||
		p.State == core.PeerStateDisconnected ||
		p.State == core.PeerStateAcknowledgingDisconnect ||
		p.State == core.PeerStateZombie {
		return nil
	}

	h.clearPeerQueues(p)
	if p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater {
		if err := h.queueDisconnectCommand(p, data, protocol.CommandFlagAcknowledge); err != nil {
			return err
		}

		h.runtime[p].eventData = data
		h.runtime[p].disconnectLater = false
		p.State = core.PeerStateDisconnecting
		return nil
	}

	if err := h.queueDisconnectCommand(p, data, protocol.CommandFlagUnsequenced); err != nil {
		return err
	}
	if err := h.Flush(ctx); err != nil {
		return err
	}
	h.resetPeer(p)
	return nil
}

// DisconnectNow force-flushes an unsequenced disconnect and resets the peer locally.
// ctx scopes the synchronous flush of the disconnect command.
func (h *Host) DisconnectNow(ctx context.Context, p *peer.Peer, data uint32) error {
	if p == nil {
		return ErrNilPeer
	}
	if p.State == core.PeerStateDisconnected {
		return nil
	}
	if p.State != core.PeerStateZombie && p.State != core.PeerStateDisconnecting {
		h.clearPeerQueues(p)
		if err := h.queueDisconnectCommand(p, data, protocol.CommandFlagUnsequenced); err != nil {
			return err
		}
		if err := h.Flush(ctx); err != nil {
			return err
		}
	}

	h.resetPeer(p)
	return nil
}

func (h *Host) queueDisconnectCommand(p *peer.Peer, data uint32, flags protocol.CommandFlag) error {
	command := &peer.OutgoingCommand{
		Command: peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandDisconnect,
				ChannelID: 0xFF,
				Flags:     flags,
			},
			Payload: &protocol.Disconnect{
				Data: data,
			},
		},
	}
	if flags&protocol.CommandFlagUnsequenced == 0 {
		return h.setupAndQueueOutgoingCommand(p, command)
	}

	p.OutgoingReliableSequenceNumber++
	command.ReliableSequenceNumber = p.OutgoingReliableSequenceNumber
	command.UnreliableSequenceNumber = 0
	command.SendAttempts = 0
	command.SentTime = 0
	command.RoundTripTimeout = 0
	p.OutgoingDataTotal += checkedUint32FromInt(commandWireSize(command))
	h.totalQueued++
	command.QueueTime = h.totalQueued
	command.Command.Header.ReliableSequenceNumber = command.ReliableSequenceNumber
	applyOutgoingHeader(command.Command.Payload, command.Command.Header)
	p.OutgoingCommands.PushBack(command)
	return nil
}

// DisconnectLater defers disconnect until the peer's outbound reliable work drains.
func (h *Host) DisconnectLater(ctx context.Context, p *peer.Peer, data uint32) error {
	if p == nil {
		return ErrNilPeer
	}
	if (p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater) && h.hasOutgoingCommands(p) {
		h.runtime[p].eventData = data
		h.runtime[p].disconnectLater = true
		p.State = core.PeerStateDisconnectLater
		return nil
	}

	return h.Disconnect(ctx, p, data)
}

// Reset immediately drops all local state for p without a wire notification.
func (h *Host) Reset(p *peer.Peer) {
	if p == nil {
		return
	}

	h.resetPeer(p)
}

// Connect allocates an outbound peer and queues an ENet connect command.
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
		return nil, ErrNoFreePeerSlot
	}

	p = h.configurePeer(p, index, addr, core.PeerStateConnecting)
	p.Channels = make([]*peer.Channel, requestedChannels)
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

	h.logger.Debug(
		"peer connect queued",
		"peer_id", p.IncomingPeerID,
		"addr", addr.AddrPort(),
		"channel_count", requestedChannels,
		"connect_id", p.ConnectID,
	)

	return p, nil
}

func (h *Host) newPeerSlot(index int) *peer.Peer {
	p := &peer.Peer{}
	h.initializePeer(p, index, core.Address{}, core.PeerStateDisconnected, protocolMaximumPeerID, 0xFF, 0xFF)
	h.runtime[p] = defaultPeerRuntime()
	return p
}

func (h *Host) configurePeer(p *peer.Peer, index int, addr core.Address, state core.PeerState) *peer.Peer {
	channels := make([]*peer.Channel, h.config.ChannelLimit)
	for i := range channels {
		channels[i] = peer.NewChannel()
	}

	outgoingPeerID := checkedUint16FromInt(index + 1)
	if state == core.PeerStateConnecting {
		outgoingPeerID = protocolMaximumPeerID
	}

	h.initializePeer(p, index, addr, state, outgoingPeerID, 0xFF, peerSessionIDForIndex(index, h.sessionIDSeed))
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
		IncomingPeerID:               checkedUint16FromInt(index),
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

// randomConnectIDSeed seeds the per-host connect ID counter so peers can
// disambiguate stale datagrams across host restarts. crypto/rand is preferred;
// if entropy is unavailable (e.g. a locked-down sandbox) it falls back to
// nanosecond wall time so the counter still varies between consecutive starts.
func randomConnectIDSeed(logger *slog.Logger) uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		core.ComponentLogger(logger, "engine").Warn(
			"connect id seed entropy unavailable, falling back to wall time",
			"err", err,
		)
		return uint32(time.Now().UnixNano()) //nolint:gosec // intentional truncation; only mixes entropy.
	}
	return binary.BigEndian.Uint32(b[:])
}

func randomSessionIDSeed(logger *slog.Logger) uint8 {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		core.ComponentLogger(logger, "engine").Warn(
			"session id seed entropy unavailable, falling back to wall time",
			"err", err,
		)
		return uint8(time.Now().UnixNano()) % 3 //nolint:gosec // intentional truncation; mod-3 mix.
	}
	return b[0] % 3
}

func checkedUint32FromInt(value int) uint32 {
	if value < 0 || uint64(value) > math.MaxUint32 {
		panic(fmt.Sprintf("engine: int value %d overflows uint32", value))
	}

	return uint32(value)
}

func checkedUint16FromInt(value int) uint16 {
	if value < 0 || value > math.MaxUint16 {
		panic(fmt.Sprintf("engine: int value %d overflows uint16", value))
	}

	return uint16(value)
}

func peerSessionIDForIndex(index int, seed uint8) uint8 {
	return [...]uint8{1, 2, 3}[(index+int(seed%3))%3]
}

func lowUint16FromUint32(value uint32) uint16 {
	var wire [4]byte
	binary.BigEndian.PutUint32(wire[:], value)
	return binary.BigEndian.Uint16(wire[2:])
}

func (h *Host) outboundWindowSize() uint32 {
	if h.outgoingBandwidth == 0 {
		return protocol.MaximumWindowSize
	}

	windowSize := (h.outgoingBandwidth / peerWindowSizeScale) * protocol.MinimumWindowSize
	return clampUint32(windowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize)
}

// negotiatedPeerWindowSize replicates the C ENet `peer->windowSize` derivation in
// enet_protocol_handle_connect (enet.h:1934-1945): MAX when one side advertises
// zero, MIN when both have non-zero caps, clamped to the protocol window range.
func negotiatedPeerWindowSize(hostOutgoing, peerIncoming uint32) uint32 {
	var windowSize uint32
	switch {
	case hostOutgoing == 0 && peerIncoming == 0:
		windowSize = protocol.MaximumWindowSize
	case hostOutgoing == 0 || peerIncoming == 0:
		windowSize = (maxUint32(hostOutgoing, peerIncoming) / peerWindowSizeScale) * protocol.MinimumWindowSize
	default:
		windowSize = (minUint32(hostOutgoing, peerIncoming) / peerWindowSizeScale) * protocol.MinimumWindowSize
	}
	return clampUint32(windowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize)
}

// verifyConnectWindowSize replicates the C ENet `windowSize` derivation in
// enet_protocol_handle_connect (enet.h:1948-1962) used to populate the
// VerifyConnect command's window size before MIN-clamping against the peer's
// requested window.
func verifyConnectWindowSize(hostIncoming uint32) uint32 {
	if hostIncoming == 0 {
		return protocol.MaximumWindowSize
	}
	return (hostIncoming / peerWindowSizeScale) * protocol.MinimumWindowSize
}

func (h *Host) hasOutgoingCommands(p *peer.Peer) bool {
	return p.OutgoingCommands.Len() > 0 || p.OutgoingSendReliableCommands.Len() > 0 || p.SentReliableCommands.Len() > 0
}

// BandwidthLimit updates the host bandwidth caps and schedules peer recomputation.
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
