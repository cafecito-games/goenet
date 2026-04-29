package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
	"github.com/cafecito-games/goenet/internal/timeutil"
)

const (
	protocolMaximumPeerID               = protocol.MaximumPeerID
	peerReliableWindows                 = 16
	peerReliableWindowSize              = 0x1000
	peerFreeReliableWindows             = 8
	peerUnsequencedWindows              = 64
	peerUnsequencedWindowSize           = 1024
	peerFreeUnsequencedWindows          = 32
	protocolMaximumFragmentCount uint32 = 1024 * 1024
)

type inboundDisposition uint8

const (
	inboundReject inboundDisposition = iota
	inboundIgnore
	inboundAccept
)

// Event is the engine-level service event before public API translation.
type Event struct {
	Type      core.EventType
	Peer      *peer.Peer
	ChannelID uint8
	Data      uint32
	Packet    *core.Packet
}

type peerRuntime struct {
	eventData       uint32
	windowSize      uint32
	disconnectLater bool
}

func defaultPeerRuntime() *peerRuntime {
	return &peerRuntime{
		windowSize: protocol.MaximumWindowSize,
	}
}

// Service advances the engine state machine and returns the next visible event.
func (h *Host) Service(ctx context.Context, timeout uint32) (Event, error) {
	if h.intercepted != nil {
		event := *h.intercepted
		h.intercepted = nil
		return event, nil
	}
	if event, ok := h.dispatchEvent(); ok {
		return event, nil
	}
	if timeutil.Difference(h.serviceTime, h.bandwidthThrottleEpoch) >= defaultBandwidthThrottleInterval {
		h.bandwidthThrottle()
	}
	if event, ok := h.checkTimeouts(); ok {
		return event, nil
	}
	if err := h.Flush(ctx); err != nil {
		return Event{}, err
	}
	if err := h.receiveIncoming(ctx); err != nil {
		return Event{}, err
	}
	if h.intercepted != nil {
		event := *h.intercepted
		h.intercepted = nil
		return event, nil
	}
	if err := h.Flush(ctx); err != nil {
		return Event{}, err
	}
	if event, ok := h.dispatchEvent(); ok {
		return event, nil
	}
	h.serviceTime += timeout
	return Event{}, nil
}

func (h *Host) receiveIncoming(ctx context.Context) error {
	buf := make([]byte, h.config.MTU)
	for packets := 0; packets < 256; packets++ {
		n, addr, err := h.socket.ReadPacket(ctx, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := h.handleIncomingDatagram(buf[:n], addr); err != nil {
			return err
		}
	}

	return nil
}

func (h *Host) handleIncomingDatagram(payload []byte, addr core.Address) error {
	if h.config.Intercept != nil {
		decision, err := h.config.Intercept.Intercept(addr.AddrPort(), payload)
		if err != nil {
			return err
		}
		if decision.Result == core.InterceptResultConsume && decision.Event != nil {
			h.intercepted = &Event{
				Type:      decision.Event.Type,
				ChannelID: decision.Event.ChannelID,
				Data:      decision.Event.Data,
				Packet:    decision.Event.Packet,
			}
		}
		if decision.Result == core.InterceptResultConsume {
			return nil
		}
	}

	header, ok := parseHeader(payload)
	if !ok {
		return nil
	}

	protocolHeaderSize := protocolHeaderSizeWithoutSentTime
	if header.Flags&protocol.HeaderFlagSentTime != 0 {
		protocolHeaderSize = protocolHeaderSizeWithSentTime
	}
	offset := protocolHeaderSize
	if h.config.Checksum != nil {
		offset += 4
	}
	if offset > len(payload) {
		return nil
	}

	currentPeer, ok := h.lookupPeer(header, addr)
	if !ok {
		return nil
	}
	if currentPeer != nil {
		currentPeer.IncomingDataTotal += checkedUint32FromInt(len(payload))
	}

	workingPayload := payload
	if header.Flags&protocol.HeaderFlagCompressed != 0 {
		if h.config.Compressor == nil {
			return nil
		}
		outLimit := int(h.config.MTU) - offset
		if outLimit <= 0 {
			return nil
		}
		decompressed := make([]byte, outLimit)
		n, ok := decompressPayload(h.config.Compressor, payload[offset:], decompressed)
		if !ok {
			return nil
		}

		workingPayload = make([]byte, 0, offset+n)
		workingPayload = append(workingPayload, payload[:offset]...)
		workingPayload = append(workingPayload, decompressed[:n]...)
	} else if h.config.Checksum != nil {
		workingPayload = append([]byte(nil), payload...)
	}

	if h.config.Checksum != nil {
		checksumOffset := protocolHeaderSize
		desired := binary.LittleEndian.Uint32(workingPayload[checksumOffset : checksumOffset+4])
		binary.LittleEndian.PutUint32(workingPayload[checksumOffset:checksumOffset+4], incomingChecksumSeed(currentPeer))
		if h.config.Checksum.Checksum([]core.Buffer{{Data: workingPayload}}) != desired {
			return nil
		}
	}

	for offset < len(workingPayload) {
		command, used, ok := parseCommand(workingPayload[offset:])
		if !ok {
			return nil
		}
		offset += used

		if currentPeer == nil {
			if _, ok := command.(protocol.Connect); !ok || offset != len(workingPayload) {
				return nil
			}
		}

		disposition := h.handleIncomingCommand(header, &currentPeer, command, addr)
		if disposition == inboundReject {
			return nil
		}

		acknowledgeHeader := commandHeader(command)
		if disposition == inboundReject || acknowledgeHeader.Flags&protocol.CommandFlagAcknowledge == 0 || currentPeer == nil {
			continue
		}
		if header.Flags&protocol.HeaderFlagSentTime == 0 {
			return nil
		}

		h.queueAcknowledgement(currentPeer, acknowledgeHeader, header.SentTime)
	}

	return nil
}

func (h *Host) lookupPeer(header protocol.Header, addr core.Address) (*peer.Peer, bool) {
	if header.PeerID == protocol.MaximumPeerID {
		return nil, true
	}
	if int(header.PeerID) >= len(h.peers) {
		return nil, false
	}

	candidate := h.peers[int(header.PeerID)]
	if candidate == nil {
		return nil, false
	}
	if candidate.State == core.PeerStateDisconnected || candidate.State == core.PeerStateZombie {
		return nil, false
	}
	if candidate.Address != addr {
		return nil, false
	}
	if candidate.OutgoingPeerID < protocol.MaximumPeerID && header.SessionID != candidate.IncomingSessionID {
		return nil, false
	}

	return candidate, true
}

func (h *Host) handleIncomingCommand(
	packetHeader protocol.Header,
	currentPeer **peer.Peer,
	command protocol.PacketCommand,
	addr core.Address,
) inboundDisposition {
	switch cmd := command.(type) {
	case protocol.Acknowledge:
		if *currentPeer == nil {
			return inboundReject
		}
		if h.handleAcknowledge(*currentPeer, cmd) {
			return inboundAccept
		}
		return inboundReject
	case protocol.Connect:
		if *currentPeer != nil {
			return inboundReject
		}
		connectedPeer := h.handleConnect(addr, cmd)
		if connectedPeer == nil {
			return inboundReject
		}
		*currentPeer = connectedPeer
		return inboundAccept
	case protocol.VerifyConnect:
		if *currentPeer == nil {
			return inboundReject
		}
		return h.handleVerifyConnect(*currentPeer, cmd)
	case protocol.Disconnect:
		if *currentPeer == nil {
			return inboundReject
		}
		return h.handleDisconnect(*currentPeer, cmd)
	case protocol.Ping:
		if *currentPeer == nil {
			return inboundReject
		}
		return inboundAccept
	case protocol.SendReliable:
		if *currentPeer == nil {
			return inboundReject
		}
		return h.handleSendReliable(*currentPeer, cmd)
	case protocol.SendUnreliable:
		if *currentPeer == nil {
			return inboundReject
		}
		return h.handleSendUnreliable(*currentPeer, cmd)
	case protocol.SendUnsequenced:
		if *currentPeer == nil {
			return inboundReject
		}
		return h.handleSendUnsequenced(*currentPeer, cmd)
	case protocol.SendFragment:
		if *currentPeer == nil {
			return inboundReject
		}
		if cmd.Header.Command == protocol.CommandSendUnreliableFragment {
			return h.handleSendUnreliableFragment(*currentPeer, cmd)
		}
		return h.handleSendFragment(*currentPeer, cmd)
	case protocol.BandwidthLimit:
		if *currentPeer == nil {
			return inboundReject
		}
		if h.handleBandwidthLimit(*currentPeer, cmd) {
			return inboundAccept
		}
		return inboundReject
	case protocol.ThrottleConfigure:
		if *currentPeer == nil {
			return inboundReject
		}
		if h.handleThrottleConfigure(*currentPeer, cmd) {
			return inboundAccept
		}
		return inboundReject
	default:
		_ = packetHeader
		return inboundReject
	}
}

func (h *Host) handleConnect(addr core.Address, command protocol.Connect) *peer.Peer {
	if command.ChannelCount < protocol.MinimumChannelCount || command.ChannelCount > protocol.MaximumChannelCount {
		return nil
	}

	var selected *peer.Peer
	for _, candidate := range h.peers {
		if candidate != nil && candidate.State == core.PeerStateDisconnected {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return nil
	}

	channelCount := command.ChannelCount
	if channelCount > uint32(h.config.ChannelLimit) {
		channelCount = uint32(h.config.ChannelLimit)
	}

	selected.Address = addr
	selected.State = core.PeerStateAcknowledgingConnect
	selected.ConnectID = command.ConnectID
	selected.OutgoingPeerID = command.OutgoingPeerID
	selected.MTU = minUint32(h.config.MTU, clampUint32(command.MTU, protocol.MinimumMTU, protocol.MaximumMTU))
	selected.IncomingBandwidth = command.IncomingBandwidth
	selected.OutgoingBandwidth = command.OutgoingBandwidth
	selected.Channels = make([]peer.Channel, channelCount)
	for i := range selected.Channels {
		selected.Channels[i] = peer.NewChannel()
	}

	incomingSessionID := nextSessionID(selected.OutgoingSessionID, command.IncomingSessionID)
	outgoingSessionID := nextSessionID(selected.IncomingSessionID, command.OutgoingSessionID)
	selected.OutgoingSessionID = incomingSessionID
	selected.IncomingSessionID = outgoingSessionID

	runtime := h.runtime[selected]
	runtime.eventData = command.Data
	runtime.windowSize = clampUint32(command.WindowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize)
	selected.PacketThrottleInterval = command.PacketThrottleInterval
	selected.PacketThrottleAcceleration = command.PacketThrottleAcceleration
	selected.PacketThrottleDeceleration = command.PacketThrottleDeceleration

	verify := protocol.VerifyConnect{
		Header: protocol.CommandHeader{
			ChannelID: 0xFF,
		},
		OutgoingPeerID:             selected.IncomingPeerID,
		IncomingSessionID:          incomingSessionID,
		OutgoingSessionID:          outgoingSessionID,
		MTU:                        selected.MTU,
		WindowSize:                 runtime.windowSize,
		ChannelCount:               channelCount,
		IncomingBandwidth:          0,
		OutgoingBandwidth:          0,
		PacketThrottleInterval:     selected.PacketThrottleInterval,
		PacketThrottleAcceleration: selected.PacketThrottleAcceleration,
		PacketThrottleDeceleration: selected.PacketThrottleDeceleration,
		ConnectID:                  selected.ConnectID,
	}
	if err := h.queueOutgoingControlCommand(selected, peer.Command{
		Header: peer.Header{
			Command:   protocol.CommandVerifyConnect,
			ChannelID: 0xFF,
			Flags:     protocol.CommandFlagAcknowledge,
		},
		Payload: &verify,
	}); err != nil {
		return nil
	}

	return selected
}

func (h *Host) handleVerifyConnect(p *peer.Peer, command protocol.VerifyConnect) inboundDisposition {
	if p.State != core.PeerStateConnecting {
		return inboundIgnore
	}

	runtime := h.runtime[p]
	if command.ChannelCount < protocol.MinimumChannelCount || command.ChannelCount > protocol.MaximumChannelCount {
		p.State = core.PeerStateZombie
		h.enqueuePeerDispatch(p)
		return inboundReject
	}
	if command.PacketThrottleInterval != p.PacketThrottleInterval ||
		command.PacketThrottleAcceleration != p.PacketThrottleAcceleration ||
		command.PacketThrottleDeceleration != p.PacketThrottleDeceleration ||
		command.ConnectID != p.ConnectID {
		p.State = core.PeerStateZombie
		h.enqueuePeerDispatch(p)
		return inboundReject
	}

	h.removeSentReliableCommand(p, 1, 0xFF)
	if int(command.ChannelCount) < len(p.Channels) {
		p.Channels = p.Channels[:int(command.ChannelCount)]
	}

	p.OutgoingPeerID = command.OutgoingPeerID
	p.IncomingSessionID = command.IncomingSessionID
	p.OutgoingSessionID = command.OutgoingSessionID
	p.MTU = minUint32(p.MTU, clampUint32(command.MTU, protocol.MinimumMTU, protocol.MaximumMTU))
	p.IncomingBandwidth = command.IncomingBandwidth
	p.OutgoingBandwidth = command.OutgoingBandwidth
	runtime.windowSize = minUint32(runtime.windowSize, clampUint32(command.WindowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize))

	h.notifyConnect(p)
	return inboundAccept
}

func (h *Host) handleAcknowledge(p *peer.Peer, command protocol.Acknowledge) bool {
	if p.State == core.PeerStateDisconnected || p.State == core.PeerStateZombie {
		return true
	}

	receivedSentTime := uint32(command.ReceivedSentTime) | (h.serviceTime & 0xFFFF0000)
	if (receivedSentTime & 0x8000) > (h.serviceTime & 0x8000) {
		receivedSentTime -= 0x10000
	}
	if timeutil.Less(h.serviceTime, receivedSentTime) {
		return false
	}

	roundTripTime := maxUint32(timeutil.Difference(h.serviceTime, receivedSentTime), 1)
	if p.LastReceiveTime > 0 {
		peerThrottle(p, roundTripTime)
		p.RoundTripTimeVariance -= p.RoundTripTimeVariance / 4
		if roundTripTime >= p.RoundTripTime {
			diff := roundTripTime - p.RoundTripTime
			p.RoundTripTimeVariance += diff / 4
			p.RoundTripTime += diff / 8
		} else {
			diff := p.RoundTripTime - roundTripTime
			p.RoundTripTimeVariance += diff / 4
			p.RoundTripTime -= diff / 8
		}
	} else {
		p.RoundTripTime = roundTripTime
		p.RoundTripTimeVariance = (roundTripTime + 1) / 2
	}

	if p.RoundTripTime < p.LowestRoundTripTime {
		p.LowestRoundTripTime = p.RoundTripTime
	}
	if p.RoundTripTimeVariance > p.HighestRoundTripTimeVariance {
		p.HighestRoundTripTimeVariance = p.RoundTripTimeVariance
	}
	if p.PacketThrottleEpoch == 0 || timeutil.Difference(h.serviceTime, p.PacketThrottleEpoch) >= p.PacketThrottleInterval {
		p.LastRoundTripTime = p.LowestRoundTripTime
		p.LastRoundTripTimeVariance = maxUint32(p.HighestRoundTripTimeVariance, 1)
		p.LowestRoundTripTime = p.RoundTripTime
		p.HighestRoundTripTimeVariance = p.RoundTripTimeVariance
		p.PacketThrottleEpoch = h.serviceTime
	}

	p.LastReceiveTime = maxUint32(h.serviceTime, 1)
	p.EarliestTimeout = 0

	commandNumber := h.removeSentReliableCommand(p, command.ReceivedReliableSequenceNumber, command.Header.ChannelID)
	if p.State == core.PeerStateAcknowledgingConnect {
		if commandNumber != protocol.CommandVerifyConnect {
			return false
		}
		h.notifyConnect(p)
	} else if h.runtime[p].disconnectLater && p.State == core.PeerStateDisconnectLater && !h.hasOutgoingCommands(p) {
		if err := h.Disconnect(p, h.runtime[p].eventData); err != nil {
			return false
		}
	}

	return true
}

func (h *Host) handleDisconnect(p *peer.Peer, command protocol.Disconnect) inboundDisposition {
	if p.State == core.PeerStateDisconnected || p.State == core.PeerStateZombie || p.State == core.PeerStateAcknowledgingDisconnect {
		return inboundIgnore
	}

	h.clearPeerQueues(p)

	switch p.State {
	case core.PeerStateConnectionSucceeded, core.PeerStateDisconnecting, core.PeerStateConnecting:
		h.runtime[p].eventData = command.Data
		p.State = core.PeerStateZombie
		h.enqueuePeerDispatch(p)
	case core.PeerStateConnected, core.PeerStateDisconnectLater:
		h.runtime[p].eventData = command.Data
		if command.Header.Flags&protocol.CommandFlagAcknowledge != 0 {
			p.State = core.PeerStateAcknowledgingDisconnect
		} else {
			p.State = core.PeerStateZombie
			h.enqueuePeerDispatch(p)
		}
	default:
		h.resetPeer(p)
	}

	return inboundAccept
}

func (h *Host) handleSendReliable(p *peer.Peer, command protocol.SendReliable) inboundDisposition {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return inboundReject
	}
	if len(command.Data) > int(h.config.MaximumPacketSize) {
		return inboundReject
	}

	packet := &core.Packet{
		Data:  append([]byte(nil), command.Data...),
		Flags: core.PacketFlagReliable,
	}
	cmd := &peer.IncomingCommand{
		ReliableSequenceNumber: command.Header.ReliableSequenceNumber,
		Command: peer.Command{
			Header: peer.Header{
				Command:                protocol.CommandSendReliable,
				ChannelID:              command.Header.ChannelID,
				Flags:                  command.Header.Flags,
				ReliableSequenceNumber: command.Header.ReliableSequenceNumber,
			},
		},
		Packet: packet,
	}

	return h.queueReliableIncomingCommand(p, cmd)
}

func (h *Host) handleSendUnreliable(p *peer.Peer, command protocol.SendUnreliable) inboundDisposition {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return inboundReject
	}
	if len(command.Data) > int(h.config.MaximumPacketSize) {
		return inboundReject
	}

	packet := &core.Packet{Data: append([]byte(nil), command.Data...)}
	cmd := &peer.IncomingCommand{
		ReliableSequenceNumber:   command.Header.ReliableSequenceNumber,
		UnreliableSequenceNumber: command.UnreliableSequenceNumber,
		Command: peer.Command{
			Header: peer.Header{
				Command:                protocol.CommandSendUnreliable,
				ChannelID:              command.Header.ChannelID,
				Flags:                  command.Header.Flags,
				ReliableSequenceNumber: command.Header.ReliableSequenceNumber,
			},
		},
		Packet: packet,
	}

	return h.queueUnreliableIncomingCommand(p, cmd)
}

func (h *Host) handleSendUnsequenced(p *peer.Peer, command protocol.SendUnsequenced) inboundDisposition {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return inboundReject
	}
	if len(command.Data) > int(h.config.MaximumPacketSize) {
		return inboundReject
	}
	if p.State == core.PeerStateDisconnectLater {
		return inboundIgnore
	}

	unsequencedGroup := uint32(command.UnsequencedGroup)
	index := unsequencedGroup % peerUnsequencedWindowSize
	if unsequencedGroup < uint32(p.IncomingUnsequencedGroup) {
		unsequencedGroup += 0x10000
	}
	if unsequencedGroup >= uint32(p.IncomingUnsequencedGroup)+peerFreeUnsequencedWindows*peerUnsequencedWindowSize {
		return inboundIgnore
	}

	groupBase := lowUint16FromUint32(unsequencedGroup - index)
	if groupBase != p.IncomingUnsequencedGroup {
		p.IncomingUnsequencedGroup = groupBase
		for i := range p.UnsequencedWindow {
			p.UnsequencedWindow[i] = 0
		}
	} else if p.UnsequencedWindow[index/32]&(uint32(1)<<(index%32)) != 0 {
		return inboundIgnore
	}
	if !p.CanQueueWaitingData(checkedUint32FromInt(len(command.Data)), h.config.MaximumWaitingData) {
		return inboundReject
	}

	cmd := &peer.IncomingCommand{
		Command: peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandSendUnsequenced,
				ChannelID: command.Header.ChannelID,
				Flags:     command.Header.Flags,
			},
		},
		Packet: &core.Packet{
			Data:  append([]byte(nil), command.Data...),
			Flags: core.PacketFlagUnsequenced,
		},
	}
	p.AddWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)))
	p.QueueDispatchedCommand(cmd)
	p.UnsequencedWindow[index/32] |= uint32(1) << (index % 32)
	h.enqueuePeerDispatch(p)
	return inboundAccept
}

func (h *Host) handleSendFragment(p *peer.Peer, command protocol.SendFragment) inboundDisposition {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return inboundReject
	}
	if p.State == core.PeerStateDisconnectLater {
		return inboundIgnore
	}

	channel := &p.Channels[command.Header.ChannelID]
	startSequence := command.StartSequenceNumber
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, startSequence) {
		return inboundIgnore
	}
	if command.FragmentCount == 0 ||
		command.FragmentCount > protocolMaximumFragmentCount ||
		command.FragmentNumber >= command.FragmentCount ||
		command.TotalLength > h.config.MaximumPacketSize ||
		command.TotalLength < command.FragmentCount ||
		command.FragmentOffset >= command.TotalLength ||
		len(command.Data) == 0 ||
		checkedUint32FromInt(len(command.Data)) > command.TotalLength-command.FragmentOffset {
		return inboundReject
	}

	start := h.findReliableFragmentCommand(channel, startSequence)
	if start == nil {
		if !p.CanQueueWaitingData(command.TotalLength, h.config.MaximumWaitingData) {
			return inboundReject
		}
		start = &peer.IncomingCommand{
			ReliableSequenceNumber: startSequence,
			Command: peer.Command{
				Header: peer.Header{
					Command:                protocol.CommandSendFragment,
					ChannelID:              command.Header.ChannelID,
					Flags:                  command.Header.Flags,
					ReliableSequenceNumber: startSequence,
				},
			},
			Packet: &core.Packet{
				Data:  make([]byte, int(command.TotalLength)),
				Flags: core.PacketFlagReliable,
			},
		}
		start.SetFragmentCount(command.FragmentCount)
		p.AddWaitingData(command.TotalLength)
		channel.InsertIncomingReliableOrdered(start)
	} else if start.FragmentCount != command.FragmentCount || checkedUint32FromInt(len(start.Packet.Data)) != command.TotalLength {
		return inboundReject
	}

	if !start.MarkFragmentReceived(command.FragmentNumber) {
		return inboundIgnore
	}
	copy(start.Packet.Data[int(command.FragmentOffset):int(command.FragmentOffset)+len(command.Data)], command.Data)
	if start.IsComplete() {
		h.dispatchReliableCommands(p, channel)
	}
	return inboundAccept
}

func (h *Host) handleSendUnreliableFragment(p *peer.Peer, command protocol.SendFragment) inboundDisposition {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return inboundReject
	}
	if p.State == core.PeerStateDisconnectLater {
		return inboundIgnore
	}

	channel := &p.Channels[command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, command.Header.ReliableSequenceNumber) {
		return inboundIgnore
	}
	if command.Header.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber &&
		command.StartSequenceNumber <= channel.IncomingUnreliableSequenceNumber {
		return inboundIgnore
	}
	if command.FragmentCount == 0 ||
		command.FragmentCount > protocolMaximumFragmentCount ||
		command.FragmentNumber >= command.FragmentCount ||
		command.TotalLength > h.config.MaximumPacketSize ||
		command.TotalLength < command.FragmentCount ||
		command.FragmentOffset >= command.TotalLength ||
		len(command.Data) == 0 ||
		checkedUint32FromInt(len(command.Data)) > command.TotalLength-command.FragmentOffset {
		return inboundReject
	}

	start := h.findUnreliableFragmentCommand(channel, command.Header.ReliableSequenceNumber, command.StartSequenceNumber)
	if start == nil {
		if !p.CanQueueWaitingData(command.TotalLength, h.config.MaximumWaitingData) {
			return inboundReject
		}
		start = &peer.IncomingCommand{
			ReliableSequenceNumber:   command.Header.ReliableSequenceNumber,
			UnreliableSequenceNumber: command.StartSequenceNumber,
			Command: peer.Command{
				Header: peer.Header{
					Command:                protocol.CommandSendUnreliableFragment,
					ChannelID:              command.Header.ChannelID,
					Flags:                  command.Header.Flags,
					ReliableSequenceNumber: command.Header.ReliableSequenceNumber,
				},
			},
			Packet: &core.Packet{Data: make([]byte, int(command.TotalLength))},
		}
		start.SetFragmentCount(command.FragmentCount)
		p.AddWaitingData(command.TotalLength)
		channel.InsertIncomingUnreliableOrdered(start)
	} else if start.FragmentCount != command.FragmentCount || checkedUint32FromInt(len(start.Packet.Data)) != command.TotalLength {
		return inboundReject
	}

	if !start.MarkFragmentReceived(command.FragmentNumber) {
		return inboundIgnore
	}
	copy(start.Packet.Data[int(command.FragmentOffset):int(command.FragmentOffset)+len(command.Data)], command.Data)
	if start.IsComplete() {
		h.dispatchUnreliableCommands(p, channel)
	}
	return inboundAccept
}

func (h *Host) handleBandwidthLimit(p *peer.Peer, command protocol.BandwidthLimit) bool {
	if p.State != core.PeerStateConnected && p.State != core.PeerStateDisconnectLater {
		return false
	}
	p.IncomingBandwidth = command.IncomingBandwidth
	p.OutgoingBandwidth = command.OutgoingBandwidth
	h.runtime[p].windowSize = protocol.MaximumWindowSize
	return true
}

func (h *Host) handleThrottleConfigure(p *peer.Peer, command protocol.ThrottleConfigure) bool {
	if p.State != core.PeerStateConnected && p.State != core.PeerStateDisconnectLater {
		return false
	}
	p.PacketThrottleInterval = command.PacketThrottleInterval
	p.PacketThrottleAcceleration = command.PacketThrottleAcceleration
	p.PacketThrottleDeceleration = command.PacketThrottleDeceleration
	return true
}

func (h *Host) queueReliableIncomingCommand(p *peer.Peer, cmd *peer.IncomingCommand) inboundDisposition {
	if p.State == core.PeerStateDisconnectLater {
		return inboundIgnore
	}
	channel := &p.Channels[cmd.Command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
		return inboundIgnore
	}
	if cmd.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber {
		return inboundIgnore
	}
	for elem := channel.IncomingReliableCommands.Back(); elem != nil; elem = elem.Prev() {
		existing := elem.Value()
		if existing.ReliableSequenceNumber == cmd.ReliableSequenceNumber {
			return inboundIgnore
		}
		if sequenceDistance(channel.IncomingReliableSequenceNumber, existing.ReliableSequenceNumber) <
			sequenceDistance(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
			break
		}
	}
	if !p.CanQueueWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)), h.config.MaximumWaitingData) {
		return inboundReject
	}

	p.AddWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)))
	channel.InsertIncomingReliableOrdered(cmd)
	h.dispatchReliableCommands(p, channel)
	return inboundAccept
}

func (h *Host) queueUnreliableIncomingCommand(p *peer.Peer, cmd *peer.IncomingCommand) inboundDisposition {
	if p.State == core.PeerStateDisconnectLater {
		return inboundIgnore
	}
	channel := &p.Channels[cmd.Command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
		return inboundIgnore
	}
	if cmd.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber &&
		cmd.UnreliableSequenceNumber <= channel.IncomingUnreliableSequenceNumber {
		return inboundIgnore
	}
	for elem := channel.IncomingUnreliableCommands.Back(); elem != nil; elem = elem.Prev() {
		existing := elem.Value()
		if existing.ReliableSequenceNumber < cmd.ReliableSequenceNumber {
			break
		}
		if existing.ReliableSequenceNumber > cmd.ReliableSequenceNumber {
			continue
		}
		if existing.UnreliableSequenceNumber == cmd.UnreliableSequenceNumber {
			return inboundIgnore
		}
		if existing.UnreliableSequenceNumber < cmd.UnreliableSequenceNumber {
			break
		}
	}
	if !p.CanQueueWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)), h.config.MaximumWaitingData) {
		return inboundReject
	}

	p.AddWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)))
	channel.InsertIncomingUnreliableOrdered(cmd)
	h.dispatchUnreliableCommands(p, channel)
	return inboundAccept
}

func (h *Host) dispatchReliableCommands(p *peer.Peer, channel *peer.Channel) {
	moved := false
	for {
		front := channel.IncomingReliableCommands.Front()
		if front == nil {
			break
		}
		cmd := front.Value()
		if !cmd.IsComplete() || cmd.ReliableSequenceNumber != channel.IncomingReliableSequenceNumber+1 {
			break
		}

		channel.IncomingReliableCommands.Remove(front)
		channel.MarkIncomingReliableDispatched(cmd)
		p.QueueDispatchedCommand(cmd)
		moved = true
	}
	if moved {
		h.enqueuePeerDispatch(p)
	}
	if channel.IncomingUnreliableCommands.Len() > 0 {
		h.dispatchUnreliableCommands(p, channel)
	}
}

func (h *Host) dispatchUnreliableCommands(p *peer.Peer, channel *peer.Channel) {
	moved := false
	for {
		front := channel.IncomingUnreliableCommands.Front()
		if front == nil {
			break
		}
		cmd := front.Value()
		if cmd.Command.Header.Command == protocol.CommandSendUnsequenced {
			channel.IncomingUnreliableCommands.Remove(front)
			p.QueueDispatchedCommand(cmd)
			moved = true
			continue
		}

		if shouldDropUnreliable(channel, cmd) {
			channel.IncomingUnreliableCommands.Remove(front)
			if cmd.Packet != nil {
				p.ReleaseWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)))
			}
			continue
		}
		if cmd.ReliableSequenceNumber != channel.IncomingReliableSequenceNumber || !cmd.IsComplete() {
			break
		}
		channel.MarkIncomingUnreliableDispatched(cmd)
		channel.IncomingUnreliableCommands.Remove(front)
		p.QueueDispatchedCommand(cmd)
		moved = true
	}
	if moved {
		h.enqueuePeerDispatch(p)
	}
}

func shouldDropUnreliable(channel *peer.Channel, cmd *peer.IncomingCommand) bool {
	reliableWindow := cmd.ReliableSequenceNumber / peerReliableWindowSize
	currentWindow := channel.IncomingReliableSequenceNumber / peerReliableWindowSize
	if cmd.ReliableSequenceNumber < channel.IncomingReliableSequenceNumber {
		reliableWindow += peerReliableWindows
	}
	if reliableWindow < currentWindow || reliableWindow >= currentWindow+peerFreeReliableWindows-1 {
		return true
	}
	if cmd.ReliableSequenceNumber != channel.IncomingReliableSequenceNumber {
		return false
	}
	return cmd.UnreliableSequenceNumber <= channel.IncomingUnreliableSequenceNumber
}

func (h *Host) dispatchEvent() (Event, bool) {
	for len(h.dispatchQ) > 0 {
		p := h.dispatchQ[0]
		h.dispatchQ = h.dispatchQ[1:]
		delete(h.dispatchSet, p)

		switch p.State {
		case core.PeerStateConnectionPending, core.PeerStateConnectionSucceeded:
			p.State = core.PeerStateConnected
			return Event{
				Type: core.EventConnect,
				Peer: p,
				Data: h.runtime[p].eventData,
			}, true
		case core.PeerStateConnected:
			cmd := p.PopDispatchedCommand()
			if cmd == nil || cmd.Packet == nil {
				continue
			}
			p.ReleaseWaitingData(checkedUint32FromInt(len(cmd.Packet.Data)))
			if p.DispatchedCommands.Len() > 0 {
				h.enqueuePeerDispatch(p)
			}
			return Event{
				Type:      core.EventReceive,
				Peer:      p,
				ChannelID: cmd.Command.Header.ChannelID,
				Packet:    cmd.Packet,
			}, true
		case core.PeerStateZombie:
			data := h.runtime[p].eventData
			h.resetPeer(p)
			return Event{
				Type: core.EventDisconnect,
				Peer: p,
				Data: data,
			}, true
		}
	}

	return Event{}, false
}

func parseHeader(payload []byte) (protocol.Header, bool) {
	header, err := protocol.ParseHeader(payload)
	return header, err == nil
}

func parseCommand(payload []byte) (protocol.PacketCommand, int, bool) {
	command, _, used, err := protocol.ParseCommand(payload)
	return command, used, err == nil
}

func decompressPayload(compressor core.Compressor, in, out []byte) (int, bool) {
	n, err := compressor.Decompress(in, out)
	return n, err == nil
}

func (h *Host) enqueuePeerDispatch(p *peer.Peer) {
	if _, ok := h.dispatchSet[p]; ok {
		return
	}
	h.dispatchSet[p] = struct{}{}
	h.dispatchQ = append(h.dispatchQ, p)
}

func (h *Host) removePeerDispatch(p *peer.Peer) {
	delete(h.dispatchSet, p)
	filtered := h.dispatchQ[:0]
	for _, queued := range h.dispatchQ {
		if queued != p {
			filtered = append(filtered, queued)
		}
	}
	h.dispatchQ = filtered
}

func (h *Host) notifyConnect(p *peer.Peer) {
	if p.State == core.PeerStateConnecting {
		p.State = core.PeerStateConnectionSucceeded
	} else {
		p.State = core.PeerStateConnectionPending
	}
	h.enqueuePeerDispatch(p)
}

func (h *Host) clearPeerQueues(p *peer.Peer) {
	state := p.State
	incomingPeerID := p.IncomingPeerID
	outgoingPeerID := p.OutgoingPeerID
	connectID := p.ConnectID
	outgoingSessionID := p.OutgoingSessionID
	incomingSessionID := p.IncomingSessionID
	mtu := p.MTU
	address := p.Address
	incomingBandwidth := p.IncomingBandwidth
	outgoingBandwidth := p.OutgoingBandwidth
	incomingDataTotal := p.IncomingDataTotal
	outgoingDataTotal := p.OutgoingDataTotal
	incomingBandwidthThrottleEpoch := p.IncomingBandwidthThrottleEpoch
	outgoingBandwidthThrottleEpoch := p.OutgoingBandwidthThrottleEpoch
	lastSendTime := p.LastSendTime
	lastReceiveTime := p.LastReceiveTime
	nextTimeout := p.NextTimeout
	earliestTimeout := p.EarliestTimeout
	packetsLost := p.PacketsLost
	totalPacketsLost := p.TotalPacketsLost
	packetThrottle := p.PacketThrottle
	packetThrottleLimit := p.PacketThrottleLimit
	packetThrottleCounter := p.PacketThrottleCounter
	packetThrottleEpoch := p.PacketThrottleEpoch
	packetThrottleAcceleration := p.PacketThrottleAcceleration
	packetThrottleDeceleration := p.PacketThrottleDeceleration
	packetThrottleInterval := p.PacketThrottleInterval
	timeoutLimit := p.TimeoutLimit
	timeoutMinimum := p.TimeoutMinimum
	timeoutMaximum := p.TimeoutMaximum
	lastRoundTripTime := p.LastRoundTripTime
	lowestRoundTripTime := p.LowestRoundTripTime
	lastRoundTripTimeVariance := p.LastRoundTripTimeVariance
	highestRoundTripTimeVariance := p.HighestRoundTripTimeVariance
	roundTripTime := p.RoundTripTime
	roundTripTimeVariance := p.RoundTripTimeVariance
	reliableDataInTransit := p.ReliableDataInTransit
	outgoingReliableSequenceNumber := p.OutgoingReliableSequenceNumber
	outgoingUnsequencedGroup := p.OutgoingUnsequencedGroup
	incomingUnsequencedGroup := p.IncomingUnsequencedGroup
	unsequencedWindow := p.UnsequencedWindow

	h.removePeerDispatch(p)
	*p = peer.Peer{
		OutgoingReliableSequenceNumber: outgoingReliableSequenceNumber,
		OutgoingUnsequencedGroup:       outgoingUnsequencedGroup,
		OutgoingPeerID:                 outgoingPeerID,
		IncomingPeerID:                 incomingPeerID,
		ConnectID:                      connectID,
		OutgoingSessionID:              outgoingSessionID,
		IncomingSessionID:              incomingSessionID,
		MTU:                            mtu,
		Address:                        address,
		State:                          state,
		IncomingBandwidth:              incomingBandwidth,
		OutgoingBandwidth:              outgoingBandwidth,
		IncomingDataTotal:              incomingDataTotal,
		OutgoingDataTotal:              outgoingDataTotal,
		IncomingBandwidthThrottleEpoch: incomingBandwidthThrottleEpoch,
		OutgoingBandwidthThrottleEpoch: outgoingBandwidthThrottleEpoch,
		LastSendTime:                   lastSendTime,
		LastReceiveTime:                lastReceiveTime,
		NextTimeout:                    nextTimeout,
		EarliestTimeout:                earliestTimeout,
		PacketsLost:                    packetsLost,
		TotalPacketsLost:               totalPacketsLost,
		PacketThrottle:                 packetThrottle,
		PacketThrottleLimit:            packetThrottleLimit,
		PacketThrottleCounter:          packetThrottleCounter,
		PacketThrottleEpoch:            packetThrottleEpoch,
		PacketThrottleAcceleration:     packetThrottleAcceleration,
		PacketThrottleDeceleration:     packetThrottleDeceleration,
		PacketThrottleInterval:         packetThrottleInterval,
		TimeoutLimit:                   timeoutLimit,
		TimeoutMinimum:                 timeoutMinimum,
		TimeoutMaximum:                 timeoutMaximum,
		LastRoundTripTime:              lastRoundTripTime,
		LowestRoundTripTime:            lowestRoundTripTime,
		LastRoundTripTimeVariance:      lastRoundTripTimeVariance,
		HighestRoundTripTimeVariance:   highestRoundTripTimeVariance,
		RoundTripTime:                  roundTripTime,
		RoundTripTimeVariance:          roundTripTimeVariance,
		ReliableDataInTransit:          reliableDataInTransit,
		IncomingUnsequencedGroup:       incomingUnsequencedGroup,
		UnsequencedWindow:              unsequencedWindow,
	}
}

func (h *Host) resetPeer(p *peer.Peer) {
	incomingPeerID := p.IncomingPeerID
	connectID := p.ConnectID

	h.removePeerDispatch(p)
	h.initializePeer(p, int(incomingPeerID), core.Address{}, core.PeerStateDisconnected, protocolMaximumPeerID, 0xFF, 0xFF)
	p.ConnectID = connectID
	h.runtime[p] = defaultPeerRuntime()
}

func (h *Host) queueAcknowledgement(p *peer.Peer, header protocol.CommandHeader, sentTime uint16) {
	switch p.State {
	case core.PeerStateDisconnecting, core.PeerStateAcknowledgingConnect, core.PeerStateDisconnected, core.PeerStateZombie:
		return
	case core.PeerStateAcknowledgingDisconnect:
		if header.Command != protocol.CommandDisconnect {
			return
		}
	}
	if int(header.ChannelID) < len(p.Channels) {
		channel := &p.Channels[header.ChannelID]
		reliableWindow := header.ReliableSequenceNumber / peerReliableWindowSize
		currentWindow := channel.IncomingReliableSequenceNumber / peerReliableWindowSize
		if header.ReliableSequenceNumber < channel.IncomingReliableSequenceNumber {
			reliableWindow += peerReliableWindows
		}
		if reliableWindow >= currentWindow+peerFreeReliableWindows-1 &&
			reliableWindow <= currentWindow+peerFreeReliableWindows {
			return
		}
	}

	ack := &peer.Acknowledgement{
		SentTime: uint32(sentTime),
		Command: peer.Command{
			Header: peer.Header{
				Command:                header.Command,
				ChannelID:              header.ChannelID,
				Flags:                  header.Flags,
				ReliableSequenceNumber: header.ReliableSequenceNumber,
			},
		},
	}
	p.OutgoingDataTotal += checkedUint32FromInt(len(marshalAcknowledgement(ack).MarshalBinary(nil)))
	p.Acknowledgements.PushBack(ack)
}

func (h *Host) removeSentReliableCommand(p *peer.Peer, reliableSequenceNumber uint16, channelID uint8) protocol.Command {
	for elem := p.SentReliableCommands.Front(); elem != nil; elem = elem.Next() {
		cmd := elem.Value()
		if cmd.ReliableSequenceNumber != reliableSequenceNumber || cmd.Command.Header.ChannelID != channelID {
			continue
		}
		p.SentReliableCommands.Remove(elem)
		if cmd.Packet != nil {
			if uint32(cmd.FragmentLength) >= p.ReliableDataInTransit {
				p.ReliableDataInTransit = 0
			} else {
				p.ReliableDataInTransit -= uint32(cmd.FragmentLength)
			}
		}
		h.updateNextTimeout(p)
		return cmd.Command.Header.Command
	}

	return protocol.CommandNone
}

func peerThrottle(p *peer.Peer, roundTripTime uint32) int {
	if p.LastRoundTripTime <= p.LastRoundTripTimeVariance {
		p.PacketThrottle = p.PacketThrottleLimit
	} else if roundTripTime <= p.LastRoundTripTime {
		p.PacketThrottle += p.PacketThrottleAcceleration
		if p.PacketThrottle > p.PacketThrottleLimit {
			p.PacketThrottle = p.PacketThrottleLimit
		}
		return 1
	} else if roundTripTime > p.LastRoundTripTime+2*p.LastRoundTripTimeVariance {
		if p.PacketThrottle > p.PacketThrottleDeceleration {
			p.PacketThrottle -= p.PacketThrottleDeceleration
		} else {
			p.PacketThrottle = 0
		}
		return -1
	}

	return 0
}

func (h *Host) findReliableFragmentCommand(channel *peer.Channel, startSequence uint16) *peer.IncomingCommand {
	for elem := channel.IncomingReliableCommands.Back(); elem != nil; elem = elem.Prev() {
		cmd := elem.Value()
		if cmd.ReliableSequenceNumber == startSequence {
			return cmd
		}
		if sequenceDistance(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) <
			sequenceDistance(channel.IncomingReliableSequenceNumber, startSequence) {
			break
		}
	}

	return nil
}

func (h *Host) findUnreliableFragmentCommand(channel *peer.Channel, reliableSequence, startSequence uint16) *peer.IncomingCommand {
	for elem := channel.IncomingUnreliableCommands.Back(); elem != nil; elem = elem.Prev() {
		cmd := elem.Value()
		if cmd.ReliableSequenceNumber == reliableSequence && cmd.UnreliableSequenceNumber == startSequence {
			return cmd
		}
	}

	return nil
}

func commandHeader(command protocol.PacketCommand) protocol.CommandHeader {
	switch cmd := command.(type) {
	case protocol.Acknowledge:
		return cmd.Header
	case protocol.Connect:
		return cmd.Header
	case protocol.VerifyConnect:
		return cmd.Header
	case protocol.Disconnect:
		return cmd.Header
	case protocol.Ping:
		return cmd.Header
	case protocol.SendReliable:
		return cmd.Header
	case protocol.SendUnreliable:
		return cmd.Header
	case protocol.SendUnsequenced:
		return cmd.Header
	case protocol.SendFragment:
		return cmd.Header
	case protocol.BandwidthLimit:
		return cmd.Header
	case protocol.ThrottleConfigure:
		return cmd.Header
	default:
		return protocol.CommandHeader{}
	}
}

func canReceiveOnChannel(p *peer.Peer, channelID uint8) bool {
	if int(channelID) >= len(p.Channels) {
		return false
	}
	return p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater
}

func reliableSequenceWithinWindow(anchor, sequence uint16) bool {
	reliableWindow := sequence / peerReliableWindowSize
	currentWindow := anchor / peerReliableWindowSize
	if sequence < anchor {
		reliableWindow += peerReliableWindows
	}

	return reliableWindow >= currentWindow && reliableWindow < currentWindow+peerFreeReliableWindows-1
}

func nextSessionID(current, requested uint8) uint8 {
	const sessionMask = uint8(protocol.HeaderSessionMask >> protocol.HeaderSessionShift)

	sessionID := requested
	if sessionID == 0xFF {
		sessionID = current
	}
	sessionID = (sessionID + 1) & sessionMask
	if sessionID == current {
		sessionID = (sessionID + 1) & sessionMask
	}
	return sessionID
}

func clampUint32(value, minimum, maximum uint32) uint32 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func incomingChecksumSeed(p *peer.Peer) uint32 {
	if p == nil {
		return 0
	}

	return p.ConnectID
}

func sequenceDistance(anchor, sequence uint16) uint32 {
	if sequence >= anchor {
		return uint32(sequence - anchor)
	}

	return uint32(sequence) + (1 << 16) - uint32(anchor)
}
