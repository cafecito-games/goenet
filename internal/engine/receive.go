package engine

import (
	"context"
	"errors"
	"io"
	"net/netip"

	"github.com/cafecito-games/goenet"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
)

const (
	protocolMaximumPeerID                    = protocol.MaximumPeerID
	peerReliableWindows                      = 16
	peerReliableWindowSize                   = 0x1000
	peerFreeReliableWindows                  = 8
	protocolMaximumFragmentCount      uint32 = 1024 * 1024
	defaultPacketThrottleInterval     uint32 = 5000
	defaultPacketThrottleAcceleration        = 2
	defaultPacketThrottleDeceleration        = 2
)

type Event struct {
	Type      goenet.EventType
	Peer      *peer.Peer
	ChannelID uint8
	Data      uint32
	Packet    *goenet.Packet
}

type peerRuntime struct {
	eventData                  uint32
	windowSize                 uint32
	packetThrottleInterval     uint32
	packetThrottleAcceleration uint32
	packetThrottleDeceleration uint32
}

func defaultPeerRuntime() *peerRuntime {
	return &peerRuntime{
		windowSize:                 protocol.MaximumWindowSize,
		packetThrottleInterval:     defaultPacketThrottleInterval,
		packetThrottleAcceleration: defaultPacketThrottleAcceleration,
		packetThrottleDeceleration: defaultPacketThrottleDeceleration,
	}
}

func (h *Host) Service(ctx context.Context, timeout uint32) (Event, error) {
	if event, ok := h.dispatchEvent(); ok {
		return event, nil
	}
	if err := h.Flush(ctx); err != nil {
		return Event{}, err
	}
	if err := h.receiveIncoming(ctx); err != nil {
		return Event{}, err
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
		h.handleIncomingDatagram(buf[:n], addr)
	}

	return nil
}

func (h *Host) handleIncomingDatagram(payload []byte, addr netip.AddrPort) {
	header, err := protocol.ParseHeader(payload)
	if err != nil {
		return
	}

	offset := protocolHeaderSizeWithoutSentTime
	if header.Flags&protocol.HeaderFlagSentTime != 0 {
		offset = protocolHeaderSizeWithSentTime
	}
	if offset > len(payload) {
		return
	}

	currentPeer, ok := h.lookupPeer(header, addr)
	if !ok {
		return
	}

	for offset < len(payload) {
		command, _, used, err := protocol.ParseCommand(payload[offset:])
		if err != nil {
			return
		}
		offset += used

		if currentPeer == nil {
			if _, ok := command.(protocol.Connect); !ok || offset != len(payload) {
				return
			}
		}

		if !h.handleIncomingCommand(header, &currentPeer, command, addr) {
			return
		}

		acknowledgeHeader := commandHeader(command)
		if acknowledgeHeader.Flags&protocol.CommandFlagAcknowledge == 0 || currentPeer == nil {
			continue
		}
		if header.Flags&protocol.HeaderFlagSentTime == 0 {
			return
		}

		h.queueAcknowledgement(currentPeer, acknowledgeHeader, header.SentTime)
	}
}

func (h *Host) lookupPeer(header protocol.Header, addr netip.AddrPort) (*peer.Peer, bool) {
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
	if candidate.State == goenet.PeerStateDisconnected || candidate.State == goenet.PeerStateZombie {
		return nil, false
	}
	if candidate.Address.AddrPort() != addr {
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
	addr netip.AddrPort,
) bool {
	switch cmd := command.(type) {
	case protocol.Acknowledge:
		return *currentPeer != nil && h.handleAcknowledge(*currentPeer, cmd)
	case protocol.Connect:
		if *currentPeer != nil {
			return false
		}
		peer := h.handleConnect(addr, cmd)
		if peer == nil {
			return false
		}
		*currentPeer = peer
		return true
	case protocol.VerifyConnect:
		return *currentPeer != nil && h.handleVerifyConnect(*currentPeer, cmd)
	case protocol.Disconnect:
		return *currentPeer != nil
	case protocol.Ping:
		return *currentPeer != nil
	case protocol.SendReliable:
		return *currentPeer != nil && h.handleSendReliable(*currentPeer, cmd)
	case protocol.SendUnreliable:
		return *currentPeer != nil && h.handleSendUnreliable(*currentPeer, cmd)
	case protocol.SendUnsequenced:
		return *currentPeer != nil && h.handleSendUnsequenced(*currentPeer, cmd)
	case protocol.SendFragment:
		if *currentPeer == nil {
			return false
		}
		if cmd.Header.Command == protocol.CommandSendUnreliableFragment {
			return h.handleSendUnreliableFragment(*currentPeer, cmd)
		}
		return h.handleSendFragment(*currentPeer, cmd)
	case protocol.BandwidthLimit:
		return *currentPeer != nil && h.handleBandwidthLimit(*currentPeer, cmd)
	case protocol.ThrottleConfigure:
		return *currentPeer != nil && h.handleThrottleConfigure(*currentPeer, cmd)
	default:
		_ = packetHeader
		return false
	}
}

func (h *Host) handleConnect(addr netip.AddrPort, command protocol.Connect) *peer.Peer {
	if command.ChannelCount < protocol.MinimumChannelCount || command.ChannelCount > protocol.MaximumChannelCount {
		return nil
	}

	var selected *peer.Peer
	for _, candidate := range h.peers {
		if candidate != nil && candidate.State == goenet.PeerStateDisconnected {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return nil
	}

	address, err := goenet.NewAddress(addr, 0)
	if err != nil {
		return nil
	}

	channelCount := command.ChannelCount
	if channelCount > uint32(h.config.ChannelLimit) {
		channelCount = uint32(h.config.ChannelLimit)
	}

	selected.Address = address
	selected.State = goenet.PeerStateAcknowledgingConnect
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
	runtime.packetThrottleInterval = command.PacketThrottleInterval
	runtime.packetThrottleAcceleration = command.PacketThrottleAcceleration
	runtime.packetThrottleDeceleration = command.PacketThrottleDeceleration
	runtime.windowSize = clampUint32(command.WindowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize)

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
		PacketThrottleInterval:     runtime.packetThrottleInterval,
		PacketThrottleAcceleration: runtime.packetThrottleAcceleration,
		PacketThrottleDeceleration: runtime.packetThrottleDeceleration,
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

func (h *Host) handleVerifyConnect(p *peer.Peer, command protocol.VerifyConnect) bool {
	if p.State != goenet.PeerStateConnecting {
		return true
	}

	runtime := h.runtime[p]
	if command.ChannelCount < protocol.MinimumChannelCount || command.ChannelCount > protocol.MaximumChannelCount {
		p.State = goenet.PeerStateZombie
		h.enqueuePeerDispatch(p)
		return false
	}
	if command.PacketThrottleInterval != runtime.packetThrottleInterval ||
		command.PacketThrottleAcceleration != runtime.packetThrottleAcceleration ||
		command.PacketThrottleDeceleration != runtime.packetThrottleDeceleration ||
		command.ConnectID != p.ConnectID {
		p.State = goenet.PeerStateZombie
		h.enqueuePeerDispatch(p)
		return false
	}

	h.removeSentReliableCommand(p, 1, 0xFF)
	if command.ChannelCount < uint32(len(p.Channels)) {
		p.Channels = p.Channels[:command.ChannelCount]
	}

	p.OutgoingPeerID = command.OutgoingPeerID
	p.IncomingSessionID = command.IncomingSessionID
	p.OutgoingSessionID = command.OutgoingSessionID
	p.MTU = minUint32(p.MTU, clampUint32(command.MTU, protocol.MinimumMTU, protocol.MaximumMTU))
	p.IncomingBandwidth = command.IncomingBandwidth
	p.OutgoingBandwidth = command.OutgoingBandwidth
	runtime.windowSize = minUint32(runtime.windowSize, clampUint32(command.WindowSize, protocol.MinimumWindowSize, protocol.MaximumWindowSize))

	h.notifyConnect(p)
	return true
}

func (h *Host) handleAcknowledge(p *peer.Peer, command protocol.Acknowledge) bool {
	if p.State == goenet.PeerStateDisconnected || p.State == goenet.PeerStateZombie {
		return true
	}

	commandNumber := h.removeSentReliableCommand(p, command.ReceivedReliableSequenceNumber, command.Header.ChannelID)
	if p.State == goenet.PeerStateAcknowledgingConnect {
		if commandNumber != protocol.CommandVerifyConnect {
			return false
		}
		h.notifyConnect(p)
	}

	return true
}

func (h *Host) handleSendReliable(p *peer.Peer, command protocol.SendReliable) bool {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return false
	}

	packet := &goenet.Packet{
		Data:  append([]byte(nil), command.Data...),
		Flags: goenet.PacketFlagReliable,
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

func (h *Host) handleSendUnreliable(p *peer.Peer, command protocol.SendUnreliable) bool {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return false
	}

	packet := &goenet.Packet{Data: append([]byte(nil), command.Data...)}
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

func (h *Host) handleSendUnsequenced(p *peer.Peer, command protocol.SendUnsequenced) bool {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return false
	}
	if !p.CanQueueWaitingData(uint32(len(command.Data)), h.config.MaximumWaitingData) {
		return false
	}

	cmd := &peer.IncomingCommand{
		Command: peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandSendUnsequenced,
				ChannelID: command.Header.ChannelID,
				Flags:     command.Header.Flags,
			},
		},
		Packet: &goenet.Packet{
			Data:  append([]byte(nil), command.Data...),
			Flags: goenet.PacketFlagUnsequenced,
		},
	}
	p.AddWaitingData(uint32(len(cmd.Packet.Data)))
	p.QueueDispatchedCommand(cmd)
	h.enqueuePeerDispatch(p)
	return true
}

func (h *Host) handleSendFragment(p *peer.Peer, command protocol.SendFragment) bool {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return false
	}

	channel := &p.Channels[command.Header.ChannelID]
	startSequence := command.StartSequenceNumber
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, startSequence) {
		return true
	}
	if command.FragmentCount == 0 ||
		command.FragmentCount > protocolMaximumFragmentCount ||
		command.FragmentNumber >= command.FragmentCount ||
		command.TotalLength > h.config.MaximumPacketSize ||
		command.TotalLength < command.FragmentCount ||
		command.FragmentOffset >= command.TotalLength ||
		uint32(len(command.Data)) > command.TotalLength-command.FragmentOffset {
		return false
	}

	start := h.findReliableFragmentCommand(channel, startSequence)
	if start == nil {
		if !p.CanQueueWaitingData(command.TotalLength, h.config.MaximumWaitingData) {
			return false
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
			Packet: &goenet.Packet{
				Data:  make([]byte, int(command.TotalLength)),
				Flags: goenet.PacketFlagReliable,
			},
		}
		start.SetFragmentCount(command.FragmentCount)
		p.AddWaitingData(command.TotalLength)
		channel.InsertIncomingReliableOrdered(start)
	} else if start.FragmentCount != command.FragmentCount || uint32(len(start.Packet.Data)) != command.TotalLength {
		return false
	}

	if !start.MarkFragmentReceived(command.FragmentNumber) {
		return true
	}
	copy(start.Packet.Data[int(command.FragmentOffset):int(command.FragmentOffset)+len(command.Data)], command.Data)
	if start.IsComplete() {
		h.dispatchReliableCommands(p, channel)
	}
	return true
}

func (h *Host) handleSendUnreliableFragment(p *peer.Peer, command protocol.SendFragment) bool {
	if !canReceiveOnChannel(p, command.Header.ChannelID) {
		return false
	}

	channel := &p.Channels[command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, command.Header.ReliableSequenceNumber) {
		return true
	}
	if command.Header.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber &&
		command.StartSequenceNumber <= channel.IncomingUnreliableSequenceNumber {
		return true
	}
	if command.FragmentCount == 0 ||
		command.FragmentCount > protocolMaximumFragmentCount ||
		command.FragmentNumber >= command.FragmentCount ||
		command.TotalLength > h.config.MaximumPacketSize ||
		command.TotalLength < command.FragmentCount ||
		command.FragmentOffset >= command.TotalLength ||
		uint32(len(command.Data)) > command.TotalLength-command.FragmentOffset {
		return false
	}

	start := h.findUnreliableFragmentCommand(channel, command.Header.ReliableSequenceNumber, command.StartSequenceNumber)
	if start == nil {
		if !p.CanQueueWaitingData(command.TotalLength, h.config.MaximumWaitingData) {
			return false
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
			Packet: &goenet.Packet{Data: make([]byte, int(command.TotalLength))},
		}
		start.SetFragmentCount(command.FragmentCount)
		p.AddWaitingData(command.TotalLength)
		channel.InsertIncomingUnreliableOrdered(start)
	} else if start.FragmentCount != command.FragmentCount || uint32(len(start.Packet.Data)) != command.TotalLength {
		return false
	}

	if !start.MarkFragmentReceived(command.FragmentNumber) {
		return true
	}
	copy(start.Packet.Data[int(command.FragmentOffset):int(command.FragmentOffset)+len(command.Data)], command.Data)
	if start.IsComplete() {
		h.dispatchUnreliableCommands(p, channel)
	}
	return true
}

func (h *Host) handleBandwidthLimit(p *peer.Peer, command protocol.BandwidthLimit) bool {
	if p.State != goenet.PeerStateConnected && p.State != goenet.PeerStateDisconnectLater {
		return false
	}
	p.IncomingBandwidth = command.IncomingBandwidth
	p.OutgoingBandwidth = command.OutgoingBandwidth
	h.runtime[p].windowSize = protocol.MaximumWindowSize
	return true
}

func (h *Host) handleThrottleConfigure(p *peer.Peer, command protocol.ThrottleConfigure) bool {
	if p.State != goenet.PeerStateConnected && p.State != goenet.PeerStateDisconnectLater {
		return false
	}
	runtime := h.runtime[p]
	runtime.packetThrottleInterval = command.PacketThrottleInterval
	runtime.packetThrottleAcceleration = command.PacketThrottleAcceleration
	runtime.packetThrottleDeceleration = command.PacketThrottleDeceleration
	return true
}

func (h *Host) queueReliableIncomingCommand(p *peer.Peer, cmd *peer.IncomingCommand) bool {
	channel := &p.Channels[cmd.Command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
		return true
	}
	if cmd.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber {
		return true
	}
	for elem := channel.IncomingReliableCommands.Back(); elem != nil; elem = elem.Prev() {
		existing := elem.Value()
		if existing.ReliableSequenceNumber == cmd.ReliableSequenceNumber {
			return true
		}
		if sequenceDistance(channel.IncomingReliableSequenceNumber, existing.ReliableSequenceNumber) <
			sequenceDistance(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
			break
		}
	}
	if !p.CanQueueWaitingData(uint32(len(cmd.Packet.Data)), h.config.MaximumWaitingData) {
		return false
	}

	p.AddWaitingData(uint32(len(cmd.Packet.Data)))
	channel.InsertIncomingReliableOrdered(cmd)
	h.dispatchReliableCommands(p, channel)
	return true
}

func (h *Host) queueUnreliableIncomingCommand(p *peer.Peer, cmd *peer.IncomingCommand) bool {
	channel := &p.Channels[cmd.Command.Header.ChannelID]
	if !reliableSequenceWithinWindow(channel.IncomingReliableSequenceNumber, cmd.ReliableSequenceNumber) {
		return true
	}
	if cmd.ReliableSequenceNumber == channel.IncomingReliableSequenceNumber &&
		cmd.UnreliableSequenceNumber <= channel.IncomingUnreliableSequenceNumber {
		return true
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
			return true
		}
		if existing.UnreliableSequenceNumber < cmd.UnreliableSequenceNumber {
			break
		}
	}
	if !p.CanQueueWaitingData(uint32(len(cmd.Packet.Data)), h.config.MaximumWaitingData) {
		return false
	}

	p.AddWaitingData(uint32(len(cmd.Packet.Data)))
	channel.InsertIncomingUnreliableOrdered(cmd)
	h.dispatchUnreliableCommands(p, channel)
	return true
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
		if cmd.Command.Header.Command != protocol.CommandSendUnsequenced {
			if cmd.ReliableSequenceNumber != channel.IncomingReliableSequenceNumber || !cmd.IsComplete() {
				break
			}
			channel.MarkIncomingUnreliableDispatched(cmd)
		}
		channel.IncomingUnreliableCommands.Remove(front)
		p.QueueDispatchedCommand(cmd)
		moved = true
	}
	if moved {
		h.enqueuePeerDispatch(p)
	}
}

func (h *Host) dispatchEvent() (Event, bool) {
	for len(h.dispatchQ) > 0 {
		p := h.dispatchQ[0]
		h.dispatchQ = h.dispatchQ[1:]
		delete(h.dispatchSet, p)

		switch p.State {
		case goenet.PeerStateConnectionPending, goenet.PeerStateConnectionSucceeded:
			p.State = goenet.PeerStateConnected
			return Event{
				Type: goenet.EventConnect,
				Peer: p,
				Data: h.runtime[p].eventData,
			}, true
		case goenet.PeerStateConnected:
			cmd := p.PopDispatchedCommand()
			if cmd == nil || cmd.Packet == nil {
				continue
			}
			p.ReleaseWaitingData(uint32(len(cmd.Packet.Data)))
			if p.DispatchedCommands.Len() > 0 {
				h.enqueuePeerDispatch(p)
			}
			return Event{
				Type:      goenet.EventReceive,
				Peer:      p,
				ChannelID: cmd.Command.Header.ChannelID,
				Packet:    cmd.Packet,
			}, true
		case goenet.PeerStateZombie:
			return Event{
				Type: goenet.EventDisconnect,
				Peer: p,
				Data: h.runtime[p].eventData,
			}, true
		}
	}

	return Event{}, false
}

func (h *Host) enqueuePeerDispatch(p *peer.Peer) {
	if _, ok := h.dispatchSet[p]; ok {
		return
	}
	h.dispatchSet[p] = struct{}{}
	h.dispatchQ = append(h.dispatchQ, p)
}

func (h *Host) notifyConnect(p *peer.Peer) {
	if p.State == goenet.PeerStateConnecting {
		p.State = goenet.PeerStateConnectionSucceeded
	} else {
		p.State = goenet.PeerStateConnectionPending
	}
	h.enqueuePeerDispatch(p)
}

func (h *Host) queueAcknowledgement(p *peer.Peer, header protocol.CommandHeader, sentTime uint16) {
	switch p.State {
	case goenet.PeerStateDisconnecting, goenet.PeerStateAcknowledgingConnect, goenet.PeerStateDisconnected, goenet.PeerStateZombie:
		return
	case goenet.PeerStateAcknowledgingDisconnect:
		if header.Command != protocol.CommandDisconnect {
			return
		}
	}

	p.Acknowledgements.PushBack(&peer.Acknowledgement{
		SentTime: uint32(sentTime),
		Command: peer.Command{
			Header: peer.Header{
				Command:                header.Command,
				ChannelID:              header.ChannelID,
				Flags:                  header.Flags,
				ReliableSequenceNumber: header.ReliableSequenceNumber,
			},
		},
	})
}

func (h *Host) removeSentReliableCommand(p *peer.Peer, reliableSequenceNumber uint16, channelID uint8) protocol.Command {
	for elem := p.SentReliableCommands.Front(); elem != nil; elem = elem.Next() {
		cmd := elem.Value()
		if cmd.ReliableSequenceNumber != reliableSequenceNumber || cmd.Command.Header.ChannelID != channelID {
			continue
		}
		p.SentReliableCommands.Remove(elem)
		return cmd.Command.Header.Command
	}

	return protocol.CommandNone
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
	return p.State == goenet.PeerStateConnected || p.State == goenet.PeerStateDisconnectLater
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

func sequenceDistance(anchor, sequence uint16) uint32 {
	if sequence >= anchor {
		return uint32(sequence - anchor)
	}

	return uint32(sequence) + (1 << 16) - uint32(anchor)
}
