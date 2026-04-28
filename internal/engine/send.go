package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
)

const (
	protocolHeaderSizeWithoutSentTime = 2
	protocolHeaderSizeWithSentTime    = 4
	sendReliableCommandSize           = 6
	sendUnreliableCommandSize         = 8
)

type outgoingSelection struct {
	command           *peer.OutgoingCommand
	ack               *peer.Acknowledgement
	fromReliableQueue bool
	requiresAck       bool
	wireSize          int
}

type preparedDatagram struct {
	payload  []byte
	selected []outgoingSelection
}

type outgoingPayloadSequencer interface {
	setOutgoingSequenceNumbers(reliable, unreliable uint16)
}

type sendReliablePayload struct {
	channelID              uint8
	reliableSequenceNumber uint16
	data                   []byte
}

func (p *sendReliablePayload) setOutgoingSequenceNumbers(reliable, _ uint16) {
	p.reliableSequenceNumber = reliable
}

func (p *sendReliablePayload) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 6+len(p.data))...)
	dst[start] = byte(protocol.CommandSendReliable | protocol.Command(protocol.CommandFlagAcknowledge))
	dst[start+1] = p.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], p.reliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], uint16(len(p.data)))
	copy(dst[start+6:], p.data)
	return dst
}

type sendUnreliablePayload struct {
	channelID                uint8
	reliableSequenceNumber   uint16
	unreliableSequenceNumber uint16
	data                     []byte
}

func (p *sendUnreliablePayload) setOutgoingSequenceNumbers(reliable, unreliable uint16) {
	p.reliableSequenceNumber = reliable
	p.unreliableSequenceNumber = unreliable
}

func (p *sendUnreliablePayload) MarshalBinary(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 8+len(p.data))...)
	dst[start] = byte(protocol.CommandSendUnreliable)
	dst[start+1] = p.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], p.reliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], p.unreliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], uint16(len(p.data)))
	copy(dst[start+8:], p.data)
	return dst
}

func (h *Host) queueOutgoingCommand(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	if err := h.validatePacketSize(p, packet); err != nil {
		return err
	}

	command := &peer.OutgoingCommand{
		FragmentLength: uint16(len(packet.Data)),
		Packet:         packet,
	}

	if packet.Flags&core.PacketFlagReliable != 0 || p.Channels[channelID].OutgoingUnreliableSequenceNumber >= 0xFFFF {
		command.Command = peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandSendReliable,
				ChannelID: channelID,
				Flags:     protocol.CommandFlagAcknowledge,
			},
			Payload: &sendReliablePayload{
				channelID: channelID,
				data:      append([]byte(nil), packet.Data...),
			},
		}
	} else {
		command.Command = peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandSendUnreliable,
				ChannelID: channelID,
			},
			Payload: &sendUnreliablePayload{
				channelID: channelID,
				data:      append([]byte(nil), packet.Data...),
			},
		}
	}

	return h.setupAndQueueOutgoingCommand(p, command)
}

func (h *Host) queueOutgoingControlCommand(p *peer.Peer, command peer.Command) error {
	return h.setupAndQueueOutgoingCommand(p, &peer.OutgoingCommand{Command: command})
}

func (h *Host) Flush(ctx context.Context) error {
	for _, p := range h.peers {
		if blocked := findUnsendableQueuedCommand(p, h.config.Checksum != nil); blocked != nil {
			return fmt.Errorf(
				"engine: queued command %d cannot fit within peer MTU %d",
				blocked.Command.Header.Command,
				p.MTU,
			)
		}

		for {
			datagram, wroteAny, err := h.preparePeerDatagram(p)
			if err != nil {
				return err
			}
			if !wroteAny {
				break
			}

			n, err := h.socket.WritePacket(ctx, p.Address.AddrPort(), datagram.payload)
			if err != nil {
				return err
			}
			if n != len(datagram.payload) {
				return fmt.Errorf("engine: short write: wrote %d of %d", n, len(datagram.payload))
			}

			h.commitPreparedDatagram(p, datagram)
		}
	}

	return nil
}

func findUnsendableQueuedCommand(p *peer.Peer, withChecksum bool) *peer.OutgoingCommand {
	for elem := p.OutgoingSendReliableCommands.Front(); elem != nil; elem = elem.Next() {
		if !commandFitsPeerMTU(p, elem.Value(), withChecksum) {
			return elem.Value()
		}
	}
	for elem := p.OutgoingCommands.Front(); elem != nil; elem = elem.Next() {
		if !commandFitsPeerMTU(p, elem.Value(), withChecksum) {
			return elem.Value()
		}
	}

	return nil
}

func (h *Host) preparePeerDatagram(p *peer.Peer) (preparedDatagram, bool, error) {
	selected, blocked := h.selectOutgoingBatch(p)
	if blocked != nil {
		return preparedDatagram{}, false, fmt.Errorf(
			"engine: queued command %d cannot fit within peer MTU %d",
			blocked.command.Command.Header.Command,
			p.MTU,
		)
	}
	if len(selected) == 0 {
		return preparedDatagram{}, false, nil
	}

	header := protocol.Header{
		PeerID:    p.OutgoingPeerID,
		SessionID: p.OutgoingSessionID,
	}
	if batchRequiresAck(selected) {
		header.Flags = protocol.HeaderFlagSentTime
		header.SentTime = uint16(h.serviceTime)
	}

	body := make([]byte, 0)

	for _, item := range selected {
		if item.ack != nil {
			body = marshalAcknowledgement(item.ack).MarshalBinary(body)
			continue
		}
		body = item.command.Command.Payload.MarshalBinary(body)
	}

	if h.config.Compressor != nil {
		compressed := make([]byte, len(body))
		n, err := h.config.Compressor.Compress([]core.Buffer{{Data: body}}, len(body), compressed)
		if err != nil {
			return preparedDatagram{}, false, err
		}
		if n > 0 && n < len(body) {
			header.Flags |= protocol.HeaderFlagCompressed
			body = compressed[:n]
		}
	}

	headerBytes := header.MarshalBinary(nil)
	payload := make([]byte, 0, len(headerBytes)+len(body)+checksumSize(h.config.Checksum))
	payload = append(payload, headerBytes...)
	if h.config.Checksum != nil {
		checksumBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(checksumBytes, outgoingChecksumSeed(p))
		sum := h.config.Checksum.Checksum([]core.Buffer{
			{Data: headerBytes},
			{Data: checksumBytes},
			{Data: body},
		})
		binary.LittleEndian.PutUint32(checksumBytes, sum)
		payload = append(payload, checksumBytes...)
	}
	payload = append(payload, body...)

	return preparedDatagram{
		payload:  payload,
		selected: selected,
	}, true, nil
}

func (h *Host) commitPreparedDatagram(p *peer.Peer, datagram preparedDatagram) {
	for _, item := range datagram.selected {
		if item.ack != nil {
			front := p.Acknowledgements.Front()
			if front == nil || front.Value() != item.ack {
				panic("engine: acknowledgement queue commit order mismatch")
			}
			p.Acknowledgements.Remove(front)
			if item.ack.Command.Header.Command == protocol.CommandDisconnect && p.State == core.PeerStateAcknowledgingDisconnect {
				p.State = core.PeerStateZombie
				h.enqueuePeerDispatch(p)
			}
			continue
		}

		cmd := removeCommittedCommand(p, item)

		if !item.requiresAck {
			continue
		}

		wasEmpty := p.SentReliableCommands.Len() == 0
		markCommandInFlight(p, cmd, h.serviceTime)
		if cmd.Packet != nil {
			p.ReliableDataInTransit += uint32(cmd.FragmentLength)
		}
		if wasEmpty {
			p.NextTimeout = h.serviceTime + cmd.RoundTripTimeout
		}
		p.SentReliableCommands.PushBack(cmd)
	}
}

func removeCommittedCommand(p *peer.Peer, item outgoingSelection) *peer.OutgoingCommand {
	if item.fromReliableQueue {
		front := p.OutgoingSendReliableCommands.Front()
		if front == nil || front.Value() != item.command {
			panic("engine: reliable outgoing queue commit order mismatch")
		}
		return p.OutgoingSendReliableCommands.Remove(front)
	}

	front := p.OutgoingCommands.Front()
	if front == nil || front.Value() != item.command {
		panic("engine: outgoing queue commit order mismatch")
	}
	return p.OutgoingCommands.Remove(front)
}

func markCommandInFlight(p *peer.Peer, cmd *peer.OutgoingCommand, serviceTime uint32) {
	cmd.SendAttempts++
	cmd.SentTime = serviceTime
	if cmd.RoundTripTimeout == 0 {
		cmd.RoundTripTimeout = p.RoundTripTime + 4*p.RoundTripTimeVariance
	}
}

func (h *Host) selectOutgoingBatch(p *peer.Peer) ([]outgoingSelection, *outgoingSelection) {
	ackFront := p.Acknowledgements.Front()
	reliableFront := p.OutgoingSendReliableCommands.Front()
	outgoingFront := p.OutgoingCommands.Front()

	selected := make([]outgoingSelection, 0, protocol.MaximumPacketCommands)
	bodySize := 0
	hasAck := false

	for ackFront != nil || reliableFront != nil || outgoingFront != nil {
		var next outgoingSelection
		switch {
		case ackFront != nil:
			next = buildAckSelection(ackFront.Value())
			ackFront = ackFront.Next()
		case reliableFront == nil:
			next = buildSelection(outgoingFront.Value(), false)
			outgoingFront = outgoingFront.Next()
		case outgoingFront == nil:
			next = buildSelection(reliableFront.Value(), true)
			reliableFront = reliableFront.Next()
		case reliableFront.Value().QueueTime <= outgoingFront.Value().QueueTime:
			next = buildSelection(reliableFront.Value(), true)
			reliableFront = reliableFront.Next()
		default:
			next = buildSelection(outgoingFront.Value(), false)
			outgoingFront = outgoingFront.Next()
		}

		nextHasAck := hasAck || next.requiresAck
		headerSize := headerOverhead(nextHasAck, h.config.Checksum != nil)

		if headerSize+bodySize+next.wireSize > int(p.MTU) {
			if len(selected) == 0 {
				blocked := next
				return nil, &blocked
			}
			break
		}
		if len(selected) >= int(protocol.MaximumPacketCommands) {
			break
		}

		selected = append(selected, next)
		bodySize += next.wireSize
		hasAck = nextHasAck
	}

	return selected, nil
}

func buildSelection(cmd *peer.OutgoingCommand, fromReliableQueue bool) outgoingSelection {
	return outgoingSelection{
		command:           cmd,
		fromReliableQueue: fromReliableQueue,
		requiresAck:       commandRequiresAck(cmd),
		wireSize:          commandWireSize(cmd),
	}
}

func buildAckSelection(ack *peer.Acknowledgement) outgoingSelection {
	return outgoingSelection{
		ack:      ack,
		wireSize: len(marshalAcknowledgement(ack).MarshalBinary(nil)),
	}
}

func batchRequiresAck(selected []outgoingSelection) bool {
	for _, item := range selected {
		if item.requiresAck {
			return true
		}
	}

	return false
}

func commandRequiresAck(cmd *peer.OutgoingCommand) bool {
	return cmd.Command.Header.Flags&protocol.CommandFlagAcknowledge != 0
}

func commandWireSize(cmd *peer.OutgoingCommand) int {
	return len(cmd.Command.Payload.MarshalBinary(nil))
}

func commandFitsPeerMTU(p *peer.Peer, cmd *peer.OutgoingCommand, withChecksum bool) bool {
	return headerOverhead(commandRequiresAck(cmd), withChecksum)+commandWireSize(cmd) <= int(p.MTU)
}

func marshalAcknowledgement(ack *peer.Acknowledgement) protocol.Acknowledge {
	return protocol.Acknowledge{
		Header: protocol.CommandHeader{
			ChannelID:              ack.Command.Header.ChannelID,
			ReliableSequenceNumber: ack.Command.Header.ReliableSequenceNumber,
		},
		ReceivedReliableSequenceNumber: ack.Command.Header.ReliableSequenceNumber,
		ReceivedSentTime:               uint16(ack.SentTime),
	}
}

func (h *Host) validatePacketSize(p *peer.Peer, packet *core.Packet) error {
	if packet.Flags&core.PacketFlagUnsequenced != 0 {
		return fmt.Errorf("engine: unsequenced packets are not supported in task 5")
	}
	if len(packet.Data) > math.MaxUint16 {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}
	if len(packet.Data) > h.maxPacketDataLength(p, packet.Flags) {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}

	return nil
}

func (h *Host) maxPacketDataLength(p *peer.Peer, flags core.PacketFlag) int {
	commandSize := sendUnreliableCommandSize
	requiresSentTime := false
	if flags&core.PacketFlagReliable != 0 {
		requiresSentTime = true
		commandSize = sendReliableCommandSize
	}

	overhead := headerOverhead(requiresSentTime, h.config.Checksum != nil) + commandSize
	if p.MTU <= uint32(overhead) {
		return 0
	}

	return int(p.MTU) - overhead
}

func (h *Host) setupAndQueueOutgoingCommand(p *peer.Peer, command *peer.OutgoingCommand) error {
	if err := h.prepareOutgoingCommand(p, command); err != nil {
		return err
	}

	if commandRequiresAck(command) && command.Packet != nil {
		p.OutgoingSendReliableCommands.PushBack(command)
		return nil
	}

	p.OutgoingCommands.PushBack(command)
	return nil
}

func (h *Host) prepareOutgoingCommand(p *peer.Peer, command *peer.OutgoingCommand) error {
	channelID := command.Command.Header.ChannelID
	if channelID != 0xFF && int(channelID) >= len(p.Channels) {
		return fmt.Errorf("engine: channel %d out of range", channelID)
	}
	if command.Command.Header.Flags&protocol.CommandFlagUnsequenced != 0 {
		return fmt.Errorf("engine: unsequenced commands are not supported in task 5")
	}

	var reliable, unreliable uint16
	switch {
	case channelID == 0xFF:
		p.OutgoingReliableSequenceNumber++
		reliable = p.OutgoingReliableSequenceNumber
	case commandRequiresAck(command):
		channel := &p.Channels[channelID]
		channel.OutgoingReliableSequenceNumber++
		channel.OutgoingUnreliableSequenceNumber = 0
		reliable = channel.OutgoingReliableSequenceNumber
	default:
		channel := &p.Channels[channelID]
		channel.OutgoingUnreliableSequenceNumber++
		reliable = channel.OutgoingReliableSequenceNumber
		unreliable = channel.OutgoingUnreliableSequenceNumber
	}

	command.ReliableSequenceNumber = reliable
	command.UnreliableSequenceNumber = unreliable
	command.SendAttempts = 0
	command.SentTime = 0
	command.RoundTripTimeout = 0
	p.OutgoingDataTotal += uint32(commandWireSize(command))
	h.totalQueued++
	command.QueueTime = h.totalQueued
	command.Command.Header.ReliableSequenceNumber = reliable
	applyOutgoingHeader(command.Command.Payload, command.Command.Header)

	if sequencer, ok := command.Command.Payload.(outgoingPayloadSequencer); ok {
		sequencer.setOutgoingSequenceNumbers(reliable, unreliable)
	}

	return nil
}

func applyOutgoingHeader(payload protocol.PacketCommand, header peer.Header) {
	commandHeader := protocol.CommandHeader{
		Command:                header.Command,
		ChannelID:              header.ChannelID,
		Flags:                  header.Flags,
		ReliableSequenceNumber: header.ReliableSequenceNumber,
	}

	switch cmd := payload.(type) {
	case *protocol.Connect:
		cmd.Header = commandHeader
	case *protocol.VerifyConnect:
		cmd.Header = commandHeader
	case *protocol.Disconnect:
		cmd.Header = commandHeader
	case *protocol.Ping:
		cmd.Header = commandHeader
	case *protocol.SendFragment:
		cmd.Header = commandHeader
	case *protocol.BandwidthLimit:
		cmd.Header = commandHeader
	case *protocol.ThrottleConfigure:
		cmd.Header = commandHeader
	}
}

func headerOverhead(withSentTime, withChecksum bool) int {
	size := protocolHeaderSizeWithoutSentTime
	if withSentTime {
		size = protocolHeaderSizeWithSentTime
	}
	if withChecksum {
		size += 4
	}

	return size
}

func checksumSize(checksummer core.Checksummer) int {
	if checksummer == nil {
		return 0
	}

	return 4
}

func outgoingChecksumSeed(p *peer.Peer) uint32 {
	if p.OutgoingPeerID >= protocol.MaximumPeerID {
		return 0
	}

	return p.ConnectID
}
