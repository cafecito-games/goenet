package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cafecito-games/goenet"
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

func (h *Host) queueOutgoingCommand(p *peer.Peer, channelID uint8, packet *goenet.Packet) error {
	if err := h.validatePacketSize(p, packet); err != nil {
		return err
	}

	command := &peer.OutgoingCommand{
		FragmentLength: uint16(len(packet.Data)),
		Packet:         packet,
	}

	if packet.Flags&goenet.PacketFlagReliable != 0 || p.Channels[channelID].OutgoingUnreliableSequenceNumber >= 0xFFFF {
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

func (h *Host) preparePeerDatagram(p *peer.Peer) (preparedDatagram, bool, error) {
	selected := h.selectOutgoingBatch(p)
	if len(selected) == 0 {
		return preparedDatagram{}, false, nil
	}

	header := protocol.Header{}
	if batchRequiresAck(selected) {
		header.Flags = protocol.HeaderFlagSentTime
		header.SentTime = uint16(h.serviceTime)
	}
	payload := header.MarshalBinary(nil)

	for _, item := range selected {
		payload = item.command.Command.Payload.MarshalBinary(payload)
	}

	return preparedDatagram{
		payload:  payload,
		selected: selected,
	}, true, nil
}

func (h *Host) commitPreparedDatagram(p *peer.Peer, datagram preparedDatagram) {
	for _, item := range datagram.selected {
		var cmd *peer.OutgoingCommand
		if item.fromReliableQueue {
			cmd = p.OutgoingSendReliableCommands.Remove(p.OutgoingSendReliableCommands.Front())
		} else {
			cmd = p.OutgoingCommands.Remove(p.OutgoingCommands.Front())
		}

		if !item.requiresAck {
			continue
		}

		markCommandInFlight(cmd, h.serviceTime)
		p.SentReliableCommands.PushBack(cmd)
	}
}

func markCommandInFlight(cmd *peer.OutgoingCommand, serviceTime uint32) {
	cmd.SendAttempts++
	cmd.SentTime = serviceTime
	if cmd.RoundTripTimeout == 0 {
		cmd.RoundTripTimeout = defaultRoundTripTimeout
	}
}

func (h *Host) selectOutgoingBatch(p *peer.Peer) []outgoingSelection {
	reliableFront := p.OutgoingSendReliableCommands.Front()
	outgoingFront := p.OutgoingCommands.Front()

	selected := make([]outgoingSelection, 0, protocol.MaximumPacketCommands)
	bodySize := 0
	hasAck := false

	for reliableFront != nil || outgoingFront != nil {
		var next outgoingSelection
		switch {
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
		headerSize := protocolHeaderSizeWithoutSentTime
		if nextHasAck {
			headerSize = protocolHeaderSizeWithSentTime
		}

		if len(selected) >= int(protocol.MaximumPacketCommands) ||
			headerSize+bodySize+next.wireSize > int(p.MTU) {
			break
		}

		selected = append(selected, next)
		bodySize += next.wireSize
		hasAck = nextHasAck
	}

	return selected
}

func buildSelection(cmd *peer.OutgoingCommand, fromReliableQueue bool) outgoingSelection {
	return outgoingSelection{
		command:           cmd,
		fromReliableQueue: fromReliableQueue,
		requiresAck:       commandRequiresAck(cmd),
		wireSize:          commandWireSize(cmd),
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

func (h *Host) validatePacketSize(p *peer.Peer, packet *goenet.Packet) error {
	if packet.Flags&goenet.PacketFlagUnsequenced != 0 {
		return fmt.Errorf("engine: unsequenced packets are not supported in task 5")
	}
	if len(packet.Data) > math.MaxUint16 {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}
	if len(packet.Data) > maxPacketDataLength(p, packet.Flags) {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}

	return nil
}

func maxPacketDataLength(p *peer.Peer, flags goenet.PacketFlag) int {
	headerSize := protocolHeaderSizeWithoutSentTime
	commandSize := sendUnreliableCommandSize
	if flags&goenet.PacketFlagReliable != 0 {
		headerSize = protocolHeaderSizeWithSentTime
		commandSize = sendReliableCommandSize
	}

	overhead := headerSize + commandSize
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
	h.totalQueued++
	command.QueueTime = h.totalQueued
	command.Command.Header.ReliableSequenceNumber = reliable

	if sequencer, ok := command.Command.Payload.(outgoingPayloadSequencer); ok {
		sequencer.setOutgoingSequenceNumbers(reliable, unreliable)
	}

	return nil
}
