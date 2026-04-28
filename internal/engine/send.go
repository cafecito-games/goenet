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

type sendReliablePayload struct {
	channelID              uint8
	reliableSequenceNumber uint16
	data                   []byte
}

func (p sendReliablePayload) MarshalBinary(dst []byte) []byte {
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

func (p sendUnreliablePayload) MarshalBinary(dst []byte) []byte {
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
	if err := h.validatePacketSize(packet); err != nil {
		return err
	}

	channel := &p.Channels[channelID]
	command := &peer.OutgoingCommand{
		FragmentLength: uint16(len(packet.Data)),
		Packet:         packet,
	}
	h.totalQueued++
	command.QueueTime = h.totalQueued

	if packet.Flags&goenet.PacketFlagReliable != 0 || channel.OutgoingUnreliableSequenceNumber >= 0xFFFF {
		channel.OutgoingReliableSequenceNumber++
		channel.OutgoingUnreliableSequenceNumber = 0

		command.ReliableSequenceNumber = channel.OutgoingReliableSequenceNumber
		command.Command = peer.Command{
			Header: peer.Header{
				Command:                protocol.CommandSendReliable,
				ChannelID:              channelID,
				Flags:                  protocol.CommandFlagAcknowledge,
				ReliableSequenceNumber: command.ReliableSequenceNumber,
			},
			Payload: sendReliablePayload{
				channelID:              channelID,
				reliableSequenceNumber: command.ReliableSequenceNumber,
				data:                   append([]byte(nil), packet.Data...),
			},
		}
		p.OutgoingSendReliableCommands.PushBack(command)
	} else {
		channel.OutgoingUnreliableSequenceNumber++

		command.ReliableSequenceNumber = channel.OutgoingReliableSequenceNumber
		command.UnreliableSequenceNumber = channel.OutgoingUnreliableSequenceNumber
		command.Command = peer.Command{
			Header: peer.Header{
				Command:                protocol.CommandSendUnreliable,
				ChannelID:              channelID,
				ReliableSequenceNumber: command.ReliableSequenceNumber,
			},
			Payload: sendUnreliablePayload{
				channelID:                channelID,
				reliableSequenceNumber:   command.ReliableSequenceNumber,
				unreliableSequenceNumber: command.UnreliableSequenceNumber,
				data:                     append([]byte(nil), packet.Data...),
			},
		}
		p.OutgoingCommands.PushBack(command)
	}

	return nil
}

func (h *Host) Flush(ctx context.Context) error {
	for _, p := range h.peers {
		for {
			payload, wroteAny, err := h.flushPeerDatagram(ctx, p)
			if err != nil {
				return err
			}
			if !wroteAny {
				break
			}

			n, err := h.socket.WritePacket(ctx, p.Address.AddrPort(), payload)
			if err != nil {
				return err
			}
			if n != len(payload) {
				return fmt.Errorf("engine: short write: wrote %d of %d", n, len(payload))
			}
		}
	}

	return nil
}

func (h *Host) flushPeerDatagram(ctx context.Context, p *peer.Peer) ([]byte, bool, error) {
	_ = ctx

	selected := h.selectOutgoingBatch(p)
	if len(selected) == 0 {
		return nil, false, nil
	}

	header := protocol.Header{}
	if batchRequiresAck(selected) {
		header.Flags = protocol.HeaderFlagSentTime
		header.SentTime = uint16(h.serviceTime)
	}
	payload := header.MarshalBinary(nil)

	for _, item := range selected {
		if item.fromReliableQueue {
			cmd := p.OutgoingSendReliableCommands.Remove(p.OutgoingSendReliableCommands.Front())
			if item.requiresAck {
				markCommandInFlight(cmd, h.serviceTime)
				payload = cmd.Command.Payload.MarshalBinary(payload)
				p.SentReliableCommands.PushBack(cmd)
				continue
			}

			payload = cmd.Command.Payload.MarshalBinary(payload)
			continue
		}

		cmd := p.OutgoingCommands.Remove(p.OutgoingCommands.Front())
		if item.requiresAck {
			markCommandInFlight(cmd, h.serviceTime)
			payload = cmd.Command.Payload.MarshalBinary(payload)
			p.SentReliableCommands.PushBack(cmd)
			continue
		}

		payload = cmd.Command.Payload.MarshalBinary(payload)
	}

	return payload, true, nil
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
			headerSize+bodySize+next.wireSize > int(h.config.MTU) {
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

func (h *Host) validatePacketSize(packet *goenet.Packet) error {
	if len(packet.Data) > math.MaxUint16 {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}
	if len(packet.Data) > h.maxPacketDataLength(packet.Flags) {
		return fmt.Errorf("engine: packet exceeds no-fragmentation limit: %d", len(packet.Data))
	}

	return nil
}

func (h *Host) maxPacketDataLength(flags goenet.PacketFlag) int {
	headerSize := protocolHeaderSizeWithoutSentTime
	commandSize := sendUnreliableCommandSize
	if flags&goenet.PacketFlagReliable != 0 {
		headerSize = protocolHeaderSizeWithSentTime
		commandSize = sendReliableCommandSize
	}

	overhead := headerSize + commandSize
	if h.config.MTU <= uint32(overhead) {
		return 0
	}

	return int(h.config.MTU) - overhead
}
