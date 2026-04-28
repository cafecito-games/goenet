package engine

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cafecito-games/goenet"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
)

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
		payload, wroteAny, err := h.flushPeer(ctx, p)
		if err != nil {
			return err
		}
		if !wroteAny {
			continue
		}

		n, err := h.socket.WritePacket(ctx, p.Address.AddrPort(), payload)
		if err != nil {
			return err
		}
		if n != len(payload) {
			return fmt.Errorf("engine: short write: wrote %d of %d", n, len(payload))
		}
	}

	return nil
}

func (h *Host) flushPeer(ctx context.Context, p *peer.Peer) ([]byte, bool, error) {
	_ = ctx

	reliableFront := p.OutgoingSendReliableCommands.Front()
	unreliableFront := p.OutgoingCommands.Front()
	if reliableFront == nil && unreliableFront == nil {
		return nil, false, nil
	}

	payload := protocol.Header{
		Flags:    protocol.HeaderFlagSentTime,
		SentTime: uint16(h.serviceTime),
	}.MarshalBinary(nil)

	for reliableFront != nil || unreliableFront != nil {
		useReliable := false
		switch {
		case reliableFront == nil:
		case unreliableFront == nil:
			useReliable = true
		case reliableFront.Value().QueueTime <= unreliableFront.Value().QueueTime:
			useReliable = true
		}

		if useReliable {
			cmd := p.OutgoingSendReliableCommands.Remove(reliableFront)
			reliableFront = p.OutgoingSendReliableCommands.Front()

			cmd.SendAttempts++
			cmd.SentTime = h.serviceTime
			if cmd.RoundTripTimeout == 0 {
				cmd.RoundTripTimeout = defaultRoundTripTimeout
			}

			payload = cmd.Command.Payload.MarshalBinary(payload)
			p.SentReliableCommands.PushBack(cmd)
			continue
		}

		cmd := p.OutgoingCommands.Remove(unreliableFront)
		unreliableFront = p.OutgoingCommands.Front()
		payload = cmd.Command.Payload.MarshalBinary(payload)
	}

	return payload, true, nil
}
