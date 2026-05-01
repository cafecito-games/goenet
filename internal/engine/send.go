package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/protocol"
)

const (
	protocolHeaderSizeWithoutSentTime = protocol.HeaderSizeMinimal
	protocolHeaderSizeWithSentTime    = protocol.HeaderSizeWithSentTime
	sendReliableCommandSize           = protocol.SendReliableCommandSize
	sendUnreliableCommandSize         = protocol.SendUnreliableCommandSize
	sendUnsequencedCommandSize        = protocol.SendUnsequencedCommandSize
	sendFragmentCommandSize           = protocol.SendFragmentCommandSize
	maximumDatagramsPerPeerFlush      = int(protocol.MaximumPacketCommands)
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

type wireSizer interface {
	WireSize() int
}

type sendReliablePayload struct {
	channelID              uint8
	reliableSequenceNumber uint16
	data                   []byte
}

func (p *sendReliablePayload) setOutgoingSequenceNumbers(reliable, _ uint16) {
	p.reliableSequenceNumber = reliable
}

// AppendBinary appends the reliable payload wire encoding to dst.
func (p *sendReliablePayload) AppendBinary(dst []byte) ([]byte, error) {
	dst, start := appendLen(dst, p.WireSize())
	dst[start] = byte(protocol.CommandSendReliable | protocol.Command(protocol.CommandFlagAcknowledge))
	dst[start+1] = p.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], p.reliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], checkedUint16FromInt(len(p.data)))
	copy(dst[start+6:], p.data)
	return dst, nil
}

// WireSize reports the encoded size of the reliable payload command.
func (p *sendReliablePayload) WireSize() int {
	return sendReliableCommandSize + len(p.data)
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

// AppendBinary appends the unreliable payload wire encoding to dst.
func (p *sendUnreliablePayload) AppendBinary(dst []byte) ([]byte, error) {
	dst, start := appendLen(dst, p.WireSize())
	dst[start] = byte(protocol.CommandSendUnreliable)
	dst[start+1] = p.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], p.reliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], p.unreliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+6:start+8], checkedUint16FromInt(len(p.data)))
	copy(dst[start+8:], p.data)
	return dst, nil
}

// WireSize reports the encoded size of the unreliable payload command.
func (p *sendUnreliablePayload) WireSize() int {
	return sendUnreliableCommandSize + len(p.data)
}

type sendUnsequencedPayload struct {
	channelID              uint8
	reliableSequenceNumber uint16
	unsequencedGroup       uint16
	data                   []byte
}

func (p *sendUnsequencedPayload) setOutgoingSequenceNumbers(reliable, _ uint16) {
	p.reliableSequenceNumber = reliable
}

// AppendBinary appends the unsequenced payload wire encoding to dst.
func (p *sendUnsequencedPayload) AppendBinary(dst []byte) ([]byte, error) {
	dst, start := appendLen(dst, p.WireSize())
	dst[start] = byte(protocol.CommandSendUnsequenced | protocol.Command(protocol.CommandFlagUnsequenced))
	dst[start+1] = p.channelID
	binary.BigEndian.PutUint16(dst[start+2:start+4], p.reliableSequenceNumber)
	binary.BigEndian.PutUint16(dst[start+4:start+6], p.unsequencedGroup)
	binary.BigEndian.PutUint16(dst[start+6:start+8], checkedUint16FromInt(len(p.data)))
	copy(dst[start+8:], p.data)
	return dst, nil
}

// WireSize reports the encoded size of the unsequenced payload command.
func (p *sendUnsequencedPayload) WireSize() int {
	return sendUnsequencedCommandSize + len(p.data)
}

func (h *Host) queueOutgoingCommand(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	if err := h.validatePacketSize(p, channelID, packet); err != nil {
		return err
	}
	maxPacketDataLength := h.maxPacketDataLength(p, packet.Flags)
	if needsFragmentation(packet, maxPacketDataLength) {
		if packet.Flags&core.PacketFlagReliable != 0 {
			return h.queueOutgoingReliableFragments(p, channelID, packet)
		}
		return h.queueOutgoingUnreliableFragments(p, channelID, packet)
	}

	command := &peer.OutgoingCommand{
		FragmentLength: checkedUint16FromInt(len(packet.Data)),
		Packet:         packet,
	}

	if packet.Flags&(core.PacketFlagReliable|core.PacketFlagUnsequenced) == core.PacketFlagUnsequenced {
		command.Command = peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandSendUnsequenced,
				ChannelID: channelID,
				Flags:     protocol.CommandFlagUnsequenced,
			},
			Payload: &sendUnsequencedPayload{
				channelID: channelID,
				data:      append([]byte(nil), packet.Data...),
			},
		}
	} else if packet.Flags&core.PacketFlagReliable != 0 || p.Channels[channelID].OutgoingUnreliableSequenceNumber == math.MaxUint16 {
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

// needsFragmentation reports whether the packet must be split into fragment
// commands. ENet supports fragmentation for both reliable and (since the
// CommandSendUnreliableFragment opcode was introduced) unreliable packets;
// unsequenced packets are never fragmented.
func needsFragmentation(packet *core.Packet, maxPacketDataLength int) bool {
	if packet.Flags&core.PacketFlagUnsequenced != 0 && packet.Flags&core.PacketFlagReliable == 0 {
		return false
	}
	return len(packet.Data) > maxPacketDataLength
}

func (h *Host) queueOutgoingReliableFragments(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	startSequenceNumber := p.Channels[channelID].OutgoingReliableSequenceNumber + 1
	return h.queueOutgoingFragments(
		p,
		channelID,
		packet,
		protocol.CommandSendFragment,
		protocol.CommandFlagAcknowledge,
		startSequenceNumber,
		h.maxReliableFragmentDataLength(p),
	)
}

func (h *Host) queueOutgoingUnreliableFragments(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	startSequenceNumber := p.Channels[channelID].OutgoingUnreliableSequenceNumber + 1
	return h.queueOutgoingFragments(
		p,
		channelID,
		packet,
		protocol.CommandSendUnreliableFragment,
		0,
		startSequenceNumber,
		h.maxUnreliableFragmentDataLength(p),
	)
}

func (h *Host) queueOutgoingFragments(
	p *peer.Peer,
	channelID uint8,
	packet *core.Packet,
	commandID protocol.Command,
	flags protocol.CommandFlag,
	startSequenceNumber uint16,
	fragmentLength int,
) error {
	fragmentCount := fragmentCountForLength(len(packet.Data), fragmentLength)
	for fragmentNumber := 0; fragmentNumber < fragmentCount; fragmentNumber++ {
		offset := fragmentNumber * fragmentLength
		end := offset + fragmentLength
		if end > len(packet.Data) {
			end = len(packet.Data)
		}

		command := &peer.OutgoingCommand{
			FragmentOffset: checkedUint32FromInt(offset),
			FragmentLength: checkedUint16FromInt(end - offset),
			Packet:         packet,
			Command: peer.Command{
				Header: peer.Header{
					Command:   commandID,
					ChannelID: channelID,
					Flags:     flags,
				},
				Payload: &protocol.SendFragment{
					Header: protocol.CommandHeader{
						Command: commandID,
					},
					StartSequenceNumber: startSequenceNumber,
					FragmentCount:       checkedUint32FromInt(fragmentCount),
					FragmentNumber:      checkedUint32FromInt(fragmentNumber),
					TotalLength:         checkedUint32FromInt(len(packet.Data)),
					FragmentOffset:      checkedUint32FromInt(offset),
					Data:                append([]byte(nil), packet.Data[offset:end]...),
				},
			},
		}
		if err := h.setupAndQueueOutgoingCommand(p, command); err != nil {
			return err
		}
	}

	return nil
}

func fragmentCountForLength(totalLength, fragmentLength int) int {
	if totalLength <= 0 || fragmentLength <= 0 {
		return 0
	}

	return (totalLength + fragmentLength - 1) / fragmentLength
}

func (h *Host) queueOutgoingControlCommand(p *peer.Peer, command peer.Command) error {
	return h.setupAndQueueOutgoingCommand(p, &peer.OutgoingCommand{Command: command})
}

// Flush serializes and writes all currently queued outbound peer traffic.
func (h *Host) Flush(ctx context.Context) error {
	writeBudget := maximumDatagramsPerPeerFlush
	if len(h.peers) > 1 {
		writeBudget *= len(h.peers)
	}

	for _, p := range h.peers {
		if blocked := findUnsendableQueuedCommand(p, h.config.Checksum != nil); blocked != nil {
			return fmt.Errorf(
				"%w: command %d, peer mtu %d",
				ErrCommandExceedsMTU,
				blocked.Command.Header.Command,
				p.MTU,
			)
		}

		for {
			if writeBudget == 0 {
				h.logger.Warn("flush budget exhausted", "peer_id", p.IncomingPeerID)
				return nil
			}
			datagram, wroteAny, err := h.preparePeerDatagram(p)
			if err != nil {
				return err
			}
			if !wroteAny {
				break
			}

			n, err := h.socket.WritePacket(ctx, p.Address, datagram.payload)
			if err != nil {
				return err
			}
			if n != len(datagram.payload) {
				return fmt.Errorf("%w: wrote %d of %d", ErrShortWrite, n, len(datagram.payload))
			}

			h.commitPreparedDatagram(p, datagram)
			writeBudget--
			if h.runtime[p].disconnectLater && p.State == core.PeerStateDisconnectLater && !h.hasOutgoingCommands(p) {
				// Same as the receive-path call: always hits the no-flush branch.
				if err := h.Disconnect(ctx, p, h.runtime[p].eventData); err != nil {
					return err
				}
			}
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
			"%w: command %d, peer mtu %d",
			ErrCommandExceedsMTU,
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
		header.SentTime = lowUint16FromUint32(h.serviceTime)
	}

	body := make([]byte, 0)

	for _, item := range selected {
		if item.ack != nil {
			ackBytes, err := marshalAcknowledgement(item.ack).AppendBinary(body)
			if err != nil {
				return preparedDatagram{}, false, fmt.Errorf("engine: append ack: %w", err)
			}
			body = ackBytes
			continue
		}
		appended, err := item.command.Command.Payload.AppendBinary(body)
		if err != nil {
			return preparedDatagram{}, false, fmt.Errorf("engine: append command %d: %w", item.command.Command.Header.Command, err)
		}
		body = appended
	}

	if h.config.Compressor != nil {
		compressed := make([]byte, len(body))
		n, err := h.config.Compressor.Compress([][]byte{body}, len(body), compressed)
		if err != nil {
			return preparedDatagram{}, false, err
		}
		if n > 0 && n < len(body) {
			header.Flags |= protocol.HeaderFlagCompressed
			body = compressed[:n]
		}
	}

	headerBytes, err := header.AppendBinary(nil)
	if err != nil {
		return preparedDatagram{}, false, fmt.Errorf("engine: append header: %w", err)
	}
	payload := make([]byte, 0, len(headerBytes)+len(body)+checksumSize(h.config.Checksum))
	payload = append(payload, headerBytes...)
	if h.config.Checksum != nil {
		checksumBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(checksumBytes, outgoingChecksumSeed(p))
		sum := h.config.Checksum.Checksum([][]byte{headerBytes, checksumBytes, body})
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
		p.IndexSentReliableCommand(p.SentReliableCommands.PushBack(cmd))
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

func (h *Host) selectOutgoingBatch(p *peer.Peer) (selected []outgoingSelection, blocked *outgoingSelection) {
	ackFront := p.Acknowledgements.Front()
	reliableFront := p.OutgoingSendReliableCommands.Front()
	outgoingFront := p.OutgoingCommands.Front()

	selected = make([]outgoingSelection, 0, protocol.MaximumPacketCommands)
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
		wireSize: marshalAcknowledgement(ack).WireSize(),
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
	if sized, ok := cmd.Command.Payload.(wireSizer); ok {
		return sized.WireSize()
	}
	// Every payload type used by the engine — both the parallel
	// sendReliablePayload/sendUnreliablePayload/sendUnsequencedPayload and the
	// protocol command types — implements WireSize. Reaching this fallback
	// means a new payload type was added without it; treat that as a
	// compile-time-style invariant violation.
	panic(fmt.Sprintf("engine: command payload %T missing WireSize", cmd.Command.Payload))
}

func commandFitsPeerMTU(p *peer.Peer, cmd *peer.OutgoingCommand, withChecksum bool) bool {
	return headerOverhead(commandRequiresAck(cmd), withChecksum)+commandWireSize(cmd) <= int(p.MTU)
}

func marshalAcknowledgement(ack *peer.Acknowledgement) protocol.Acknowledge {
	// Command is set explicitly here so the wire encoding is fully determined
	// by the constructed value; we no longer depend on protocol.Acknowledge's
	// MarshalBinary patching the command byte at serialization time.
	return protocol.Acknowledge{
		Header: protocol.CommandHeader{
			Command:                protocol.CommandAcknowledge,
			ChannelID:              ack.Command.Header.ChannelID,
			ReliableSequenceNumber: ack.Command.Header.ReliableSequenceNumber,
		},
		ReceivedReliableSequenceNumber: ack.Command.Header.ReliableSequenceNumber,
		ReceivedSentTime:               lowUint16FromUint32(ack.SentTime),
	}
}

func (h *Host) validatePacketSize(p *peer.Peer, channelID uint8, packet *core.Packet) error {
	maxPacketDataLength := h.maxPacketDataLength(p, packet.Flags)
	if len(packet.Data) <= maxPacketDataLength {
		return nil
	}
	// Unsequenced packets cannot be fragmented (no per-fragment ordering anchor),
	// so they must fit within a single command body.
	if packet.Flags&core.PacketFlagUnsequenced != 0 && packet.Flags&core.PacketFlagReliable == 0 {
		return fmt.Errorf("%w: unsequenced %d-byte packet exceeds limit", ErrNoFragmentation, len(packet.Data))
	}

	var fragmentLength int
	reliable := packet.Flags&core.PacketFlagReliable != 0
	if reliable {
		fragmentLength = h.maxReliableFragmentDataLength(p)
	} else {
		fragmentLength = h.maxUnreliableFragmentDataLength(p)
	}
	if fragmentLength <= 0 {
		return fmt.Errorf("%w: peer mtu %d too small to fragment %d-byte packet", ErrNoFragmentation, p.MTU, len(packet.Data))
	}
	fragmentCount := fragmentCountForLength(len(packet.Data), fragmentLength)
	if fragmentCount > int(protocolMaximumFragmentCount) {
		return fmt.Errorf("%w: %d-byte packet would need %d fragments", ErrFragmentationLimit, len(packet.Data), fragmentCount)
	}
	if reliable && fragmentCount > remainingReliableSequenceSpace(p, channelID) {
		return fmt.Errorf("%w: reliable sequence space exhausted for fragmented send", ErrReliableSequenceExhausted)
	}

	return nil
}

func remainingReliableSequenceSpace(p *peer.Peer, channelID uint8) int {
	return int(math.MaxUint16 - p.Channels[channelID].OutgoingReliableSequenceNumber)
}

func (h *Host) maxPacketDataLength(p *peer.Peer, flags core.PacketFlag) int {
	commandSize := sendUnreliableCommandSize
	requiresSentTime := false
	if flags&(core.PacketFlagReliable|core.PacketFlagUnsequenced) == core.PacketFlagUnsequenced {
		commandSize = sendUnsequencedCommandSize
	} else if flags&core.PacketFlagReliable != 0 {
		requiresSentTime = true
		commandSize = sendReliableCommandSize
	}

	overhead := headerOverhead(requiresSentTime, h.config.Checksum != nil) + commandSize
	if p.MTU <= checkedUint32FromInt(overhead) {
		return 0
	}

	return int(p.MTU) - overhead
}

func (h *Host) maxReliableFragmentDataLength(p *peer.Peer) int {
	overhead := headerOverhead(true, h.config.Checksum != nil) + sendFragmentCommandSize
	if p.MTU <= checkedUint32FromInt(overhead) {
		return 0
	}

	return int(p.MTU) - overhead
}

func (h *Host) maxUnreliableFragmentDataLength(p *peer.Peer) int {
	// Unreliable fragments share the same wire layout but ride in a datagram
	// without the sent-time header, since unreliable commands do not require
	// acknowledgement.
	overhead := headerOverhead(false, h.config.Checksum != nil) + sendFragmentCommandSize
	if p.MTU <= checkedUint32FromInt(overhead) {
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
		return fmt.Errorf("%w: %d", ErrChannelOutOfRange, channelID)
	}

	var reliable, unreliable uint16
	switch {
	case channelID == 0xFF:
		p.OutgoingReliableSequenceNumber++
		reliable = p.OutgoingReliableSequenceNumber
	case commandRequiresAck(command):
		channel := p.Channels[channelID]
		channel.OutgoingReliableSequenceNumber++
		channel.OutgoingUnreliableSequenceNumber = 0
		reliable = channel.OutgoingReliableSequenceNumber
	case command.Command.Header.Flags&protocol.CommandFlagUnsequenced != 0:
		p.OutgoingUnsequencedGroup++
	default:
		channel := p.Channels[channelID]
		// Match enet.h:4174-4176: only the first fragment of an unreliable packet
		// gets a fresh unreliable sequence number; later fragments share it so the
		// receiver's reassembly anchor sees them as one logical packet.
		if command.FragmentOffset == 0 {
			channel.OutgoingUnreliableSequenceNumber++
		}
		reliable = channel.OutgoingReliableSequenceNumber
		unreliable = channel.OutgoingUnreliableSequenceNumber
	}

	command.ReliableSequenceNumber = reliable
	command.UnreliableSequenceNumber = unreliable
	command.SendAttempts = 0
	command.SentTime = 0
	command.RoundTripTimeout = 0
	p.OutgoingDataTotal += checkedUint32FromInt(commandWireSize(command))
	h.totalQueued++
	command.QueueTime = h.totalQueued
	command.Command.Header.ReliableSequenceNumber = reliable
	applyOutgoingHeader(command.Command.Payload, command.Command.Header)

	if sequencer, ok := command.Command.Payload.(outgoingPayloadSequencer); ok {
		sequencer.setOutgoingSequenceNumbers(reliable, unreliable)
	}
	if payload, ok := command.Command.Payload.(*sendUnsequencedPayload); ok {
		payload.unsequencedGroup = p.OutgoingUnsequencedGroup
	}

	return nil
}

func applyOutgoingHeader(payload protocol.PacketCommand, header peer.Header) {
	hc, ok := payload.(protocol.HeaderedCommand)
	if !ok {
		return
	}
	hc.SetHeader(protocol.CommandHeader{
		Command:                header.Command,
		ChannelID:              header.ChannelID,
		Flags:                  header.Flags,
		ReliableSequenceNumber: header.ReliableSequenceNumber,
	})
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

func appendLen(dst []byte, n int) (buf []byte, start int) {
	start = len(dst)
	dst = slices.Grow(dst, n)
	dst = dst[:start+n]
	return dst, start
}

func outgoingChecksumSeed(p *peer.Peer) uint32 {
	if p.OutgoingPeerID >= protocol.MaximumPeerID {
		return 0
	}

	return p.ConnectID
}
