package engine

import (
	"fmt"

	"github.com/cafecito-games/goenet"
	"github.com/cafecito-games/goenet/internal/peer"
	"github.com/cafecito-games/goenet/internal/socket"
)

const defaultRoundTripTimeout uint32 = 500

// Host carries the minimal outbound engine state for queueing and flush tests.
type Host struct {
	config      goenet.Config
	socket      socket.DatagramSocket
	peers       []*peer.Peer
	serviceTime uint32
	totalQueued uint32
	dispatchSet map[*peer.Peer]struct{}
	dispatchQ   []*peer.Peer
	runtime     map[*peer.Peer]*peerRuntime
	intercepted *Event
}

func NewHost(config goenet.Config, sock socket.DatagramSocket, serviceTime uint32) *Host {
	cfg := goenet.DefaultConfig()
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

func (h *Host) AddPeer(addr goenet.Address, state goenet.PeerState) *peer.Peer {
	for index, candidate := range h.peers {
		if candidate != nil && candidate.State == goenet.PeerStateDisconnected {
			return h.configurePeer(candidate, index, addr, state)
		}
	}

	index := len(h.peers)
	p := h.newPeerSlot(index)
	p = h.configurePeer(p, index, addr, state)
	h.peers = append(h.peers, p)
	return p
}

func (h *Host) Send(p *peer.Peer, channelID uint8, packet *goenet.Packet) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if packet == nil {
		return fmt.Errorf("engine: nil packet")
	}
	if p.State != goenet.PeerStateConnected {
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

func (h *Host) newPeerSlot(index int) *peer.Peer {
	p := &peer.Peer{
		IncomingPeerID:    uint16(index),
		OutgoingPeerID:    protocolMaximumPeerID,
		IncomingSessionID: 0xFF,
		OutgoingSessionID: 0xFF,
		MTU:               h.config.MTU,
		State:             goenet.PeerStateDisconnected,
	}
	h.runtime[p] = defaultPeerRuntime()
	return p
}

func (h *Host) configurePeer(p *peer.Peer, index int, addr goenet.Address, state goenet.PeerState) *peer.Peer {
	channels := make([]peer.Channel, h.config.ChannelLimit)
	for i := range channels {
		channels[i] = peer.NewChannel()
	}

	outgoingPeerID := uint16(index + 1)
	if state == goenet.PeerStateConnecting {
		outgoingPeerID = protocolMaximumPeerID
	}

	*p = peer.Peer{
		IncomingPeerID:    uint16(index),
		OutgoingPeerID:    outgoingPeerID,
		IncomingSessionID: 0xFF,
		OutgoingSessionID: uint8((index % 3) + 1),
		MTU:               h.config.MTU,
		Address:           addr,
		State:             state,
		Channels:          channels,
	}
	h.runtime[p] = defaultPeerRuntime()
	return p
}
