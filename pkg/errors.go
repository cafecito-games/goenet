package goenet

import "github.com/cafecito-games/goenet/internal/engine"

// Sentinel errors returned by the packet-send surface (Peer.Send,
// Host.Broadcast) and the flush surface (Host.Flush). They are re-exported from
// the internal engine so callers can classify failures with errors.Is instead
// of matching on error strings.
//
// Each is returned wrapped (via fmt.Errorf("%w: ...", ...)) with additional
// context such as the peer state, channel ID, or byte size, so always compare
// with errors.Is — never ==.
//
// Note: Peer.Send and other peer methods invoked on a nil or detached peer
// handle return ErrNilPeer (defined alongside ErrHostClosed in host.go), which
// is distinct from these engine-surfaced errors.
var (
	// ErrPeerNotConnected is returned by a send when the target peer is not in a
	// connected state — it has disconnected or is mid-teardown. This is the
	// expected signal that a client dropped its connection rather than a fault.
	ErrPeerNotConnected = engine.ErrPeerNotConnected

	// ErrNilPacket is returned by a send when the packet argument is nil.
	ErrNilPacket = engine.ErrNilPacket

	// ErrChannelOutOfRange is returned by a send for a channel ID outside the
	// peer's negotiated channel count.
	ErrChannelOutOfRange = engine.ErrChannelOutOfRange

	// ErrPacketTooLarge is returned by a send when the packet exceeds the host's
	// configured MaximumPacketSize.
	ErrPacketTooLarge = engine.ErrPacketTooLarge

	// ErrFragmentationLimit is returned by Flush when a reliable send would
	// exceed the maximum fragment count.
	ErrFragmentationLimit = engine.ErrFragmentationLimit

	// ErrNoFragmentation is returned by Flush when a packet cannot be fragmented
	// — for example an unsequenced packet over the limit, or a peer MTU too small
	// to fragment the payload.
	ErrNoFragmentation = engine.ErrNoFragmentation

	// ErrReliableSequenceExhausted is returned by Flush when a channel's outgoing
	// reliable sequence space is exhausted.
	ErrReliableSequenceExhausted = engine.ErrReliableSequenceExhausted

	// ErrShortWrite is returned by Flush when the underlying socket reports a
	// partial datagram write.
	ErrShortWrite = engine.ErrShortWrite

	// ErrCommandExceedsMTU is returned by Flush when a queued command's wire size
	// exceeds the peer MTU.
	ErrCommandExceedsMTU = engine.ErrCommandExceedsMTU
)
