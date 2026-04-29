// Package protocol implements ENet packet headers and command wire formats.
package protocol

// Command is an ENet protocol command identifier.
type Command uint8

// CommandFlag stores the ENet command-level bit flags packed into the command byte.
type CommandFlag uint8

// HeaderFlag stores the ENet packet-header bit flags packed alongside the peer ID.
type HeaderFlag uint16

const (
	// MinimumMTU is ENet's minimum accepted peer MTU.
	MinimumMTU uint32 = 576
	// MaximumMTU is ENet's maximum accepted peer MTU.
	MaximumMTU uint32 = 4096
	// MaximumPacketCommands is ENet's hard cap on commands per UDP datagram.
	MaximumPacketCommands uint32 = 32
	// MinimumWindowSize is ENet's minimum reliable window size.
	MinimumWindowSize uint32 = 4096
	// MaximumWindowSize is ENet's maximum reliable window size.
	MaximumWindowSize uint32 = 65536
	// MinimumChannelCount is ENet's minimum channel count per peer.
	MinimumChannelCount uint32 = 1
	// MaximumChannelCount is ENet's maximum channel count per peer.
	MaximumChannelCount uint32 = 255
	// MaximumPeerID is the peer ID mask ENet stores in packet headers.
	MaximumPeerID uint16 = 0x0FFF
	// UnsequencedWindowSize is ENet's per-peer unsequenced acknowledgment window in commands.
	UnsequencedWindowSize uint32 = 1024
	// MaximumFragmentCount caps the number of fragments per logical packet — must
	// match ENET_PROTOCOL_MAXIMUM_FRAGMENT_COUNT in the C reference. Used to reject
	// hostile peer-controlled values before fragment-state allocation.
	MaximumFragmentCount uint32 = 1024 * 1024

	// CommandNone is a zero-value placeholder and not a valid on-the-wire command.
	CommandNone Command = 0
	// CommandAcknowledge is ENet's acknowledge command identifier.
	CommandAcknowledge Command = 1
	// CommandConnect is ENet's connect command identifier.
	CommandConnect Command = 2
	// CommandVerifyConnect is ENet's verify-connect command identifier.
	CommandVerifyConnect Command = 3
	// CommandDisconnect is ENet's disconnect command identifier.
	CommandDisconnect Command = 4
	// CommandPing is ENet's ping command identifier.
	CommandPing Command = 5
	// CommandSendReliable is ENet's reliable payload command identifier.
	CommandSendReliable Command = 6
	// CommandSendUnreliable is ENet's unreliable payload command identifier.
	CommandSendUnreliable Command = 7
	// CommandSendFragment is ENet's reliable fragment command identifier.
	CommandSendFragment Command = 8
	// CommandSendUnsequenced is ENet's unsequenced payload command identifier.
	CommandSendUnsequenced Command = 9
	// CommandBandwidthLimit is ENet's bandwidth-limit command identifier.
	CommandBandwidthLimit Command = 10
	// CommandThrottleConfigure is ENet's throttle-configure command identifier.
	CommandThrottleConfigure Command = 11
	// CommandSendUnreliableFragment is ENet's unreliable fragment command identifier.
	CommandSendUnreliableFragment Command = 12
	// CommandCount is the exclusive upper bound of supported command identifiers.
	CommandCount Command = 13

	// CommandMask selects the low nibble that stores the command ID.
	CommandMask CommandFlag = 0x0F
	// CommandFlagUnsequenced marks a command as unsequenced.
	CommandFlagUnsequenced CommandFlag = 1 << 6
	// CommandFlagAcknowledge marks a command as requiring acknowledgment.
	CommandFlagAcknowledge CommandFlag = 1 << 7
	// HeaderFlagCompressed marks a packet payload as compressor-encoded.
	HeaderFlagCompressed HeaderFlag = 1 << 14
	// HeaderFlagSentTime indicates that the packet carries a sent-time field.
	HeaderFlagSentTime HeaderFlag = 1 << 15
	// HeaderFlagMask selects the header flag bits from the packed peer ID field.
	HeaderFlagMask HeaderFlag = HeaderFlagCompressed | HeaderFlagSentTime
	// HeaderSessionMask selects the packed session bits from the header peer ID field.
	HeaderSessionMask uint16 = 3 << 12
	// HeaderSessionShift is the bit offset of the packed session ID within the header peer ID field.
	HeaderSessionShift           = 12
	headerMinimalSize            = 2
	headerSize                   = 4
	commandHeaderSize            = 4
	acknowledgeCommandSize       = 8
	connectCommandSize           = 48
	verifyConnectCommandSize     = 44
	disconnectCommandSize        = 8
	pingCommandSize              = 4
	sendReliableCommandSize      = 6
	sendUnreliableCommandSize    = 8
	sendUnsequencedCommandSize   = 8
	sendFragmentCommandSize      = 24
	bandwidthLimitCommandSize    = 12
	throttleConfigureCommandSize = 16
)
