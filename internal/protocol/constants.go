package protocol

// Command is an ENet protocol command identifier.
type Command uint8
type CommandFlag uint8
type HeaderFlag uint16

const (
	MinimumMTU            uint32 = 576
	MaximumMTU            uint32 = 4096
	MaximumPacketCommands uint32 = 32
	MinimumWindowSize     uint32 = 4096
	MaximumWindowSize     uint32 = 65536
	MinimumChannelCount   uint32 = 1
	MaximumChannelCount   uint32 = 255
	MaximumPeerID         uint16 = 0x0FFF

	CommandNone                   Command = 0
	CommandAcknowledge            Command = 1
	CommandConnect                Command = 2
	CommandVerifyConnect          Command = 3
	CommandDisconnect             Command = 4
	CommandPing                   Command = 5
	CommandSendReliable           Command = 6
	CommandSendUnreliable         Command = 7
	CommandSendFragment           Command = 8
	CommandSendUnsequenced        Command = 9
	CommandBandwidthLimit         Command = 10
	CommandThrottleConfigure      Command = 11
	CommandSendUnreliableFragment Command = 12
	CommandCount                  Command = 13

	CommandMask               CommandFlag = 0x0F
	CommandFlagUnsequenced    CommandFlag = 1 << 6
	CommandFlagAcknowledge    CommandFlag = 1 << 7
	HeaderFlagCompressed      HeaderFlag  = 1 << 14
	HeaderFlagSentTime        HeaderFlag  = 1 << 15
	HeaderFlagMask            HeaderFlag  = HeaderFlagCompressed | HeaderFlagSentTime
	HeaderSessionMask         uint16      = 3 << 12
	HeaderSessionShift                    = 12
	headerMinimalSize                     = 2
	headerSize                            = 4
	commandHeaderSize                     = 4
	acknowledgeCommandSize                = 8
	connectCommandSize                    = 48
	verifyConnectCommandSize              = 44
	disconnectCommandSize                 = 8
	pingCommandSize                       = 4
	sendReliableCommandSize               = 6
	sendUnreliableCommandSize             = 8
	sendFragmentCommandSize               = 24
)
