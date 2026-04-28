package protocol

// Command is an ENet protocol command identifier.
type Command uint8

const (
	MinimumMTU            uint32  = 576
	MaximumMTU            uint32  = 4096
	MaximumPacketCommands uint32  = 32
	CommandAcknowledge    Command = 1
	CommandConnect        Command = 2
	CommandVerifyConnect  Command = 3
	CommandDisconnect     Command = 4
	CommandPing           Command = 5
	CommandSendReliable   Command = 6
)
