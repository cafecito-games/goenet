package protocol_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cafecito-games/goenet/internal/protocol"
)

func TestCommandHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	header := protocol.CommandHeader{
		Command:                protocol.CommandSendFragment,
		ChannelID:              0x07,
		Flags:                  protocol.CommandFlagAcknowledge,
		ReliableSequenceNumber: 0x1234,
	}

	wire := header.MarshalBinary(nil)
	wantWire := []byte{0x88, 0x07, 0x12, 0x34}
	if !bytes.Equal(wire, wantWire) {
		t.Fatalf("marshal bytes = %x, want %x", wire, wantWire)
	}

	got, err := protocol.ParseCommandHeader(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got != header {
		t.Fatalf("header mismatch: %+v != %+v", got, header)
	}
}

func TestCommandMatchesGolden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		golden   string
		flags    protocol.CommandFlag
		want     protocol.PacketCommand
		validate func(*testing.T, []byte)
	}{
		{
			name:   "acknowledge",
			golden: "ack.bin",
			want: protocol.Acknowledge{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandAcknowledge,
					ChannelID:              0x02,
					ReliableSequenceNumber: 5,
				},
				ReceivedReliableSequenceNumber: 4,
				ReceivedSentTime:               1234,
			},
		},
		{
			name:   "connect",
			golden: "connect.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.Connect{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandConnect,
					ChannelID:              0xff,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 3,
				},
				OutgoingPeerID:             7,
				IncomingSessionID:          1,
				OutgoingSessionID:          2,
				MTU:                        1400,
				WindowSize:                 32768,
				ChannelCount:               2,
				IncomingBandwidth:          60000,
				OutgoingBandwidth:          30000,
				PacketThrottleInterval:     5000,
				PacketThrottleAcceleration: 2,
				PacketThrottleDeceleration: 3,
				ConnectID:                  0xdeadbeef,
				Data:                       0x10203040,
			},
			validate: func(t *testing.T, wire []byte) {
				t.Helper()
				if got, want := wire[40:44], []byte{0xef, 0xbe, 0xad, 0xde}; !bytes.Equal(got, want) {
					t.Fatalf("connectID bytes = %x, want %x", got, want)
				}
			},
		},
		{
			name:   "verify connect",
			golden: "verify_connect.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.VerifyConnect{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandVerifyConnect,
					ChannelID:              0xff,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 9,
				},
				OutgoingPeerID:             33,
				IncomingSessionID:          3,
				OutgoingSessionID:          4,
				MTU:                        1400,
				WindowSize:                 32768,
				ChannelCount:               2,
				IncomingBandwidth:          60000,
				OutgoingBandwidth:          30000,
				PacketThrottleInterval:     5000,
				PacketThrottleAcceleration: 2,
				PacketThrottleDeceleration: 3,
				ConnectID:                  0x12345678,
			},
		},
		{
			name:   "disconnect",
			golden: "disconnect.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.Disconnect{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandDisconnect,
					ChannelID:              0xff,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 11,
				},
				Data: 0xaabbccdd,
			},
		},
		{
			name:   "ping",
			golden: "ping.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.Ping{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandPing,
					ChannelID:              0xff,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 12,
				},
			},
		},
		{
			name:   "send reliable",
			golden: "send_reliable.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.SendReliable{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandSendReliable,
					ChannelID:              0x02,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 13,
				},
				Data: []byte("hello"),
			},
		},
		{
			name:   "send unreliable",
			golden: "send_unreliable.bin",
			want: protocol.SendUnreliable{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandSendUnreliable,
					ChannelID:              0x03,
					ReliableSequenceNumber: 14,
				},
				UnreliableSequenceNumber: 4,
				Data:                     []byte{0xde, 0xad, 0xbe, 0xef},
			},
		},
		{
			name:   "send fragment",
			golden: "send_fragment.bin",
			flags:  protocol.CommandFlagAcknowledge,
			want: protocol.SendFragment{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandSendFragment,
					ChannelID:              0x01,
					Flags:                  protocol.CommandFlagAcknowledge,
					ReliableSequenceNumber: 15,
				},
				StartSequenceNumber: 10,
				FragmentCount:       4,
				FragmentNumber:      2,
				TotalLength:         10,
				FragmentOffset:      6,
				Data:                []byte("xyz"),
			},
		},
		{
			name:   "send unreliable fragment",
			golden: "send_unreliable_fragment.bin",
			want: protocol.SendFragment{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandSendUnreliableFragment,
					ChannelID:              0x04,
					ReliableSequenceNumber: 16,
				},
				StartSequenceNumber: 7,
				FragmentCount:       3,
				FragmentNumber:      1,
				TotalLength:         12,
				FragmentOffset:      4,
				Data:                []byte("part"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			wire, err := os.ReadFile(filepath.Join("..", "..", "testdata", "protocol", tt.golden))
			if err != nil {
				t.Fatal(err)
			}
			if tt.validate != nil {
				tt.validate(t, wire)
			}

			cmd, flags, n, err := protocol.ParseCommand(wire)
			if err != nil {
				t.Fatal(err)
			}
			if flags != tt.flags {
				t.Fatalf("flags = 0x%02x, want 0x%02x", flags, tt.flags)
			}
			if n != len(wire) {
				t.Fatalf("consumed = %d, want %d", n, len(wire))
			}
			if !reflect.DeepEqual(cmd, tt.want) {
				t.Fatalf("command mismatch:\n got: %#v\nwant: %#v", cmd, tt.want)
			}

			if got := cmd.MarshalBinary(nil); !bytes.Equal(got, wire) {
				t.Fatalf("marshal mismatch: %x != %x", got, wire)
			}
		})
	}
}

func TestParseCommandRejectsTruncatedPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wire []byte
	}{
		{name: "acknowledge", wire: []byte{0x01, 0x02, 0x00, 0x05, 0x00, 0x04, 0x04}},
		{name: "connect", wire: []byte{0x82, 0xff, 0x00, 0x03}},
		{name: "verify connect", wire: []byte{0x83, 0xff, 0x00, 0x09}},
		{name: "disconnect", wire: []byte{0x84, 0xff, 0x00, 0x0b, 0xaa}},
		{name: "send reliable", wire: []byte{0x86, 0x02, 0x00, 0x0d, 0x00}},
		{name: "send unreliable", wire: []byte{0x07, 0x03, 0x00, 0x0e, 0x00, 0x04, 0x00}},
		{name: "send unsequenced", wire: []byte{0x49, 0x05, 0x00, 0x11, 0x00, 0x2a, 0x00}},
		{name: "send fragment", wire: []byte{0x88, 0x01, 0x00, 0x0f, 0x00, 0x0a}},
		{name: "send unreliable fragment", wire: []byte{0x0c, 0x04, 0x00, 0x10, 0x00, 0x07}},
		{name: "bandwidth limit", wire: []byte{0x0a, 0xff, 0x00, 0x12, 0x00, 0x00, 0x01}},
		{name: "throttle configure", wire: []byte{0x0b, 0xff, 0x00, 0x13, 0x00, 0x00, 0x13}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := protocol.ParseCommand(tt.wire); err == nil {
				t.Fatal("ParseCommand succeeded on truncated input")
			}
		})
	}
}

func TestParseSendFragmentRejectsHostileFragmentCount(t *testing.T) {
	t.Parallel()

	// 24-byte fragment header + zero-length data. FragmentCount at bytes 8..12.
	build := func(fragmentCount, fragmentNumber uint32) []byte {
		wire := []byte{
			0x88, 0x01, 0x00, 0x0f, // header
			0x00, 0x00, 0x00, 0x00, // start sequence + dataLength
			0x00, 0x00, 0x00, 0x00, // fragmentCount
			0x00, 0x00, 0x00, 0x00, // fragmentNumber
			0x00, 0x00, 0x00, 0x00, // totalLength
			0x00, 0x00, 0x00, 0x00, // fragmentOffset
		}
		// fragmentCount at offset 8
		wire[8] = byte(fragmentCount >> 24)
		wire[9] = byte(fragmentCount >> 16)
		wire[10] = byte(fragmentCount >> 8)
		wire[11] = byte(fragmentCount)
		// fragmentNumber at offset 12
		wire[12] = byte(fragmentNumber >> 24)
		wire[13] = byte(fragmentNumber >> 16)
		wire[14] = byte(fragmentNumber >> 8)
		wire[15] = byte(fragmentNumber)
		return wire
	}

	tests := []struct {
		name           string
		fragmentCount  uint32
		fragmentNumber uint32
	}{
		{name: "zero count", fragmentCount: 0, fragmentNumber: 0},
		{name: "above maximum", fragmentCount: protocol.MaximumFragmentCount + 1, fragmentNumber: 0},
		{name: "max uint32", fragmentCount: 0xFFFFFFFF, fragmentNumber: 0},
		{name: "number out of range", fragmentCount: 4, fragmentNumber: 4},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := protocol.ParseCommand(build(tt.fragmentCount, tt.fragmentNumber)); err == nil {
				t.Fatalf("ParseCommand accepted hostile fragmentCount=%d fragmentNumber=%d", tt.fragmentCount, tt.fragmentNumber)
			}
		})
	}
}

func TestAdditionalCommandRoundTrips(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		wire  []byte
		flags protocol.CommandFlag
		want  protocol.PacketCommand
	}{
		{
			name:  "send unsequenced",
			wire:  []byte{0x49, 0x05, 0x00, 0x11, 0x00, 0x2a, 0x00, 0x03, 'o', 'n', 'e'},
			flags: protocol.CommandFlagUnsequenced,
			want: protocol.SendUnsequenced{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandSendUnsequenced,
					ChannelID:              0x05,
					Flags:                  protocol.CommandFlagUnsequenced,
					ReliableSequenceNumber: 17,
				},
				UnsequencedGroup: 42,
				Data:             []byte("one"),
			},
		},
		{
			name:  "bandwidth limit",
			wire:  []byte{0x0a, 0xff, 0x00, 0x12, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x08, 0x00},
			flags: 0,
			want: protocol.BandwidthLimit{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandBandwidthLimit,
					ChannelID:              0xff,
					ReliableSequenceNumber: 18,
				},
				IncomingBandwidth: 1024,
				OutgoingBandwidth: 2048,
			},
		},
		{
			name:  "throttle configure",
			wire:  []byte{0x0b, 0xff, 0x00, 0x13, 0x00, 0x00, 0x13, 0x88, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03},
			flags: 0,
			want: protocol.ThrottleConfigure{
				Header: protocol.CommandHeader{
					Command:                protocol.CommandThrottleConfigure,
					ChannelID:              0xff,
					ReliableSequenceNumber: 19,
				},
				PacketThrottleInterval:     5000,
				PacketThrottleAcceleration: 2,
				PacketThrottleDeceleration: 3,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cmd, flags, n, err := protocol.ParseCommand(tt.wire)
			if err != nil {
				t.Fatal(err)
			}
			if flags != tt.flags {
				t.Fatalf("flags = 0x%02x, want 0x%02x", flags, tt.flags)
			}
			if n != len(tt.wire) {
				t.Fatalf("consumed = %d, want %d", n, len(tt.wire))
			}
			if !reflect.DeepEqual(cmd, tt.want) {
				t.Fatalf("command mismatch:\n got: %#v\nwant: %#v", cmd, tt.want)
			}
			if got := cmd.MarshalBinary(nil); !bytes.Equal(got, tt.wire) {
				t.Fatalf("marshal mismatch: %x != %x", got, tt.wire)
			}
		})
	}
}
