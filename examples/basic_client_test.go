package examples_test

import (
	"fmt"

	"github.com/cafecito-games/goenet"
)

func Example_basicClientPacket() {
	packet := goenet.Packet{
		Data:  []byte("hello"),
		Flags: goenet.PacketFlagReliable,
	}

	event := goenet.Event{
		Type:      goenet.EventReceive,
		ChannelID: 0,
		Packet:    &packet,
	}

	fmt.Printf("event=%d reliable=%t bytes=%d\n",
		event.Type,
		packet.Flags&goenet.PacketFlagReliable != 0,
		len(event.Packet.Data),
	)
	// Output:
	// event=3 reliable=true bytes=5
}
