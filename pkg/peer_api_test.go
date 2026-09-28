package goenet_test

import goenet "github.com/cafecito-games/goenet/pkg"

type consumerPeer interface {
	goenet.PeerSender
	Label() string
}

type fakeConsumerPeer struct{}

func (*fakeConsumerPeer) Send(uint8, *goenet.Packet) error { return nil }

func (*fakeConsumerPeer) Label() string { return "fake" }

var (
	_ goenet.PeerSender = (*goenet.Peer)(nil)
	_ consumerPeer      = (*fakeConsumerPeer)(nil)
)
