// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package lagrelay_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	goenet "github.com/cafecito-games/goenet/pkg"
	"github.com/cafecito-games/goenet/pkg/lagrelay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelay_ForwardsENetSessionUnderImpairment(t *testing.T) {
	serverConfig := goenet.DefaultConfig()
	serverConfig.PeerCount = 4
	serverConfig.ChannelLimit = 2

	server, err := goenet.Listen("127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	relay, err := lagrelay.New(lagrelay.Config{
		Upstream: server.LocalAddr().String(),
		InitialProfile: lagrelay.Profile{
			UpDelayMS:   15,
			DownDelayMS: 15,
			JitterMS:    10,
			LossPercent: 5,
		},
		Seed: 42,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-relayDone)
	}()

	clientConfig := goenet.DefaultConfig()
	clientConfig.ChannelLimit = 2
	client, err := goenet.NewHost(clientConfig)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	peer, err := client.Connect(relay.Addr().String(), 2, 0xBEEF)
	require.NoError(t, err)
	require.NoError(t, client.Flush(ctx))

	payload := bytes.Repeat([]byte("goenet-over-lagrelay-"), 120) // > MTU to exercise fragmentation too
	clientConnected := false
	echoReceived := false

	for !echoReceived {
		require.NoError(t, ctx.Err(), "timed out waiting for ENet echo through lagrelay")

		serverEvent, serverErr := server.Service(ctx, 5*time.Millisecond)
		require.NoError(t, serverErr)
		if serverEvent.Type == goenet.EventReceive {
			reply := &goenet.Packet{
				Data:  append([]byte(nil), serverEvent.Packet.Data...),
				Flags: goenet.PacketFlagReliable,
			}
			require.NoError(t, serverEvent.Peer.Send(serverEvent.ChannelID, reply))
			require.NoError(t, server.Flush(ctx))
		}

		clientEvent, clientErr := client.Service(ctx, 5*time.Millisecond)
		require.NoError(t, clientErr)
		switch clientEvent.Type {
		case goenet.EventConnect:
			clientConnected = true
			outbound := &goenet.Packet{
				Data:  payload,
				Flags: goenet.PacketFlagReliable,
			}
			require.NoError(t, peer.Send(0, outbound))
			require.NoError(t, client.Flush(ctx))
		case goenet.EventReceive:
			assert.Equal(t, payload, clientEvent.Packet.Data)
			echoReceived = true
		}
	}

	assert.True(t, clientConnected)
	stats := relay.Stats()
	assert.Positive(t, stats.UpForwarded)
	assert.Positive(t, stats.DownForwarded)
	assert.Equal(t, 1, stats.Sessions)
}
