package lagrelay

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startEchoServer(t *testing.T) string {
	t.Helper()
	connection, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	go func() {
		buffer := make([]byte, maxDatagramBytes)
		for {
			length, sender, readErr := connection.ReadFromUDPAddrPort(buffer)
			if readErr != nil {
				return
			}
			_, _ = connection.WriteToUDPAddrPort(buffer[:length], sender)
		}
	}()
	return connection.LocalAddr().String()
}

func startTestRelay(t *testing.T, config Config) (*Relay, *net.UDPConn) {
	t.Helper()
	if config.Upstream == "" {
		config.Upstream = startEchoServer(t)
	}
	if config.Seed == 0 {
		config.Seed = 7
	}
	relay, err := New(config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- relay.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-finished)
	})

	client, err := net.DialUDP("udp", nil, relay.Addr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return relay, client
}

func roundTrip(t *testing.T, client *net.UDPConn, payload []byte, timeout time.Duration) (time.Duration, bool) {
	t.Helper()
	started := time.Now()
	_, err := client.Write(payload)
	require.NoError(t, err)
	require.NoError(t, client.SetReadDeadline(started.Add(timeout)))
	buffer := make([]byte, maxDatagramBytes)
	length, err := client.Read(buffer)
	if err != nil {
		return 0, false
	}
	require.Equal(t, payload, buffer[:length])
	return time.Since(started), true
}

func TestRelay_ForwardsUnchangedWithNoImpairment(t *testing.T) {
	_, client := startTestRelay(t, Config{})

	_, delivered := roundTrip(t, client, []byte("hello"), 2*time.Second)

	assert.True(t, delivered)
}

func TestRelay_DelaysEachDirectionByItsOwnAmount(t *testing.T) {
	relay, client := startTestRelay(t, Config{
		InitialProfile: Profile{UpDelayMS: 60, DownDelayMS: 90},
	})

	elapsed, delivered := roundTrip(t, client, []byte("ping"), 3*time.Second)

	require.True(t, delivered)
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "round trip is the sum of both one-way delays")
	assert.Less(t, elapsed, 1500*time.Millisecond)
	stats := relay.Stats()
	assert.Equal(t, uint64(1), stats.UpForwarded)
	assert.Equal(t, uint64(1), stats.DownForwarded)
}

func TestRelay_AppliesProfileChangeToSubsequentDatagrams(t *testing.T) {
	relay, client := startTestRelay(t, Config{})
	_, delivered := roundTrip(t, client, []byte("before"), 2*time.Second)
	require.True(t, delivered)

	require.NoError(t, relay.SetProfile(Profile{UpDelayMS: 100, DownDelayMS: 100}))
	elapsed, delivered := roundTrip(t, client, []byte("after"), 3*time.Second)

	require.True(t, delivered)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
}

func TestRelay_PreservesOrderUnderJitter(t *testing.T) {
	_, client := startTestRelay(t, Config{
		InitialProfile: Profile{UpDelayMS: 5, DownDelayMS: 5, JitterMS: 40},
	})
	const datagrams = 200

	for sequence := range uint32(datagrams) {
		payload := make([]byte, 4)
		binary.BigEndian.PutUint32(payload, sequence)
		_, err := client.Write(payload)
		require.NoError(t, err)
	}

	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	buffer := make([]byte, maxDatagramBytes)
	for expected := range uint32(datagrams) {
		length, err := client.Read(buffer)
		require.NoError(t, err)
		require.Equal(t, 4, length)
		require.Equal(t, expected, binary.BigEndian.Uint32(buffer[:length]), "jitter must not reorder a direction")
	}
}

func TestRelay_DropsEverythingAtFullLoss(t *testing.T) {
	relay, client := startTestRelay(t, Config{
		InitialProfile: Profile{LossPercent: 100},
	})

	_, delivered := roundTrip(t, client, []byte("lost"), 300*time.Millisecond)

	assert.False(t, delivered)
	stats := relay.Stats()
	assert.Equal(t, uint64(1), stats.UpDropped)
	assert.Zero(t, stats.UpForwarded)
}

func TestRelay_GivesEachClientItsOwnUpstreamPeer(t *testing.T) {
	relay, first := startTestRelay(t, Config{})
	second, err := net.DialUDP("udp", nil, relay.Addr().(*net.UDPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	_, firstDelivered := roundTrip(t, first, []byte("first"), 2*time.Second)
	_, secondDelivered := roundTrip(t, second, []byte("second"), 2*time.Second)

	assert.True(t, firstDelivered)
	assert.True(t, secondDelivered)
	assert.Equal(t, 2, relay.Stats().Sessions)
}

func TestRelay_ReapsIdleSessions(t *testing.T) {
	relay, client := startTestRelay(t, Config{
		IdleTimeout: 30 * time.Millisecond,
	})

	_, delivered := roundTrip(t, client, []byte("active"), 2*time.Second)
	require.True(t, delivered)
	require.Equal(t, 1, relay.Stats().Sessions)

	require.Eventually(t, func() bool {
		return relay.Stats().Sessions == 0
	}, time.Second, 10*time.Millisecond)
}

func TestRelay_CloseIsIdempotentBeforeRun(t *testing.T) {
	relay, err := New(Config{Upstream: "127.0.0.1:9"})
	require.NoError(t, err)
	assert.NotNil(t, relay.UpstreamAddr())
	require.NoError(t, relay.Close())
	require.NoError(t, relay.Close())
}

func TestNew_RequiresUpstream(t *testing.T) {
	_, err := New(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upstream")
}

func TestProfile_RefusesValuesOutsideRange(t *testing.T) {
	cases := map[string]Profile{
		"negative up delay": {UpDelayMS: -1},
		"excessive down":    {DownDelayMS: MaxDelayMS + 1},
		"negative jitter":   {JitterMS: -1},
		"nan jitter":        {JitterMS: math.NaN()},
		"loss below zero":   {LossPercent: -0.1},
		"loss above 100":    {LossPercent: 101},
		"nan loss":          {LossPercent: math.NaN()},
	}
	for name, profile := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, profile.Validate())
		})
	}
}

func TestPresets_AreValidAndSorted(t *testing.T) {
	names := PresetNames()
	require.NotEmpty(t, names)
	assert.IsIncreasing(t, names)

	for _, name := range names {
		profile, found := Preset(name)
		require.True(t, found)
		require.NoError(t, profile.Validate())
	}

	_, found := Preset("nonexistent")
	assert.False(t, found)
}
