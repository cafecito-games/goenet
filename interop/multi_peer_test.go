package interop_test

import (
	"strconv"
	"strings"
	"testing"
)

func TestMultipleCClientsConnectToOneGoHost(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "multi_client_connect")

	host := mustListenEngineHost(t)
	client := startScenario(t, "multi_client_connect",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--peer-count", "3",
	)

	peers := waitForEngineConnectCount(t, host.Engine, 3)
	if len(peers) != 3 {
		t.Fatalf("connect count = %d, want 3", len(peers))
	}

	output := mustWaitProcessSuccess(t, client)
	for _, marker := range []string{"CONNECT 0", "CONNECT 1", "CONNECT 2"} {
		if !strings.Contains(output, marker) {
			t.Fatalf("client output missing %q:\n%s", marker, output)
		}
	}
}

func TestGoBroadcastReachesAllCClients(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "broadcast_receive")

	host := mustListenPublicHost(t)
	client := startScenario(t, "broadcast_receive",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--peer-count", "3",
		"--expect", "broadcast-payload",
	)

	peers := waitForPublicConnectCount(t, host.Host, 3)
	if len(peers) != 3 {
		t.Fatalf("connect count = %d, want 3", len(peers))
	}

	mustBroadcastReliablePacket(t, host.Host, "broadcast-payload")

	output := mustWaitProcessSuccess(t, client)
	for _, marker := range []string{
		"CONNECT 0",
		"CONNECT 1",
		"CONNECT 2",
		"RECEIVE 0 broadcast-payload",
		"RECEIVE 1 broadcast-payload",
		"RECEIVE 2 broadcast-payload",
	} {
		if !strings.Contains(output, marker) {
			t.Fatalf("client output missing %q:\n%s", marker, output)
		}
	}
}
