package interop_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
)

func TestIdleConnectionStaysAliveAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "idle_ping")

	const idleWindow = 6200 * time.Millisecond

	host := mustListenEngineHost(t)
	client := startScenario(t, "idle_ping",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--timeout-ms", strconv.Itoa(int(idleWindow/time.Millisecond)),
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	if connect.Type != core.EventConnect {
		t.Fatalf("connect type = %v", connect.Type)
	}

	mustServiceEngineWithoutDisconnect(t, host.Engine, idleWindow+300*time.Millisecond)

	output := mustWaitProcessSuccess(t, client)
	for _, marker := range []string{"CONNECT", "IDLE_OK"} {
		if !strings.Contains(output, marker) {
			t.Fatalf("client output missing %q:\n%s", marker, output)
		}
	}
}

func TestReconnectCycleAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "reconnect_cycle")

	host := mustListenEngineHost(t)
	client := startScenario(t, "reconnect_cycle",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--expect", "reconnect-ready",
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	disconnect := waitForEngineEventType(t, host.Engine, core.EventDisconnect)
	if disconnect.Peer != connect.Peer {
		t.Fatal("disconnect peer mismatch")
	}
	secondConnect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	if secondConnect.Peer == nil {
		t.Fatal("second connect peer missing")
	}
	mustSendReliableEnginePacket(t, host.Engine, secondConnect.Peer, "reconnect-ready")

	output := mustWaitProcessSuccess(t, client)
	for _, marker := range []string{"CONNECT 1", "DISCONNECT 1", "CONNECT 2", "RECEIVE reconnect-ready"} {
		if !strings.Contains(output, marker) {
			t.Fatalf("client output missing %q:\n%s", marker, output)
		}
	}
}
