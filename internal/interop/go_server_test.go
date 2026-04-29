package interop_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
)

func TestGoServerReliableExchange(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "go_server_reliable_exchange")

	host := mustListenEngineHost(t)
	client := startScenario(t, "go_server_reliable_exchange",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", "c-client->go-server",
		"--expect", "go-server->c-client",
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	if connect.Type != core.EventConnect {
		t.Fatalf("connect type = %v", connect.Type)
	}

	receive := waitForEngineEventType(t, host.Engine, core.EventReceive)
	if got := string(receive.Packet.Data); got != "c-client->go-server" {
		t.Fatalf("payload = %q", got)
	}

	mustSendReliableEnginePacket(t, host.Engine, receive.Peer, "go-server->c-client")
	output := mustWaitProcessSuccess(t, client)
	if !strings.Contains(output, "CONNECT") {
		t.Fatalf("client output missing CONNECT:\n%s", output)
	}
	if !strings.Contains(output, "RECEIVE go-server->c-client") {
		t.Fatalf("client output missing reply payload:\n%s", output)
	}
}

func TestReliableOrderingAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "reliable_ordering")

	host := mustListenEngineHost(t)
	client := startScenario(t, "reliable_ordering",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	wantInbound := []string{"c-order-1", "c-order-2", "c-order-3"}
	for _, want := range wantInbound {
		event := mustReceiveEnginePacketWithFlags(t, host.Engine, want, core.PacketFlagReliable)
		if event.Peer != connect.Peer {
			t.Fatalf("receive peer mismatch for %q", want)
		}
	}

	wantOutbound := []string{"go-order-1", "go-order-2", "go-order-3"}
	for _, payload := range wantOutbound {
		mustSendReliableEnginePacket(t, host.Engine, connect.Peer, payload)
	}

	output := mustWaitProcessSuccess(t, client)
	for _, payload := range wantOutbound {
		if !strings.Contains(output, "RECEIVE "+payload) {
			t.Fatalf("client output missing %q:\n%s", payload, output)
		}
	}
}

func TestUnreliableExchangeAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "unreliable_exchange")

	host := mustListenEngineHost(t)
	client := startScenario(t, "unreliable_exchange",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", "c-unreliable",
		"--expect", "go-unreliable",
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	receive := mustReceiveEnginePacketWithFlags(t, host.Engine, "c-unreliable", 0)
	if receive.Peer != connect.Peer {
		t.Fatal("receive peer mismatch")
	}

	mustSendEnginePacketWithFlags(t, host.Engine, connect.Peer, "go-unreliable", 0)

	output := mustWaitProcessSuccess(t, client)
	if !strings.Contains(output, "RECEIVE go-unreliable") {
		t.Fatalf("client output missing unreliable payload:\n%s", output)
	}
}
