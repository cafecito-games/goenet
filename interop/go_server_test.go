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
