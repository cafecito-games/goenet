package interop_test

import (
	"strings"
	"testing"

	"github.com/cafecito-games/goenet"
)

func TestGoClientReliableExchange(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "c_server_reliable_exchange")

	server, port := startReadyScenarioOnEphemeralPort(t, "c_server_reliable_exchange",
		"--send", "c-server->go-client",
		"--expect", "go-client->c-server",
	)

	host := mustNewPublicHost(t)
	peer := mustConnectPublicHost(t, host, port)
	_ = peer
	waitForPublicEventType(t, host, goenet.EventConnect)
	waitForPublicPayload(t, host, "c-server->go-client")
	mustSendPublicReliablePacket(t, host, peer, "go-client->c-server")
	output := mustWaitProcessSuccess(t, server)
	if !strings.Contains(output, "CONNECT") || !strings.Contains(output, "RECEIVE go-client->c-server") {
		t.Fatalf("server output mismatch:\n%s", output)
	}
}
