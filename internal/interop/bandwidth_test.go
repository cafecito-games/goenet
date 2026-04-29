package interop_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
)

// TestBandwidthNegotiationAdvertisedToCClient verifies the H5/H9 fix:
// goenet's handleConnect now writes the host's actual incoming/outgoing
// bandwidth caps into VerifyConnect (matches enet.h:1972-1973). A C peer
// receiving the verify-connect should observe those caps on its peer.
func TestBandwidthNegotiationAdvertisedToCClient(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "bandwidth_negotiation_client")

	const (
		serverIncoming uint32 = 32 * 1024
		serverOutgoing uint32 = 64 * 1024
	)

	host := mustListenEngineHost(t)
	host.Engine.BandwidthLimit(serverIncoming, serverOutgoing)

	client := startScenario(t, "bandwidth_negotiation_client",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
	)

	// Drive the engine until the C client logs CONNECT (which it logs
	// alongside BANDWIDTH). The verify-connect handshake completes within a
	// few service ticks; mustWaitProcessSuccess then collects the C output.
	if connect := waitForEngineEventType(t, host.Engine, core.EventConnect); connect.Peer == nil {
		t.Fatal("engine: expected non-nil connect peer")
	}

	output := mustWaitProcessSuccess(t, client)
	wantSubstr := "BANDWIDTH in=" + strconv.FormatUint(uint64(serverIncoming), 10) +
		" out=" + strconv.FormatUint(uint64(serverOutgoing), 10)
	if !strings.Contains(output, wantSubstr) {
		t.Fatalf("client did not see negotiated bandwidth %q in output:\n%s", wantSubstr, output)
	}
}
