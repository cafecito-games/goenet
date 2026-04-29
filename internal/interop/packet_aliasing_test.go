package interop_test

import (
	"context"
	"strings"
	"testing"

	goenet "github.com/cafecito-games/goenet/pkg"
)

// TestPacketDataBufferReuseSafeAcrossInterop verifies the M1 fix: goenet copies
// Packet.Data at the public boundary so the C peer observes the bytes that were
// in the buffer at Send time, not whatever the caller mutated it to afterward.
// Without the copy, retransmits and downstream wire bytes would track the
// caller's slice mutations — silently corrupting payloads.
func TestPacketDataBufferReuseSafeAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "c_server_reliable_exchange")

	const original = "go-client->c-server"
	server, port := startReadyScenarioOnEphemeralPort(t, "c_server_reliable_exchange",
		"--send", "c-server->go-client",
		"--expect", original,
	)

	host := mustNewPublicHost(t)
	peer := mustConnectPublicHost(t, host, port)
	waitForPublicEventType(t, host, goenet.EventConnect)
	waitForPublicPayload(t, host, "c-server->go-client")

	// Send a reliable packet, then immediately overwrite the caller buffer
	// with a different payload. With the fix in place the C server must see
	// `original`; without it, the buffer the engine retains for retransmits
	// would now hold "MUTATED-BUFFER..." and the C server would either log a
	// mismatch or never report `original`.
	buf := []byte(original)
	if err := peer.Send(0, &goenet.Packet{Data: buf, Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}
	for i := range buf {
		buf[i] = 'X'
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	output := mustWaitProcessSuccess(t, server)
	if !strings.Contains(output, "RECEIVE "+original) {
		t.Fatalf("server did not see %q despite caller buffer reuse:\n%s", original, output)
	}
	if strings.Contains(output, "RECEIVE XXXXXXXX") {
		t.Fatalf("server observed mutated bytes; aliasing copy missing:\n%s", output)
	}
}
