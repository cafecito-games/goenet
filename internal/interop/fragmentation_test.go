package interop_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
)

func TestFragmentedReliablePayloadAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "fragmented_reliable")

	payload := repeatedPayload("fragment-", 4096)
	reply := repeatedPayload("go-fragment-", 4096)
	host := mustListenEngineHost(t)
	client := startScenario(t, "fragmented_reliable",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", payload,
		"--expect", reply,
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	receive := mustReceiveEnginePacketWithFlags(t, host.Engine, payload, core.PacketFlagReliable)
	if receive.Peer != connect.Peer {
		t.Fatal("receive peer mismatch")
	}

	mustSendReliableEnginePacket(t, host.Engine, connect.Peer, reply)
	output := mustWaitProcessSuccess(t, client)
	if !strings.Contains(output, "RECEIVE "+reply) {
		t.Fatalf("client output missing fragmented reply:\n%s", output)
	}
}
