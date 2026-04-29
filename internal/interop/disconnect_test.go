package interop_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	goenet "github.com/cafecito-games/goenet/pkg"
)

const (
	clientInitiatedDisconnectData = 0x11223344
	serverInitiatedDisconnectData = 0x55667788
	clientInitiatedReadyPayload   = "go-server-ready"
	serverInitiatedReadyPayload   = "go-client-ready"
)

func TestGoServerObservesClientInitiatedDisconnect(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "disconnect_client_initiated")

	host := mustListenEngineHost(t)
	client := startScenario(t, "disconnect_client_initiated",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--expect", clientInitiatedReadyPayload,
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	mustSendReliableEnginePacket(t, host.Engine, connect.Peer, clientInitiatedReadyPayload)
	disconnect := waitForEngineEventType(t, host.Engine, core.EventDisconnect)
	if disconnect.Peer != connect.Peer {
		t.Fatal("disconnect peer mismatch")
	}
	if disconnect.Data != clientInitiatedDisconnectData {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, clientInitiatedDisconnectData)
	}

	output := mustWaitProcessSuccess(t, client)
	if !strings.Contains(output, "CONNECT") {
		t.Fatalf("client output missing CONNECT:\n%s", output)
	}
	if !strings.Contains(output, "RECEIVE "+clientInitiatedReadyPayload) {
		t.Fatalf("client output missing ready payload:\n%s", output)
	}
	if !strings.Contains(output, "DISCONNECT_REQUEST "+strconv.FormatUint(uint64(clientInitiatedDisconnectData), 10)) {
		t.Fatalf("client output missing disconnect request:\n%s", output)
	}
	if !strings.Contains(output, "DISCONNECT") {
		t.Fatalf("client output missing disconnect completion:\n%s", output)
	}
}

func TestGoClientObservesServerInitiatedDisconnect(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "disconnect_server_initiated")

	server, port := startReadyScenarioOnEphemeralPort(t, "disconnect_server_initiated")

	host := mustNewPublicHost(t)
	peer := mustConnectPublicHost(t, host, port)
	_ = peer

	connect := waitForPublicEventType(t, host, goenet.EventConnect)
	mustSendPublicReliablePacket(t, host, connect.Peer, serverInitiatedReadyPayload)
	disconnect := waitForPublicEventType(t, host, goenet.EventDisconnect)
	if disconnect.Peer != connect.Peer {
		t.Fatal("disconnect peer mismatch")
	}
	if disconnect.Data != serverInitiatedDisconnectData {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, serverInitiatedDisconnectData)
	}

	output := mustWaitProcessSuccess(t, server)
	if !strings.Contains(output, "CONNECT") {
		t.Fatalf("server output missing CONNECT:\n%s", output)
	}
	if !strings.Contains(output, "RECEIVE "+serverInitiatedReadyPayload) {
		t.Fatalf("server output missing ready payload:\n%s", output)
	}
	if !strings.Contains(output, "DISCONNECT_REQUEST "+strconv.FormatUint(uint64(serverInitiatedDisconnectData), 10)) {
		t.Fatalf("server output missing disconnect request:\n%s", output)
	}
	if !strings.Contains(output, "DISCONNECT") {
		t.Fatalf("server output missing disconnect completion:\n%s", output)
	}
}
