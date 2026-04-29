package peer

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/testsupport"
)

func TestFakeSocketReadsScriptedInboundDatagramsAndErrors(t *testing.T) {
	sock := testsupport.NewFakeSocket()
	firstAddr := netip.MustParseAddrPort("127.0.0.1:9001")
	secondAddr := netip.MustParseAddrPort("127.0.0.1:9002")
	readErr := errors.New("read failed")

	firstPayload := []byte("first")
	sock.QueueInbound(firstAddr, firstPayload)
	firstPayload[0] = 'X'
	sock.QueueReadError(readErr)
	sock.QueueInbound(secondAddr, []byte("second"))

	buf := make([]byte, 16)

	n, addr, err := sock.ReadPacket(context.Background(), buf)
	if err != nil {
		t.Fatalf("first ReadPacket() error = %v", err)
	}
	if addr.AddrPort() != firstAddr {
		t.Fatalf("first ReadPacket() addr = %v, want %v", addr, firstAddr)
	}
	if got := string(buf[:n]); got != "first" {
		t.Fatalf("first ReadPacket() payload = %q", got)
	}

	n, addr, err = sock.ReadPacket(context.Background(), buf)
	if !errors.Is(err, readErr) {
		t.Fatalf("second ReadPacket() error = %v, want %v", err, readErr)
	}
	if n != 0 {
		t.Fatalf("second ReadPacket() n = %d", n)
	}
	if addr != (core.Address{}) {
		t.Fatalf("second ReadPacket() addr = %v, want zero", addr)
	}

	n, addr, err = sock.ReadPacket(context.Background(), buf)
	if err != nil {
		t.Fatalf("third ReadPacket() error = %v", err)
	}
	if addr.AddrPort() != secondAddr {
		t.Fatalf("third ReadPacket() addr = %v, want %v", addr, secondAddr)
	}
	if got := string(buf[:n]); got != "second" {
		t.Fatalf("third ReadPacket() payload = %q", got)
	}

	n, addr, err = sock.ReadPacket(context.Background(), buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("fourth ReadPacket() error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Fatalf("fourth ReadPacket() n = %d", n)
	}
	if addr != (core.Address{}) {
		t.Fatalf("fourth ReadPacket() addr = %v, want zero", addr)
	}
}
