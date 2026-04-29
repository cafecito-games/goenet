package socket

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
)

func TestReadPacketReturnsContextCanceledWithoutDeadline(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	sock := NewUDP(conn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, _, err := sock.ReadPacket(ctx, make([]byte, 32))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadPacket() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ReadPacket() did not return promptly for canceled context")
	}
}

func TestWritePacketReturnsContextCanceledWithoutDeadline(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	sock := NewUDP(conn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	addr, err := core.NewAddress(netip.MustParseAddrPort("127.0.0.1:9001"), 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = sock.WritePacket(ctx, addr, []byte("payload"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WritePacket() error = %v, want %v", err, context.Canceled)
	}
}

func TestAddressFromAddrPortPreservesNumericIPv6ScopeID(t *testing.T) {
	addr, err := AddressFromAddrPort(netip.MustParseAddrPort("[fe80::1%7]:7777"))
	if err != nil {
		t.Fatal(err)
	}

	if got := addr.AddrPort().String(); got != "[fe80::1]:7777" {
		t.Fatalf("AddrPort().String() = %q", got)
	}
	if got := addr.ScopeID(); got != 7 {
		t.Fatalf("ScopeID() = %d, want 7", got)
	}
}

func TestUDPAddrFromAddressRestoresNumericIPv6ScopeID(t *testing.T) {
	addr, err := core.NewAddress(netip.MustParseAddrPort("[fe80::1]:7777"), 7)
	if err != nil {
		t.Fatal(err)
	}

	udpAddr := UDPAddrFromAddress(addr)
	if got := udpAddr.IP.String(); got != "fe80::1" {
		t.Fatalf("UDPAddrFromAddress().IP = %q", got)
	}
	if udpAddr.Port != 7777 {
		t.Fatalf("UDPAddrFromAddress().Port = %d, want 7777", udpAddr.Port)
	}
	if udpAddr.Zone == "" {
		t.Fatal("UDPAddrFromAddress().Zone = empty, want populated scope zone")
	}
}
