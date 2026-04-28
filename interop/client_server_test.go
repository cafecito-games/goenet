package interop_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/engine"
)

const (
	cClientMessage = "c-client->go-server"
	goServerReply  = "go-server->c-client"
)

func TestGoServerTalksToCClient(t *testing.T) {
	binary := buildHarness(t)

	socket := newUDPSocket(t)
	host := engine.NewHost(core.Config{
		PeerCount:    8,
		ChannelLimit: 1,
	}, socket, 0)

	client := startHarness(t, binary, "client",
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", socket.LocalAddr().Port()),
		"--send", cClientMessage,
		"--expect", goServerReply,
	)

	connect := waitForEvent(t, host, 5*time.Second, func(event engine.Event) bool {
		return event.Type == core.EventConnect
	})
	if connect.Type != core.EventConnect {
		t.Fatalf("connect event type = %d", connect.Type)
	}

	receive := waitForEvent(t, host, 5*time.Second, func(event engine.Event) bool {
		return event.Type == core.EventReceive
	})
	if receive.Type != core.EventReceive {
		t.Fatalf("receive event type = %d", receive.Type)
	}
	if got := string(receive.Packet.Data); got != cClientMessage {
		t.Fatalf("received payload = %q, want %q", got, cClientMessage)
	}

	if err := host.Send(receive.Peer, 0, &core.Packet{
		Data:  []byte(goServerReply),
		Flags: core.PacketFlagReliable,
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	output := client.Wait(t, 5*time.Second)
	if !strings.Contains(output, "CONNECT") {
		t.Fatalf("client output missing CONNECT:\n%s", output)
	}
	if !strings.Contains(output, "RECEIVE "+goServerReply) {
		t.Fatalf("client output missing reply payload:\n%s", output)
	}
}

type udpSocket struct {
	conn *net.UDPConn
}

func newUDPSocket(t *testing.T) *udpSocket {
	t.Helper()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return &udpSocket{conn: conn}
}

func (s *udpSocket) LocalAddr() netip.AddrPort {
	return s.conn.LocalAddr().(*net.UDPAddr).AddrPort()
}

func (s *udpSocket) ReadPacket(ctx context.Context, buf []byte) (int, netip.AddrPort, error) {
	deadline := time.Now().Add(20 * time.Millisecond)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := s.conn.SetReadDeadline(deadline); err != nil {
		return 0, netip.AddrPort{}, err
	}

	n, addr, err := s.conn.ReadFromUDPAddrPort(buf)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return 0, netip.AddrPort{}, io.EOF
		}
		return 0, netip.AddrPort{}, err
	}

	return n, addr, nil
}

func (s *udpSocket) WritePacket(ctx context.Context, addr netip.AddrPort, payload []byte) (int, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := s.conn.SetWriteDeadline(deadline); err != nil {
			return 0, err
		}
	}

	return s.conn.WriteToUDPAddrPort(payload, addr)
}

func (s *udpSocket) Close() error {
	return s.conn.Close()
}

type harnessProcess struct {
	cmd    *exec.Cmd
	output *bytes.Buffer
}

func startHarness(t *testing.T, binary string, args ...string) *harnessProcess {
	t.Helper()

	cmd := exec.Command(binary, args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	return &harnessProcess{cmd: cmd, output: &output}
}

func (p *harnessProcess) Wait(t *testing.T, timeout time.Duration) string {
	t.Helper()

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- p.cmd.Wait()
	}()

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("harness exited with error: %v\n%s", err, p.output.String())
		}
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-waitErr
		t.Fatalf("timed out waiting for harness\n%s", p.output.String())
	}

	return p.output.String()
}

func buildHarness(t *testing.T) string {
	t.Helper()

	script := filepath.Join(interopDir(t), "build_c_harness.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatal(err)
	}

	output := filepath.Join(t.TempDir(), "enet-harness")
	cmd := exec.Command(script, output)
	cmd.Env = append(os.Environ(), "ENET_SOURCE_DIR=/Users/christian/CafecitoGames/enet")
	result, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build harness: %v\n%s", err, result)
	}

	return output
}

func interopDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func waitForEvent(t *testing.T, host *engine.Host, timeout time.Duration, match func(engine.Event) bool) engine.Event {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		event, err := host.Service(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if match(event) {
			return event
		}
	}

	t.Fatalf("timed out waiting for event after %s", timeout)
	return engine.Event{}
}
