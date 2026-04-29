package interop_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
	"github.com/cafecito-games/goenet/internal/engine"
	"github.com/cafecito-games/goenet/internal/peer"
)

type interopConfig struct {
	ENETSourceDir string
}

func loadInteropConfig() (interopConfig, error) {
	envFile := strings.TrimSpace(os.Getenv("GOENET_INTEROP_ENVFILE"))
	if envFile == "" {
		envFile = filepath.Join(interopDirFromRuntime(), ".env")
	}

	fileValue := ""
	file, err := os.Open(envFile)
	if err == nil {
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			key, value, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			if strings.TrimSpace(key) != "ENET_SOURCE_DIR" {
				continue
			}

			fileValue = strings.TrimSpace(value)
			break
		}
	}

	dir := strings.TrimSpace(os.Getenv("ENET_SOURCE_DIR"))
	if dir == "" {
		dir = fileValue
	}
	if dir == "" {
		return interopConfig{}, fmt.Errorf("interop: ENET_SOURCE_DIR must be set in the environment or interop/.env")
	}

	return interopConfig{ENETSourceDir: dir}, nil
}

func interopDirFromRuntime() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func harnessBinaryPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "bin", name)
}

func scenarioSourcePath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "cases", name+".c")
}

func interopScriptPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "scripts", name)
}

func modTime(t *testing.T, path string) time.Time {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return info.ModTime()
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustSetModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()

	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
}

func mustCreateENETSourceDir(t *testing.T, sourceRoot string) string {
	t.Helper()

	root := t.TempDir()
	includeDir := filepath.Join(root, "include")
	if err := os.MkdirAll(includeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	mustCopyFile(t, filepath.Join(sourceRoot, "include", "enet.h"), filepath.Join(includeDir, "enet.h"))
	return root
}

func mustCopyFile(t *testing.T, src, dst string) {
	t.Helper()

	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

type engineHost struct {
	Engine *engine.Host
	socket *udpSocket
}

func mustListenEngineHost(t *testing.T) *engineHost {
	t.Helper()

	socket := newUDPSocket(t)
	return &engineHost{
		Engine: engine.NewHost(core.Config{
			PeerCount:    8,
			ChannelLimit: 1,
		}, socket, 0),
		socket: socket,
	}
}

func (h *engineHost) Port() int {
	return int(h.socket.LocalAddr().Port())
}

func waitForEngineEventType(t *testing.T, host *engine.Host, want core.EventType) engine.Event {
	t.Helper()

	deadline := time.Now().Add(scenarioTimeout)
	for time.Now().Before(deadline) {
		event, err := host.Service(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == want {
			return event
		}
	}

	t.Fatalf("timed out waiting for event type %v", want)
	return engine.Event{}
}

func mustSendReliableEnginePacket(t *testing.T, host *engine.Host, peer *peer.Peer, payload string) {
	t.Helper()

	if err := host.Send(peer, 0, &core.Packet{
		Data:  []byte(payload),
		Flags: core.PacketFlagReliable,
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
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
