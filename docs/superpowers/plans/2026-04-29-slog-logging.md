# slog Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** add opt-in, component-tagged `slog` logging to `goenet` with a silent default, no repeated nil checks, and representative host/engine/socket coverage.

**Architecture:** extend public and internal config with `*slog.Logger`, normalize omitted loggers once to a discard-backed no-op logger, and store derived child loggers per subsystem using flat `component` tags. Keep logging observational: log lifecycle, error, and notable control-flow decisions without changing protocol semantics or dumping payload contents.

**Tech Stack:** Go 1.26, standard library `log/slog`, existing `go test` suite, repo pre-commit hooks.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `pkg/config.go` | Add public `Logger *slog.Logger` config field and round-trip it through config translation. |
| `pkg/host.go` | Normalize/stash logger on public host, derive `component=host`, and log host-level lifecycle/orchestration events. |
| `pkg/host_test.go` | Public API tests for logger config acceptance, silent default, and host-tagged records. |
| `pkg/readme_test.go` | Ensure README documents logger support. |
| `internal/core/types.go` | Extend internal config with logger field and keep default config behavior unchanged. |
| `internal/core/logger.go` | Shared helper for no-op logger normalization and `component` child logger derivation. |
| `internal/engine/host.go` | Store `component=engine` logger and log major lifecycle/state events. |
| `internal/engine/receive.go` | Log notable receive-path decisions such as drops, rejects, and timeout-related behavior. |
| `internal/engine/send.go` | Log notable send/flush decisions such as flush budget exhaustion and write errors bubbling up. |
| `internal/engine/host_test.go` | Engine logging tests using a capture handler. |
| `internal/socket/udp.go` | Store `component=socket` logger and log low-level non-timeout read/write failures plus suppressed ICMP behavior. |
| `internal/socket/udp_test.go` | Socket logging tests using a capture handler. |
| `README.md` | Add one short logging example and silent-default note. |

### Task 1: Add Logger Config Plumbing

**Files:**
- Create: `internal/core/logger.go`
- Modify: `pkg/config.go`
- Modify: `internal/core/types.go`
- Modify: `pkg/host.go`
- Test: `pkg/host_test.go`

- [x] **Step 1: Write the failing public config/logger tests**

Add focused tests that prove a configured logger is preserved and omitted logging stays non-panicking:

```go
func TestConfigRoundTripsLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	host, sock := newConfiguredTestHost(Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	})
	_ = sock

	if got := host.Config().Logger; got != logger {
		t.Fatalf("Config().Logger = %p, want %p", got, logger)
	}
}

func TestDefaultConfigKeepsLoggerNilForCallers(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Logger != nil {
		t.Fatal("DefaultConfig().Logger should be nil for callers")
	}
}
```

- [x] **Step 2: Run the focused public logger tests to verify they fail**

Run: `go test ./pkg -run 'TestConfigRoundTripsLogger|TestDefaultConfigKeepsLoggerNilForCallers'`

Expected: FAIL with compile errors because `Config.Logger` does not exist yet.

- [x] **Step 3: Add logger fields and normalization plumbing**

Create a shared internal helper and wire logger fields through public and internal config:

`internal/core/logger.go`

```go
package core

import (
	"io"
	"log/slog"
)

func normalizeLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func ComponentLogger(logger *slog.Logger, component string) *slog.Logger {
	return normalizeLogger(logger).With("component", component)
}
```

`pkg/config.go`

```go
import (
	"log/slog"

	"github.com/cafecito-games/goenet/internal/core"
)

type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
	Checksum           Checksummer
	Compressor         Compressor
	Intercept          Interceptor
	Logger             *slog.Logger
}
```

`pkg/config.go` in `toCoreConfig` / `fromCoreConfig`

```go
if cfg.Logger != nil {
	coreCfg.Logger = cfg.Logger
}

return Config{
	PeerCount:          cfg.PeerCount,
	ChannelLimit:       cfg.ChannelLimit,
	MTU:                cfg.MTU,
	MaximumPacketSize:  cfg.MaximumPacketSize,
	MaximumWaitingData: cfg.MaximumWaitingData,
	Logger:             cfg.Logger,
}
```

`internal/core/types.go`

```go
import (
	"log/slog"
	"net/netip"
)

type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
	Checksum           Checksummer
	Compressor         Compressor
	Intercept          Interceptor
	Logger             *slog.Logger
}
```

`pkg/host.go` in `newHostWithSocket`

```go
coreCfg := toCoreConfig(cfg)
normalized := fromCoreConfig(coreCfg)
normalized.Checksum = cfg.Checksum
normalized.Compressor = cfg.Compressor
normalized.Intercept = cfg.Intercept
normalized.Logger = cfg.Logger
```

- [x] **Step 4: Run the focused public logger tests to verify they pass**

Run: `go test ./pkg -run 'TestConfigRoundTripsLogger|TestDefaultConfigKeepsLoggerNilForCallers'`

Expected: PASS

- [x] **Step 5: Commit the config plumbing**

```bash
git add pkg/config.go pkg/host.go pkg/host_test.go internal/core/types.go internal/core/logger.go
git commit -m "feat: add slog config plumbing"
```

### Task 2: Add Host-Level Logging And Public Log Capture Tests

**Files:**
- Modify: `pkg/host.go`
- Modify: `pkg/host_test.go`
- Test: `pkg/host_test.go`

- [x] **Step 1: Write the failing host logging tests**

Add a tiny capture handler in `pkg/host_test.go` and assert host-tagged logs appear for representative lifecycle events:

```go
type capturedRecord struct {
	Message string
	Attrs   map[string]any
}

type captureHandler struct {
	records []capturedRecord
}

func newCaptureHandler() *captureHandler { return &captureHandler{} }

func TestListenLogsHostLifecycleWithComponentTag(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)

	host, err := Listen("127.0.0.1:0", Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Message == "host started" && r.Attrs["component"] == "host"
	}) {
		t.Fatal("missing host started log")
	}
}
```

- [x] **Step 2: Run the focused host logging test to verify it fails**

Run: `go test ./pkg -run TestListenLogsHostLifecycleWithComponentTag`

Expected: FAIL because `pkg/host.go` does not emit any log records yet.

- [x] **Step 3: Add host logger storage and representative host logs**

Update `pkg/host.go`:

```go
import "log/slog"

type Host struct {
	mu        sync.Mutex
	config    Config
	logger    *slog.Logger
	localAddr net.Addr
	// ...
}
```

Construct the component logger once:

```go
hostLogger := core.ComponentLogger(coreCfg.Logger, "host")

host := &Host{
	config:    normalized,
	logger:    hostLogger,
	socket:    sock,
	engine:    engine.NewHost(coreCfg, sock, 0),
	peers:     make(map[*peer.Peer]*Peer),
	startTime: time.Now(),
}
host.logger.Info("host started", "addr", host.localAddr)
```

Add representative logs in methods that add real host-layer context:

```go
func (h *Host) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}
	h.logger.Info("host closed")
	return h.socket.Close()
}
```

Use the existing normalized logger from `coreCfg` so host code never checks for nil.

- [x] **Step 4: Run the focused host logging test to verify it passes**

Run: `go test ./pkg -run TestListenLogsHostLifecycleWithComponentTag`

Expected: PASS

- [x] **Step 5: Commit the host logging layer**

```bash
git add pkg/host.go pkg/host_test.go
git commit -m "feat: add host component slog logging"
```

### Task 3: Add Engine Logging For Notable Decisions

**Files:**
- Modify: `internal/engine/host.go`
- Modify: `internal/engine/receive.go`
- Modify: `internal/engine/send.go`
- Modify: `internal/engine/host_test.go`
- Test: `internal/engine/host_test.go`

- [x] **Step 1: Write the failing engine logging tests**

Add focused engine tests with a capture logger on `core.Config`:

```go
type capturedRecord struct {
	Message string
	Attrs   map[string]any
}

type captureHandler struct {
	records []capturedRecord
}

func newCaptureHandler() *captureHandler { return &captureHandler{} }

func TestConnectLogsEngineComponent(t *testing.T) {
	handler := newCaptureHandler()
	host := NewHost(core.Config{
		PeerCount:    1,
		ChannelLimit: 1,
		Logger:       slog.New(handler),
	}, testsupport.NewFakeSocket(), 77)

	_, err := host.Connect(mustAddress(t, "127.0.0.1:9001"), 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	if !handler.Contains(func(r capturedRecord) bool {
		return r.Attrs["component"] == "engine" && r.Message == "peer connect queued"
	}) {
		t.Fatal("missing engine connect log")
	}
}
```

- [x] **Step 2: Run the focused engine logging tests to verify they fail**

Run: `go test ./internal/engine -run TestConnectLogsEngineComponent`

Expected: FAIL because no engine log records are emitted yet.

- [x] **Step 3: Add engine logger storage and representative decision logs**

Update `internal/engine/host.go`:

```go
import "log/slog"

type Host struct {
	config  core.Config
	logger  *slog.Logger
	socket  socket.DatagramSocket
	// ...
}
```

Construct the child logger once:

```go
host := &Host{
	config:        cfg,
	logger:        core.ComponentLogger(cfg.Logger, "engine"),
	socket:        sock,
	dispatchSet:   make(map[*peer.Peer]struct{}),
	runtime:       make(map[*peer.Peer]*peerRuntime),
	nextConnectID: randomConnectIDSeed(),
}
```

Emit representative records only where behavior changes matter:

```go
h.logger.Info("peer connect queued", "peer_id", raw.IncomingPeerID, "addr", address.AddrPort())
h.logger.Warn("flush budget exhausted", "peer_id", p.IncomingPeerID)
h.logger.Warn("dropping packet", "peer_id", p.IncomingPeerID, "reason", "channel out of range")
h.logger.Info("peer timed out", "peer_id", p.IncomingPeerID, "state", p.State)
```

Prefer `Warn` or `Error` for anomalous decisions, `Info` for lifecycle/state transitions.

- [x] **Step 4: Run the focused engine logging tests to verify they pass**

Run: `go test ./internal/engine -run TestConnectLogsEngineComponent`

Expected: PASS

- [x] **Step 5: Commit the engine logging layer**

```bash
git add internal/engine/host.go internal/engine/receive.go internal/engine/send.go internal/engine/host_test.go
git commit -m "feat: add engine component slog logging"
```

### Task 4: Add Socket Logging And README Coverage

**Files:**
- Modify: `internal/socket/udp.go`
- Modify: `internal/socket/udp_test.go`
- Modify: `README.md`
- Modify: `pkg/readme_test.go`
- Test: `internal/socket/udp_test.go`
- Test: `pkg/readme_test.go`

- [x] **Step 1: Write the failing socket and README tests**

Add a socket logging test that asserts low-level failures are tagged:

```go
func TestWritePacketLogsSocketComponentOnWriteError(t *testing.T) {
	handler := newCaptureHandler()
	logger := slog.New(handler)
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	socket := NewUDP(conn, logger)

	_, err = socket.WritePacket(context.Background(), mustAddress(t, "127.0.0.1:9001"), []byte("abc"))
	if err == nil {
		t.Fatal("expected write error")
	}
	if !handler.Contains(func(r capturedRecord) bool {
		return r.Attrs["component"] == "socket" && r.Attrs["err"] != nil
	}) {
		t.Fatal("missing socket error log")
	}
}
```

Extend README coverage:

```go
required := []string{
	"Logger",
	"slog.Default()",
	"omitting Logger keeps the library silent",
}
```

- [x] **Step 2: Run the focused socket and README tests to verify they fail**

Run: `go test ./internal/socket -run TestWritePacketLogsSocketComponentOnWriteError`

Run: `go test ./pkg -run TestREADMETracksCurrentPublicAPI`

Expected: FAIL because socket logging and README logger docs do not exist yet.

- [x] **Step 3: Add socket logger wiring and README logging example**

Update `internal/socket/udp.go` to accept/store a logger:

```go
type UDP struct {
	conn   *net.UDPConn
	logger *slog.Logger
}

func NewUDP(conn *net.UDPConn, logger *slog.Logger) *UDP {
	return &UDP{
		conn:   conn,
		logger: core.ComponentLogger(logger, "socket"),
	}
}
```

Log only meaningful I/O outcomes:

```go
if errors.Is(err, syscall.ECONNREFUSED) {
	s.logger.Debug("suppressing conn refused on read")
	continue
}
s.logger.Error("socket write failed", "addr", addr.AddrPort(), "bytes", len(payload), "err", err)
```

Update call sites in `pkg/host.go` to pass `cfg.Logger` into `NewUDP`.

Update README with a short example:

```go
logger := slog.Default()

host, err := goenet.Listen("127.0.0.1:0", goenet.Config{
	PeerCount:    4,
	ChannelLimit: 1,
	Logger:       logger,
})
```

And add one sentence: `If Logger is omitted, goenet remains silent.`

- [x] **Step 4: Run the focused socket and README tests to verify they pass**

Run: `go test ./internal/socket -run TestWritePacketLogsSocketComponentOnWriteError`

Run: `go test ./pkg -run TestREADMETracksCurrentPublicAPI`

Expected: PASS

- [x] **Step 5: Commit the socket/docs layer**

```bash
git add internal/socket/udp.go internal/socket/udp_test.go README.md pkg/readme_test.go pkg/host.go
git commit -m "feat: add socket slog logging and docs"
```

### Task 5: Full Verification And Cleanup

**Files:**
- Modify: `pkg/host_test.go`
- Modify: `internal/engine/host_test.go`
- Modify: `internal/socket/udp_test.go`
- Modify: `README.md`
- Test: `./...`

- [x] **Step 1: Add any remaining focused regression assertions discovered during implementation**

Keep only narrow assertions that prove:

```go
func TestDefaultConfigWithoutLoggerStillConnects(t *testing.T) {
	client, err := NewHost(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
}
```

- [x] **Step 2: Run the full suite**

Run: `go test ./...`

Expected: PASS across `pkg`, `internal/engine`, `internal/socket`, and the existing interop suite.

- [x] **Step 3: Run repo checks that the pre-commit hooks expect**

Run: `task fmt:check`

Run: `task lint`

Run: `task tidy:check`

Expected: PASS

- [x] **Step 4: Commit the final verification adjustments**

```bash
git add README.md pkg/host_test.go internal/engine/host_test.go internal/socket/udp_test.go
git commit -m "test: verify slog logging integration"
```
