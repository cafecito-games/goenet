# Interop Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dedicated `interop/` test harness that uses the local C ENet source tree as the source of truth and runs an incremental build plus an end-to-end Go test battery through `task interop:test`.

**Architecture:** The implementation splits the current inline C harness into real source files under `interop/`, with one C scenario per behavior and shared C support code for parsing, logging, and ENet setup. A Go test runner builds scenario binaries via an incremental script, launches them as subprocesses, and asserts `goenet` interoperability through public APIs where possible and internal APIs only when the public surface cannot yet express the scenario.

**Tech Stack:** Go 1.26, Task, POSIX shell, C99, local ENet source tree provided by `ENET_SOURCE_DIR`.

---

## File Structure

- Create: `interop/.env.example`
- Create: `interop/.gitignore`
- Create: `interop/bin/.gitkeep`
- Create: `interop/build/.gitkeep`
- Create: `interop/include/harness.h`
- Create: `interop/lib/harness.c`
- Create: `interop/cases/go_server_reliable_exchange.c`
- Create: `interop/cases/c_server_reliable_exchange.c`
- Create: `interop/cases/reliable_ordering.c`
- Create: `interop/cases/unreliable_exchange.c`
- Create: `interop/cases/disconnect_client_initiated.c`
- Create: `interop/cases/disconnect_server_initiated.c`
- Create: `interop/cases/fragmented_reliable.c`
- Create: `interop/cases/multi_client_connect.c`
- Create: `interop/cases/broadcast_receive.c`
- Create: `interop/cases/idle_ping.c`
- Create: `interop/cases/reconnect_cycle.c`
- Create: `interop/scripts/build_harness.sh`
- Create: `interop/scripts/resolve_env.sh`
- Create: `interop/harness_build_test.go`
- Create: `interop/harness_runner_test.go`
- Create: `interop/test_helpers_test.go`
- Create: `interop/go_server_test.go`
- Create: `interop/go_client_test.go`
- Create: `interop/disconnect_test.go`
- Create: `interop/fragmentation_test.go`
- Create: `interop/multi_peer_test.go`
- Create: `interop/idle_reconnect_test.go`
- Modify: `interop/README.md`
- Delete: `interop/build_c_harness.sh`
- Delete: `interop/client_server_test.go`
- Modify: `Taskfile.yml`

### Task 1: Bootstrap Interop Project Layout

**Files:**
- Create: `interop/.env.example`
- Create: `interop/.gitignore`
- Create: `interop/bin/.gitkeep`
- Create: `interop/build/.gitkeep`
- Modify: `interop/README.md`
- Test: `interop/harness_build_test.go`

- [ ] **Step 1: Write the failing build-path tests**

```go
package interop_test

import "testing"

func TestLoadInteropConfigRequiresENETSourceDir(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "")

	_, err := loadInteropConfig()
	if err == nil {
		t.Fatal("expected missing ENET_SOURCE_DIR error")
	}
}

func TestLoadInteropConfigUsesEnvFileOverride(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "/tmp/from-env")
	t.Setenv("GOENET_INTEROP_ENVFILE", "testdata/interop/env/basic.env")

	cfg, err := loadInteropConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ENETSourceDir != "/tmp/from-env" {
		t.Fatalf("ENETSourceDir = %q", cfg.ENETSourceDir)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./interop -run 'TestLoadInteropConfigRequiresENETSourceDir|TestLoadInteropConfigUsesEnvFileOverride' -count=1`
Expected: FAIL with undefined `loadInteropConfig`

- [ ] **Step 3: Write minimal config-loading implementation and bootstrap files**

```go
package interop_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type interopConfig struct {
	ENETSourceDir string
}

func loadInteropConfig() (interopConfig, error) {
	envPath := os.Getenv("GOENET_INTEROP_ENVFILE")
	if envPath == "" {
		envPath = filepath.Join(interopDirFromRuntime(), ".env")
	}

	if file, err := os.Open(envPath); err == nil {
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
			if strings.TrimSpace(key) == "ENET_SOURCE_DIR" && os.Getenv("ENET_SOURCE_DIR") == "" {
				_ = os.Setenv("ENET_SOURCE_DIR", strings.TrimSpace(value))
			}
		}
	}

	dir := strings.TrimSpace(os.Getenv("ENET_SOURCE_DIR"))
	if dir == "" {
		return interopConfig{}, fmt.Errorf("interop: ENET_SOURCE_DIR must be set in environment or interop/.env")
	}

	return interopConfig{ENETSourceDir: dir}, nil
}
```

`interop/.env.example`

```dotenv
ENET_SOURCE_DIR=/absolute/path/to/enet
```

`interop/.gitignore`

```gitignore
.env
bin/
build/
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./interop -run 'TestLoadInteropConfigRequiresENETSourceDir|TestLoadInteropConfigUsesEnvFileOverride' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/.env.example interop/.gitignore interop/bin/.gitkeep interop/build/.gitkeep interop/README.md interop/harness_build_test.go interop/test_helpers_test.go
git commit -m "test: bootstrap interop harness project layout"
```

### Task 2: Add Taskfile Wiring For `task interop:test`

**Files:**
- Modify: `Taskfile.yml`
- Create: `interop/scripts/resolve_env.sh`
- Test: `interop/harness_build_test.go`

- [ ] **Step 1: Write the failing task-integration tests**

```go
func TestResolveInteropEnvPrefersProcessEnv(t *testing.T) {
	t.Setenv("ENET_SOURCE_DIR", "/tmp/enet")

	cfg, err := loadInteropConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ENETSourceDir != "/tmp/enet" {
		t.Fatalf("ENETSourceDir = %q, want /tmp/enet", cfg.ENETSourceDir)
	}
}

func TestBuildHarnessCommandIncludesScenarioName(t *testing.T) {
	path := harnessBinaryPath("go_server_reliable_exchange")
	if got, want := filepath.Base(path), "go_server_reliable_exchange"; got != want {
		t.Fatalf("binary basename = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./interop -run 'TestResolveInteropEnvPrefersProcessEnv|TestBuildHarnessCommandIncludesScenarioName' -count=1`
Expected: FAIL with undefined `harnessBinaryPath`

- [ ] **Step 3: Write minimal task wiring and helpers**

`Taskfile.yml`

```yaml
  interop:test:
    desc: Build the interop C harness incrementally and run interop tests.
    cmds:
      - ./interop/scripts/build_harness.sh
      - go test ./interop -count=1
```

`interop/scripts/resolve_env.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

interop_dir="$(cd "$(dirname "$0")/.." && pwd)"
env_file="$interop_dir/.env"

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

if [[ -z "${ENET_SOURCE_DIR:-}" ]]; then
  echo "interop: ENET_SOURCE_DIR must be set in environment or interop/.env" >&2
  exit 1
fi

printf '%s\n' "$ENET_SOURCE_DIR"
```

`interop/test_helpers_test.go`

```go
func harnessBinaryPath(name string) string {
	return filepath.Join(interopDirFromRuntime(), "bin", name)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./interop -run 'TestResolveInteropEnvPrefersProcessEnv|TestBuildHarnessCommandIncludesScenarioName' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Taskfile.yml interop/scripts/resolve_env.sh interop/harness_build_test.go interop/test_helpers_test.go
git commit -m "test: wire task interop test entrypoint"
```

### Task 3: Replace The Inline C Builder With Real Shared C Sources

**Files:**
- Create: `interop/include/harness.h`
- Create: `interop/lib/harness.c`
- Create: `interop/cases/go_server_reliable_exchange.c`
- Create: `interop/scripts/build_harness.sh`
- Delete: `interop/build_c_harness.sh`
- Test: `interop/harness_build_test.go`

- [ ] **Step 1: Write the failing harness-build tests**

```go
func TestBuildHarnessProducesScenarioBinary(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat binary: %v\n%s", err, output)
	}
}

func TestBuildHarnessRejectsUnknownScenario(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	_, err := exec.Command(filepath.Join(interopDirFromRuntime(), "scripts", "build_harness.sh"), "does_not_exist").CombinedOutput()
	if err == nil {
		t.Fatal("expected unknown scenario build to fail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestBuildHarnessProducesScenarioBinary|TestBuildHarnessRejectsUnknownScenario' -count=1`
Expected: FAIL because `runBuildHarness` and scenario sources do not exist

- [ ] **Step 3: Write minimal shared C harness and build script**

`interop/include/harness.h`

```c
#ifndef GOENET_INTEROP_HARNESS_H
#define GOENET_INTEROP_HARNESS_H

#include <stdbool.h>
#include <stdint.h>

#define ENET_IMPLEMENTATION
#include "enet.h"

typedef struct {
    const char *host;
    uint16_t port;
    const char *send_payload;
    const char *expect_payload;
    int timeout_ms;
    int peer_count;
} harness_config;

bool harness_parse_client_args(int argc, char **argv, harness_config *cfg);
bool harness_parse_server_args(int argc, char **argv, harness_config *cfg);
void harness_log(const char *event, const char *value);
void harness_log_number(const char *event, uint32_t value);

#endif
```

`interop/lib/harness.c`

```c
#include "harness.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void harness_init_defaults(harness_config *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->host = "127.0.0.1";
    cfg->timeout_ms = 5000;
    cfg->peer_count = 1;
}

void harness_log(const char *event, const char *value) {
    if (value == NULL) {
        printf("%s\n", event);
    } else {
        printf("%s %s\n", event, value);
    }
    fflush(stdout);
}
```

`interop/cases/go_server_reliable_exchange.c`

```c
#include "harness.h"

int main(int argc, char **argv) {
    harness_config cfg;
    if (!harness_parse_client_args(argc, argv, &cfg)) {
        return 2;
    }
    if (enet_initialize() != 0) {
        return 1;
    }
    /* Existing client behavior from build_c_harness.sh moves here. */
    enet_deinitialize();
    return 0;
}
```

`interop/scripts/build_harness.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "$0")/../.." && pwd)"
interop_dir="$root_dir/interop"
enet_root="$("$interop_dir/scripts/resolve_env.sh")"
bin_dir="$interop_dir/bin"
scenario="${1:-all}"

mkdir -p "$bin_dir" "$interop_dir/build"

build_one() {
  local name="$1"
  local source="$interop_dir/cases/$name.c"
  local output="$bin_dir/$name"
  if [[ ! -f "$source" ]]; then
    echo "interop: unknown scenario $name" >&2
    exit 1
  fi
  cc -std=c99 -Wall -Wextra -Wno-unused-parameter \
    -I"$interop_dir/include" -I"$enet_root/include" \
    "$interop_dir/lib/harness.c" "$source" -o "$output"
}

if [[ "$scenario" == "all" ]]; then
  for source in "$interop_dir"/cases/*.c; do
    build_one "$(basename "$source" .c)"
  done
else
  build_one "$scenario"
fi
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestBuildHarnessProducesScenarioBinary|TestBuildHarnessRejectsUnknownScenario' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/include/harness.h interop/lib/harness.c interop/cases/go_server_reliable_exchange.c interop/scripts/build_harness.sh interop/harness_build_test.go
git rm interop/build_c_harness.sh
git commit -m "test: extract shared c interop harness"
```

### Task 4: Make The C Build Incremental

**Files:**
- Modify: `interop/scripts/build_harness.sh`
- Test: `interop/harness_build_test.go`

- [ ] **Step 1: Write the failing incremental-build tests**

```go
func TestBuildHarnessSkipsUnchangedScenario(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, _ := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	before := modTime(t, path)
	time.Sleep(20 * time.Millisecond)
	_, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	after := modTime(t, path)
	if !strings.Contains(output, "SKIP go_server_reliable_exchange") {
		t.Fatalf("build output = %q", output)
	}
	if !before.Equal(after) {
		t.Fatalf("mod time changed: before=%v after=%v", before, after)
	}
}

func TestBuildHarnessRebuildsWhenScenarioChanges(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)

	path, _ := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	before := modTime(t, path)
	source := scenarioSourcePath("go_server_reliable_exchange")
	original := mustReadFile(t, source)
	t.Cleanup(func() { mustWriteFile(t, source, original) })
	time.Sleep(20 * time.Millisecond)
	mustWriteFile(t, source, original+"\n")
	_, output := runBuildHarness(t, cfg, "go_server_reliable_exchange")
	after := modTime(t, path)
	if !strings.Contains(output, "BUILD go_server_reliable_exchange") {
		t.Fatalf("build output = %q", output)
	}
	if !after.After(before) {
		t.Fatalf("mod time did not advance: before=%v after=%v", before, after)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestBuildHarnessSkipsUnchangedScenario|TestBuildHarnessRebuildsWhenScenarioChanges' -count=1`
Expected: FAIL because the build script always recompiles

- [ ] **Step 3: Write minimal incremental rebuild logic**

`interop/scripts/build_harness.sh`

```bash
needs_rebuild() {
  local output="$1"
  shift

  if [[ ! -f "$output" ]]; then
    return 0
  fi

  for dep in "$@"; do
    if [[ "$dep" -nt "$output" ]]; then
      return 0
    fi
  done

  return 1
}

build_one() {
  local name="$1"
  local source="$interop_dir/cases/$name.c"
  local output="$bin_dir/$name"
  local deps=(
    "$source"
    "$interop_dir/lib/harness.c"
    "$interop_dir/include/harness.h"
    "$interop_dir/scripts/build_harness.sh"
    "$enet_root/include/enet.h"
  )

  if ! needs_rebuild "$output" "${deps[@]}"; then
    echo "SKIP $name"
    return
  fi

  cc -std=c99 -Wall -Wextra -Wno-unused-parameter \
    -I"$interop_dir/include" -I"$enet_root/include" \
    "$interop_dir/lib/harness.c" "$source" -o "$output"
  echo "BUILD $name"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestBuildHarnessSkipsUnchangedScenario|TestBuildHarnessRebuildsWhenScenarioChanges' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/scripts/build_harness.sh interop/harness_build_test.go
git commit -m "test: add incremental c harness builds"
```

### Task 5: Port The Existing Passing Scenario To The New Runner

**Files:**
- Create: `interop/harness_runner_test.go`
- Create: `interop/test_helpers_test.go`
- Create: `interop/go_server_test.go`
- Delete: `interop/client_server_test.go`
- Modify: `interop/cases/go_server_reliable_exchange.c`
- Test: `interop/go_server_test.go`

- [ ] **Step 1: Write the failing end-to-end test**

```go
func TestGoServerReliableExchange(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "go_server_reliable_exchange")

	host := mustListenEngineHost(t)
	client := startScenario(t, "go_server_reliable_exchange",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", "c-client->go-server",
		"--expect", "go-server->c-client",
	)

	connect := waitForEngineEventType(t, host.Engine, core.EventConnect)
	if connect.Type != core.EventConnect {
		t.Fatalf("connect type = %v", connect.Type)
	}

	receive := waitForEngineEventType(t, host.Engine, core.EventReceive)
	if got := string(receive.Packet.Data); got != "c-client->go-server" {
		t.Fatalf("payload = %q", got)
	}

	mustSendReliableEnginePacket(t, host.Engine, receive.Peer, "go-server->c-client")
	mustWaitProcessSuccess(t, client)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run TestGoServerReliableExchange -count=1`
Expected: FAIL with undefined runner helpers

- [ ] **Step 3: Write minimal runner helpers and scenario logic**

```go
type scenarioProcess struct {
	cmd    *exec.Cmd
	output bytes.Buffer
}

func startScenario(t *testing.T, name string, args ...string) *scenarioProcess {
	t.Helper()
	cmd := exec.Command(harnessBinaryPath(name), args...)
	var p scenarioProcess
	cmd.Stdout = &p.output
	cmd.Stderr = &p.output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.cmd = cmd
	return &p
}
```

`interop/cases/go_server_reliable_exchange.c`

```c
/* Move the current "client" behavior out of the heredoc and preserve CONNECT/RECEIVE logging. */
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run TestGoServerReliableExchange -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/harness_runner_test.go interop/test_helpers_test.go interop/go_server_test.go interop/cases/go_server_reliable_exchange.c
git rm interop/client_server_test.go
git commit -m "test: port go server reliable exchange interop case"
```

### Task 6: Add Go-Client Versus C-Server Coverage

**Files:**
- Create: `interop/cases/c_server_reliable_exchange.c`
- Create: `interop/go_client_test.go`
- Test: `interop/go_client_test.go`

- [ ] **Step 1: Write the failing Go-client test**

```go
func TestGoClientReliableExchange(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "c_server_reliable_exchange")

	port := reserveUDPPort(t)
	server := startScenario(t, "c_server_reliable_exchange",
		"--port", strconv.Itoa(port),
		"--send", "c-server->go-client",
		"--expect", "go-client->c-server",
	)

	host := mustNewPublicHost(t)
	peer := mustConnectPublicHost(t, host, port)
	waitForPublicEventType(t, host, goenet.EventTypeConnect)
	waitForPublicPayload(t, host, "c-server->go-client")
	mustSendPublicReliablePacket(t, peer, "go-client->c-server")
	mustWaitProcessSuccess(t, server)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run TestGoClientReliableExchange -count=1`
Expected: FAIL because the C server scenario and public-host helpers do not exist

- [ ] **Step 3: Write minimal scenario and public-host helpers**

```c
/* c_server_reliable_exchange.c contains the current "server" behavior with READY/CONNECT/RECEIVE logging. */
```

```go
func mustNewPublicHost(t *testing.T) *goenet.Host {
	t.Helper()
	host, err := goenet.NewHost(goenet.Config{PeerCount: 8, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	return host
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run TestGoClientReliableExchange -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/cases/c_server_reliable_exchange.c interop/go_client_test.go interop/test_helpers_test.go
git commit -m "test: add go client interop coverage"
```

### Task 7: Add Ordering, Unreliable, And Fragmentation Cases

**Files:**
- Create: `interop/cases/reliable_ordering.c`
- Create: `interop/cases/unreliable_exchange.c`
- Create: `interop/cases/fragmented_reliable.c`
- Create: `interop/fragmentation_test.go`
- Modify: `interop/go_server_test.go`
- Test: `interop/go_server_test.go`
- Test: `interop/fragmentation_test.go`

- [ ] **Step 1: Write the failing protocol-behavior tests**

```go
func TestReliableOrderingAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "reliable_ordering")

	host := mustListenEngineHost(t)
	client := startScenario(t, "reliable_ordering",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", "one,two,three",
		"--expect", "alpha,beta,gamma",
	)

	waitForEngineEventType(t, host.Engine, core.EventConnect)
	mustReceiveEnginePayloadsInOrder(t, host.Engine, []string{"one", "two", "three"})
	mustSendEnginePayloadsInOrder(t, host.Engine, []string{"alpha", "beta", "gamma"})
	mustWaitProcessSuccess(t, client)
}

func TestUnreliableExchangeAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "unreliable_exchange")

	host := mustListenEngineHost(t)
	client := startScenario(t, "unreliable_exchange",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", "c-unreliable",
		"--expect", "go-unreliable",
	)

	waitForEngineEventType(t, host.Engine, core.EventConnect)
	mustReceiveEnginePacketWithFlags(t, host.Engine, "c-unreliable", 0)
	mustSendEnginePacketWithFlags(t, host.Engine, "go-unreliable", 0)
	mustWaitProcessSuccess(t, client)
}

func TestFragmentedReliablePayloadAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "fragmented_reliable")

	payload := repeatedPayload("fragment-", 4096)
	host := mustListenEngineHost(t)
	client := startScenario(t, "fragmented_reliable",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--send", payload,
		"--expect", payload,
	)

	waitForEngineEventType(t, host.Engine, core.EventConnect)
	mustReceiveEnginePayload(t, host.Engine, payload)
	mustSendReliableEnginePacket(t, host.Engine, waitForConnectedPeer(t, host.Engine), payload)
	mustWaitProcessSuccess(t, client)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestReliableOrderingAcrossInterop|TestUnreliableExchangeAcrossInterop|TestFragmentedReliablePayloadAcrossInterop' -count=1`
Expected: FAIL from placeholder assertions

- [ ] **Step 3: Write minimal scenario implementations and assertions**

```go
func repeatedPayload(prefix string, size int) string {
	return prefix + strings.Repeat("x", size-len(prefix))
}
```

```c
/* reliable_ordering.c sends packet-1, packet-2, packet-3 after CONNECT and expects the same order on receive. */
/* unreliable_exchange.c uses ENET_PACKET_FLAG_UNSEQUENCED or an unreliable send flag for small payloads. */
/* fragmented_reliable.c sends a payload larger than the configured MTU and logs RECEIVE with the full payload. */
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestReliableOrderingAcrossInterop|TestUnreliableExchangeAcrossInterop|TestFragmentedReliablePayloadAcrossInterop' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/cases/reliable_ordering.c interop/cases/unreliable_exchange.c interop/cases/fragmented_reliable.c interop/go_server_test.go interop/fragmentation_test.go interop/test_helpers_test.go
git commit -m "test: cover ordering unreliable and fragmentation interop"
```

### Task 8: Add Disconnect Coverage In Both Directions

**Files:**
- Create: `interop/cases/disconnect_client_initiated.c`
- Create: `interop/cases/disconnect_server_initiated.c`
- Create: `interop/disconnect_test.go`
- Test: `interop/disconnect_test.go`

- [ ] **Step 1: Write the failing disconnect tests**

```go
func TestClientInitiatedDisconnectAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "disconnect_client_initiated")

	host := mustListenPublicHost(t)
	client := startScenario(t, "disconnect_client_initiated",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--disconnect-data", "42",
	)

	waitForPublicEventType(t, host.Host, goenet.EventTypeConnect)
	waitForDisconnectData(t, host.Host, 42)
	mustWaitProcessSuccess(t, client)
}

func TestServerInitiatedDisconnectAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "disconnect_server_initiated")

	port := reserveUDPPort(t)
	server := startScenario(t, "disconnect_server_initiated",
		"--port", strconv.Itoa(port),
		"--disconnect-data", "99",
	)

	host := mustNewPublicHost(t)
	_, _ = mustConnectPublicHost(t, host, port)
	waitForPublicEventType(t, host, goenet.EventTypeConnect)
	waitForDisconnectData(t, host, 99)
	mustWaitProcessSuccess(t, server)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestClientInitiatedDisconnectAcrossInterop|TestServerInitiatedDisconnectAcrossInterop' -count=1`
Expected: FAIL from placeholder assertions

- [ ] **Step 3: Write minimal disconnect scenarios and assertions**

```c
/* disconnect_client_initiated.c connects, optionally exchanges one payload, calls enet_peer_disconnect, and logs DISCONNECT <data>. */
/* disconnect_server_initiated.c accepts a connection, sends optional payload, calls enet_peer_disconnect, and logs DISCONNECT <data>. */
```

```go
func waitForDisconnectData(t *testing.T, host *goenet.Host, want uint32) {
	t.Helper()
	for {
		event, err := host.Service(context.Background(), 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == goenet.EventTypeDisconnect && event.Data == want {
			return
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestClientInitiatedDisconnectAcrossInterop|TestServerInitiatedDisconnectAcrossInterop' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/cases/disconnect_client_initiated.c interop/cases/disconnect_server_initiated.c interop/disconnect_test.go interop/test_helpers_test.go
git commit -m "test: add disconnect interop scenarios"
```

### Task 9: Add Multi-Peer, Broadcast, Idle, And Reconnect Coverage

**Files:**
- Create: `interop/cases/multi_client_connect.c`
- Create: `interop/cases/broadcast_receive.c`
- Create: `interop/cases/idle_ping.c`
- Create: `interop/cases/reconnect_cycle.c`
- Create: `interop/multi_peer_test.go`
- Create: `interop/idle_reconnect_test.go`
- Test: `interop/multi_peer_test.go`
- Test: `interop/idle_reconnect_test.go`

- [ ] **Step 1: Write the failing concurrency and lifecycle tests**

```go
func TestMultipleCClientsConnectToOneGoHost(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "multi_client_connect")

	host := mustListenEngineHost(t)
	client := startScenario(t, "multi_client_connect",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--peer-count", "3",
	)

	peers := waitForConnectCount(t, host.Engine, 3)
	if len(peers) != 3 {
		t.Fatalf("connect count = %d", len(peers))
	}
	mustWaitProcessSuccess(t, client)
}

func TestGoBroadcastReachesAllCClients(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "broadcast_receive")

	host := mustListenPublicHost(t)
	client := startScenario(t, "broadcast_receive",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--peer-count", "3",
		"--expect", "broadcast-payload",
	)

	waitForPublicConnectCount(t, host.Host, 3)
	mustBroadcastReliablePacket(t, host.Host, "broadcast-payload")
	mustWaitProcessSuccess(t, client)
}

func TestIdleConnectionStaysAliveAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "idle_ping")

	host := mustListenPublicHost(t)
	client := startScenario(t, "idle_ping",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
		"--timeout-ms", "1500",
	)

	waitForPublicEventType(t, host.Host, goenet.EventTypeConnect)
	mustWaitProcessSuccess(t, client)
}

func TestReconnectCycleAcrossInterop(t *testing.T) {
	cfg := mustLoadInteropConfigForTest(t)
	mustBuildScenario(t, cfg, "reconnect_cycle")

	host := mustListenPublicHost(t)
	client := startScenario(t, "reconnect_cycle",
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(host.Port()),
	)

	waitForPublicConnectCount(t, host.Host, 2)
	waitForPublicDisconnectCount(t, host.Host, 1)
	mustWaitProcessSuccess(t, client)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestMultipleCClientsConnectToOneGoHost|TestGoBroadcastReachesAllCClients|TestIdleConnectionStaysAliveAcrossInterop|TestReconnectCycleAcrossInterop' -count=1`
Expected: FAIL from placeholder assertions

- [ ] **Step 3: Write minimal multi-peer scenarios and assertions**

```c
/* multi_client_connect.c launches N client peers from one process and logs CONNECT <index> for each connected peer. */
/* broadcast_receive.c launches N client peers and expects one shared payload from the Go host. */
/* idle_ping.c connects and waits for a stable service window before logging DONE. */
/* reconnect_cycle.c connects, disconnects cleanly, reconnects, and expects both cycles to complete. */
```

```go
func waitForConnectCount(t *testing.T, host *engine.Host, want int) []*peer.Peer {
	t.Helper()
	var peers []*peer.Peer
	for len(peers) < want {
		event, err := host.Service(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == core.EventConnect {
			peers = append(peers, event.Peer)
		}
	}
	return peers
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `ENET_SOURCE_DIR=/path/to/enet go test ./interop -run 'TestMultipleCClientsConnectToOneGoHost|TestGoBroadcastReachesAllCClients|TestIdleConnectionStaysAliveAcrossInterop|TestReconnectCycleAcrossInterop' -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add interop/cases/multi_client_connect.c interop/cases/broadcast_receive.c interop/cases/idle_ping.c interop/cases/reconnect_cycle.c interop/multi_peer_test.go interop/idle_reconnect_test.go interop/test_helpers_test.go
git commit -m "test: add multi-peer and lifecycle interop coverage"
```

### Task 10: Finish Documentation And End-To-End Verification

**Files:**
- Modify: `interop/README.md`
- Modify: `Taskfile.yml`
- Test: `interop/*.go`

- [ ] **Step 1: Write the failing operator-doc expectation**

```go
func TestInteropReadmeDocumentsTaskEntrypoint(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(interopDirFromRuntime(), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "task interop:test") {
		t.Fatal("README missing task interop:test usage")
	}
	if !strings.Contains(text, "interop/.env") {
		t.Fatal("README missing interop/.env setup")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./interop -run TestInteropReadmeDocumentsTaskEntrypoint -count=1`
Expected: FAIL until README is updated

- [ ] **Step 3: Write minimal documentation and run the full harness**

`interop/README.md`

```md
# Interop Harness

1. Copy `interop/.env.example` to `interop/.env`.
2. Set `ENET_SOURCE_DIR` to your local ENet checkout.
3. Run `task interop:test`.

The task always evaluates the C build step and only rebuilds scenario binaries whose inputs changed.
```

- [ ] **Step 4: Run verification to confirm everything passes**

Run: `task interop:test`
Expected: PASS with scenario build output followed by `ok  	github.com/cafecito-games/goenet/interop`

Run: `go test ./...`
Expected: PASS for the full repository

- [ ] **Step 5: Commit**

```bash
git add interop/README.md Taskfile.yml interop/*.go
git commit -m "test: finalize interop harness"
```
