# Review Findings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the reviewed lifecycle, config, API, and logging issues in the public package with full regression coverage, including interop tests where they strengthen confidence.

**Architecture:** Keep the existing `pkg` -> `internal/engine` layering, but tighten the public boundary. Normalize/validate config before engine creation, make close participate in the host lock protocol, propagate contexts through peer disconnect APIs, and keep logging policy decisions at the public/runtime edges rather than in the protocol core.

**Tech Stack:** Go 1.26, standard library `context`, `log/slog`, `net`, existing Go test suite, existing ENet interop harness in `internal/interop`

---

### Task 1: Record Baseline And Add Public API Regression Tests

**Files:**
- Modify: `pkg/host_test.go`
- Modify: `pkg/concurrent_test.go`
- Modify: `examples/basic_client_test.go`
- Modify: `examples/basic_server_test.go`
- Modify: `examples/public_api_test.go`
- Test: `pkg/host_test.go`
- Test: `pkg/concurrent_test.go`
- Test: `examples/basic_client_test.go`
- Test: `examples/basic_server_test.go`
- Test: `examples/public_api_test.go`

- [ ] **Step 1: Add failing public tests for the new disconnect signatures and config normalization**

Add tests covering:
- `Peer.Disconnect`, `DisconnectNow`, and `DisconnectLater` requiring a context argument
- `Host.Config()` returning the effective `ChannelLimit`
- invalid config being rejected at construction

- [ ] **Step 2: Add a concurrency regression test for close normalization**

Add a test that races `Close` against `Service`/`Flush` and asserts the public API only returns `ErrHostClosed` once closure wins, not raw socket-close errors.

- [ ] **Step 3: Update example tests to compile against the new peer disconnect API**

Adjust any example or external-package smoke test that calls the disconnect methods so the public surface is exercised with explicit contexts.

- [ ] **Step 4: Run the focused public tests and confirm they fail for the intended reasons**

Run: `go test ./pkg ./examples`
Expected: compile failures for the old disconnect signatures and/or assertion failures around config/close semantics.

### Task 2: Normalize And Validate Config At Construction

**Files:**
- Modify: `pkg/config.go`
- Modify: `pkg/host.go`
- Modify: `pkg/host_test.go`
- Modify: `internal/core/types.go` (only if shared validation helpers are cleaner there)
- Test: `pkg/host_test.go`

- [ ] **Step 1: Implement a single public config normalization path**

Add focused helpers in `pkg/config.go` to:
- apply defaults
- derive effective `ChannelLimit`
- validate constructor inputs
- return the exact config snapshot used to build the engine

- [ ] **Step 2: Make host construction use the normalized config snapshot**

Update `newHostWithSocket`, `Listen`, and `NewHost` so invalid configs fail early and `Host.Config()` returns effective values instead of the pre-normalized input.

- [ ] **Step 3: Run the focused config tests**

Run: `go test ./pkg -run 'Test(DefaultConfig|HostConfig|Listen|NewHost|InvalidConfig)'`
Expected: PASS

### Task 3: Serialize Close And Normalize Closed-Host Errors

**Files:**
- Modify: `pkg/host.go`
- Modify: `pkg/peer.go`
- Modify: `pkg/concurrent_test.go`
- Modify: `pkg/host_test.go`
- Test: `pkg/concurrent_test.go`
- Test: `pkg/host_test.go`

- [ ] **Step 1: Add failing tests for close/operation races**

Extend the public tests to cover:
- `Service` blocked while `Close` runs
- `Flush`/`Connect` after close begins
- peer methods after host close

- [ ] **Step 2: Implement serialized close semantics in the public host**

Make `Close` coordinate with `mu`, add closed checks inside the critical section for host operations, and normalize closed-socket errors to `ErrHostClosed` at the public boundary.

- [ ] **Step 3: Run the focused lifecycle tests**

Run: `go test ./pkg -run 'TestHost(Close|Service|Flush|Connect)|TestPeer.*Close|TestConcurrent'`
Expected: PASS

- [ ] **Step 4: Run the race detector for the public package**

Run: `go test -race ./pkg`
Expected: PASS

### Task 4: Replace The Public Disconnect API With Context-Aware Methods

**Files:**
- Modify: `pkg/peer.go`
- Modify: `pkg/host_test.go`
- Modify: `pkg/public_hooks_test.go`
- Modify: `examples/basic_client_test.go`
- Modify: `examples/basic_server_test.go`
- Modify: `examples/public_api_test.go`
- Modify: `internal/interop/test_helpers_test.go`
- Modify: `internal/interop/*_test.go` as needed for compile coverage
- Test: `pkg/host_test.go`
- Test: `examples/basic_client_test.go`
- Test: `internal/interop/*_test.go`

- [ ] **Step 1: Change the public peer method signatures**

Update `pkg/peer.go` to require caller-provided contexts for all disconnect variants and thread those contexts into the existing engine calls.

- [ ] **Step 2: Update all call sites and tests**

Replace old calls with explicit contexts in public tests, examples, and interop helpers. Prefer short-lived contexts in tests where the behavior under cancellation is relevant.

- [ ] **Step 3: Add coverage for context cancellation on disconnect paths**

Add at least one focused public test that proves a canceled context aborts a synchronous disconnect/flush path rather than hanging or silently proceeding.

- [ ] **Step 4: Run the affected packages**

Run: `go test ./pkg ./examples ./internal/interop`
Expected: PASS

### Task 5: Tighten Logging Levels And Expectations

**Files:**
- Modify: `pkg/host.go`
- Modify: `internal/engine/host.go`
- Modify: `internal/engine/receive.go`
- Modify: `pkg/host_test.go`
- Modify: `internal/engine/host_test.go`
- Test: `pkg/host_test.go`
- Test: `internal/engine/host_test.go`

- [ ] **Step 1: Add failing assertions for routine lifecycle logging levels**

Update capture-handler tests so normal host start/close and peer transition records are expected at `Debug`, while warnings/errors keep their existing levels.

- [ ] **Step 2: Change the routine lifecycle log sites**

Demote the agreed normal lifecycle log sites from `Info` to `Debug` without changing their structured fields.

- [ ] **Step 3: Run the logging-focused tests**

Run: `go test ./pkg ./internal/engine -run 'Test.*Log'`
Expected: PASS

### Task 6: Add Or Strengthen Interop Coverage

**Files:**
- Modify: `internal/interop/disconnect_test.go`
- Modify: `internal/interop/test_helpers_test.go`
- Modify: `internal/interop/idle_reconnect_test.go` (if the new coverage fits there)
- Test: `internal/interop/disconnect_test.go`

- [ ] **Step 1: Add interop coverage that exercises the new public disconnect API**

Prefer scenarios where the public Go host initiates disconnects against the C harness with explicit contexts, so the test proves the API change did not regress wire compatibility.

- [ ] **Step 2: Add a focused interop assertion for immediate disconnect behavior if missing**

If current coverage does not already prove `DisconnectNow` against the C peer, add or strengthen a scenario there.

- [ ] **Step 3: Run the interop suite**

Run: `go test ./internal/interop -count=1`
Expected: PASS

### Task 7: Run Full Verification

**Files:**
- Modify: `README.md` (only if public API examples or prose mention the old disconnect signatures)

- [ ] **Step 1: Update README snippets if they reference the old disconnect API**

Keep the documentation aligned with the breaking public API.

- [ ] **Step 2: Run the full test suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 3: Run the full race suite**

Run: `go test -race ./...`
Expected: PASS
