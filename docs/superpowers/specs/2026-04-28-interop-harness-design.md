# Interop Harness Design

**Goal**

Build a dedicated `interop/` test harness that treats the C ENet implementation as the source of truth and validates `goenet` against it through end-to-end UDP interoperability tests. The harness must be runnable through `task interop:test`, isolated from the exported library surface, and easy to extend with new protocol scenarios.

**Scope**

This design covers:

- a self-contained `interop/` sub-project for reference-side C cases and Go test orchestration
- incremental reference-binary builds driven by `task interop:test`
- repo-local ENet source discovery via `interop/.env`, without hardcoded absolute paths
- an initial battery of multi-process interoperability scenarios between `goenet` and C ENet

This design does not cover:

- shipping the interop harness as part of the public Go package API
- replacing existing unit tests under `internal/`
- protocol fuzzing, packet capture tooling, or CI environment provisioning beyond local task execution

## Architecture

The harness is organized around small, explicit C scenario programs and a Go-side runner. Each scenario represents one behavior under test from the reference ENet side: connect, send, receive, disconnect, fragmentation, multi-peer, and similar flows. The Go tests set up `goenet` hosts or clients, launch the matching C scenario binary as a subprocess, and assert that both sides observe the expected event sequence and payload semantics over real UDP sockets.

`task interop:test` is the single operator entry point. It loads `interop/.env`, validates `ENET_SOURCE_DIR`, invokes an incremental build script for the C scenario binaries, and then runs `go test ./interop -count=1`. The build step always runs, but only recompiles targets whose inputs changed. Inputs include scenario sources under `interop/cases`, shared harness sources under `interop/lib` and `interop/include`, build tooling under `interop/scripts`, and the files under the configured ENet source tree.

The interop project remains intentionally separate from the exported library. It may use the public `goenet` API where available and fall back to internal packages only where the current public surface cannot express a reference scenario yet. That fallback should be minimal, deliberate, and easy to remove as the public API reaches parity.

## Directory Layout

Proposed structure:

- `interop/.env.example`
  Documents required environment such as `ENET_SOURCE_DIR`.
- `interop/.env`
  Developer-local environment file, ignored by git.
- `interop/README.md`
  Operator documentation for setup, expected ENet tree shape, and how to add scenarios.
- `interop/bin/`
  Generated C binaries and build metadata, ignored by git.
- `interop/build/`
  Optional generated stamp or manifest data for incremental rebuild decisions, ignored by git.
- `interop/include/`
  Shared C headers for scenario declarations, logging, argument parsing, and common helpers.
- `interop/lib/`
  Shared C implementation files used by multiple scenarios.
- `interop/cases/`
  One C source file per reference scenario.
- `interop/scripts/`
  Build script entry points used by `task interop:test`.
- `interop/*.go`
  Go test runner files, shared process helpers, socket helpers, and per-scenario tests.

The C side should not stay embedded in a shell heredoc. Real source files make each scenario inspectable, diffable, and independently debuggable.

## Reference Build Model

The build script resolves `ENET_SOURCE_DIR` from `interop/.env` or the environment. If the variable is missing, the script fails immediately with a clear message. No fallback absolute path is permitted.

Incremental behavior:

- The build command always evaluates build state.
- Each scenario binary has a deterministic output path under `interop/bin/`.
- For each binary, the build step computes whether any dependency is newer than the binary or recorded manifest.
- Dependencies include:
  - the scenario source file
  - shared harness headers and C sources
  - the build script itself
  - ENet headers and any ENet implementation files required by the chosen build style
- If inputs are unchanged, the binary is reused.
- If inputs changed, only the affected binary is rebuilt.

This approach keeps `task interop:test` simple while avoiding unnecessary recompiles during test development.

## Scenario Interface

Each C scenario should have one narrow responsibility and an explicit CLI contract. The Go test runner launches the scenario with flags describing host, port, payloads, peer counts, and timeouts. The scenario emits machine-readable log lines that the Go runner can assert against.

Suggested conventions:

- scenario executable name matches source file name
- stdout emits single-line events such as `CONNECT`, `RECEIVE <payload>`, `DISCONNECT <data>`, `READY`, `DONE`
- stderr is reserved for diagnostics
- non-zero exit means scenario failure or assertion mismatch on the C side

Shared C support code should handle:

- argument parsing
- address setup
- ENet initialization and deinitialization
- timeout loops
- event logging
- packet creation helpers
- consistent exit codes

## Go Runner

The Go side should provide small reusable helpers instead of one monolithic test:

- build path resolution for scenario binaries
- subprocess launch and bounded waits
- stdout collection and assertion helpers
- UDP or host setup helpers for `goenet`
- event polling with clear timeout errors
- payload generators for fragmentation and ordering scenarios

Test files should be grouped by scenario family rather than kept in one large `client_server_test.go`. Each test should clearly state which side acts as server and which reference scenario it expects.

## Initial Test Matrix

The first battery should cover these end-to-end cases:

1. Go server accepts C client connect and exchanges reliable packets both directions.
2. Go client connects to C server and exchanges reliable packets both directions.
3. Reliable packet ordering is preserved from C to Go and from Go to C.
4. Unreliable packet exchange works for basic non-fragmented payloads.
5. Go-initiated graceful disconnect is observed correctly by C.
6. C-initiated graceful disconnect is observed correctly by Go.
7. Large reliable payloads are fragmented and reassembled correctly.
8. One Go host handles multiple concurrent C clients.
9. Go broadcast reaches multiple connected C clients.
10. Idle servicing without user payload still maintains a healthy connection long enough for a ping-style liveness check.
11. Reconnect succeeds after a clean disconnect cycle.

These cases are intentionally end-to-end. They validate observable compatibility, not just isolated packet bytes.

## Error Handling

Failure output must identify which side broke the contract:

- build failures should mention the missing or invalid `ENET_SOURCE_DIR`
- scenario startup failures should include the exact command line and stderr
- event assertion failures should include the collected scenario log and the Go-side observed event stream
- timeouts should identify the awaited event and elapsed duration

The goal is to make an interop regression actionable without manual reproduction.

## Testing Strategy

This harness complements, rather than replaces, existing unit tests. Unit tests continue to prove local invariants; interop tests prove wire-level behavior against the reference implementation.

The implementation should favor deterministic scenarios:

- fixed loop deadlines
- explicit payload values
- bounded peer counts
- bounded packet counts
- no probabilistic delivery assertions beyond what ENet semantics actually guarantee

Where unreliable delivery is tested, the assertion should focus on “can be sent and processed under normal local conditions” rather than pretending the network guarantees delivery.

## Migration From Current Harness

The existing inline `build_c_harness.sh` and single `client_server_test.go` flow should be split into the new layout incrementally:

- first extract the current C logic into real source files and keep the existing passing scenario
- then add shared utilities and switch the build step to scenario-based outputs
- then expand the Go tests scenario by scenario

This preserves a working baseline while the harness is being cleaned up.

## Risks And Boundaries

- Some desired scenarios may still require temporary use of internal `engine` APIs if the public `goenet` surface cannot express the needed behavior yet.
- Broadcast and multi-peer timing will be more sensitive than single-peer tests and should use explicit readiness signaling.
- Rebuild invalidation must be conservative; false-positive rebuilds are acceptable, stale binaries are not.

The design should bias toward correctness and debuggability over minimizing a small amount of local build time.
