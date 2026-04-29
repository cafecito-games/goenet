# goenet Design

## Goal

Build `github.com/cafecito-games/goenet` as a pure Go 1.26 implementation of ENet that is wire-compatible with the upstream `enet` library at the protocol and behavior level, while exposing an idiomatic Go API rather than a C-shaped API.

## Scope

This project is a full port, not a reduced subset. The initial implementation must cover the complete ENet feature set represented by the upstream ENet reference source used by this repo, including:

- connection handshake and peer state machine
- reliable, unreliable, and unsequenced delivery
- channels and per-channel sequencing windows
- fragmentation and reassembly for reliable and unreliable packets
- acknowledgements, retransmission, RTT tracking, and timeout handling
- bandwidth limits and packet throttling
- compression hook support
- checksum hook support
- raw packet interception hook support
- IPv4/IPv6 address handling compatible with ENet behavior
- host and peer statistics

The public API does not need to mirror ENet symbol-for-symbol. Compatibility is defined by:

1. on-the-wire packet format and command encoding
2. peer and host behavior observable by an interoperating ENet implementation
3. event ordering and delivery semantics expected by ENet users

## Non-Goals

- cgo wrappers around the C library
- exporting the low-level C socket API directly
- preserving ENet’s callback signatures, memory override API, or raw struct layouts in the public Go surface
- adding higher-level game networking features outside ENet’s scope, such as auth, discovery, encryption, or matchmaking

## Upstream Compatibility Target

The reference implementation is the ENet source tree used for local interop and protocol checks, specifically the single-header fork exposing ENet `2.6.5` constants and protocol layout. The Go implementation should treat that source as the behavioral oracle for:

- protocol constants such as MTU/window/channel limits
- command numbering and header flags
- peer state transitions
- packet throttle, timeout, and bandwidth algorithms
- fragmentation/reassembly rules
- checksum/compression/intercept call sites

Where ENet behavior is implicit in the implementation rather than formally documented, the Go port should match the implementation behavior, not reinterpret it.

## Recommended Approach

Use a protocol-first native port.

Internally, the code should be decomposed into focused Go packages that model ENet’s protocol engine and state transitions faithfully, but it should not transliterate the C file into a single giant Go source file. Publicly, the library should present idiomatic Go types, constructors, methods, options, and errors. Internally, it should preserve the same protocol decisions ENet makes so that existing ENet clients and servers interoperate without special handling.

This balances the two stated requirements:

- full ENet wire/protocol compatibility
- 100% Go-native implementation style

## Architecture

### Public Surface

Expose a compact `goenet` package with Go-native types:

- `Host` for client or server endpoints
- `Peer` for a remote connection
- `Packet` for payload plus delivery flags
- `Event` for serviced connection, disconnect, timeout, and receive events
- `Address` as a wrapper over `netip.AddrPort` semantics
- `Config` and functional options for host behavior

Representative operations:

- create/listen on a host
- connect to a remote address
- service or poll for events
- send a packet on a channel
- broadcast a packet
- flush pending output
- disconnect gracefully, disconnect now, or reset
- inspect peer state and host/peer statistics

The public API should be idiomatic Go:

- methods returning `error` instead of sentinel C return values
- exported enums and flags as Go typed constants
- interfaces for checksum, compression, and intercept behavior
- no exposed raw socket descriptors or allocation callbacks

### Internal Decomposition

The implementation should be split into a public package plus focused internal packages. The exact file layout can evolve, but the responsibility boundaries should look like this:

- `goenet`
  Public API, user-facing types, options, and API documentation.
- `internal/protocol`
  Wire-level structs, constants, encode/decode, packet headers, command marshaling, checksum framing, compression framing, session bits, and validation.
- `internal/engine`
  Host service loop, peer state machine, ACK handling, resend scheduling, event dispatch, timeout logic, bandwidth throttle scheduling, and flush/send passes.
- `internal/peer`
  Peer and channel state, sequence windows, incoming/outgoing command queues, fragment tracking, packet accounting, and stats helpers.
- `internal/socket`
  UDP socket abstraction over the Go standard library, address conversion, read/write batching strategy, and socket option handling where available.
- `internal/timeutil`
  ENet-compatible monotonic millisecond clock and overflow-aware time helpers matching ENet arithmetic behavior.
- `internal/buffer`
  Buffer assembly, packet ownership/refcount-style semantics translated into Go ownership rules, and efficient payload reuse.
- `internal/testsupport`
  Golden packet fixtures, interoperability helpers, deterministic clock/socket fakes, and optional process-level ENet integration harnesses.

This separation keeps the codebase navigable while preserving ENet’s behavior-critical logic.

## Behavior Mapping

### Wire Protocol

The Go port must match ENet’s command formats and header semantics:

- protocol commands `ACKNOWLEDGE`, `CONNECT`, `VERIFY_CONNECT`, `DISCONNECT`, `PING`, `SEND_RELIABLE`, `SEND_UNRELIABLE`, `SEND_FRAGMENT`, `SEND_UNSEQUENCED`, `BANDWIDTH_LIMIT`, `THROTTLE_CONFIGURE`, and `SEND_UNRELIABLE_FRAGMENT`
- command flags for acknowledge and unsequenced commands
- header flags for sent time, compression, and peer/session bits
- peer/session ID layout and associated masks/shifts

Encoding and decoding should be implemented explicitly with byte-order-safe Go code, not by relying on struct layout tricks. This avoids architecture-dependent behavior and keeps the implementation portable and testable.

### Peer State Machine

The peer state machine must preserve ENet’s state progression and event emission behavior:

- `DISCONNECTED`
- `CONNECTING`
- `ACKNOWLEDGING_CONNECT`
- `CONNECTION_PENDING`
- `CONNECTION_SUCCEEDED`
- `CONNECTED`
- `DISCONNECT_LATER`
- `DISCONNECTING`
- `ACKNOWLEDGING_DISCONNECT`
- `ZOMBIE`

The service loop must reproduce the same externally visible connect/disconnect/timeout transitions and queue draining behavior as ENet, including the “disconnect later” behavior when reliable outbound commands remain pending.

### Delivery Semantics

The implementation must preserve:

- reliable ordered delivery per channel
- unreliable sequenced delivery per channel
- unsequenced delivery group/window behavior
- duplicate suppression and sequence-window checks
- packet fragmentation/reassembly behavior and fragment count limits
- packet throttle rules for unreliable sends

These are behavior-critical for compatibility. A Go-native API must not hide or loosen these semantics.

### Timing and Retransmission

The implementation must preserve ENet’s millisecond timing model:

- monotonic service time
- time-difference and overflow comparisons compatible with ENet’s macros
- resend timeout progression
- peer timeout minimum/maximum/limit behavior
- ping intervals
- packet loss epoch calculations
- RTT and RTT variance updates
- bandwidth throttle epoch scheduling

Go’s `time.Time` and `time.Duration` can be used internally, but the engine should normalize to ENet-compatible millisecond arithmetic at decision points.

## Public API Design

### Types

The exported package should include:

- `type Host struct`
- `type Peer struct`
- `type Packet struct`
- `type Event struct`
- `type Address struct`
- `type Config struct`
- typed enums/flags for event types, peer states, packet flags, and disconnect reasons where relevant

`Address` should wrap ENet-compatible addressing while using `netip.AddrPort` internally, plus explicit IPv6 scope metadata so the implementation can preserve ENet’s `sin6_scope_id` behavior.

`Packet` should own its payload as a `[]byte` and flags as typed constants. The Go implementation should not expose mutable internal bookkeeping corresponding to ENet’s reference counting.

### Construction and Configuration

Use constructors and options instead of global initialize/deinitialize calls. Representative API shape:

- `Listen(Address, ...Option) (*Host, error)`
- `NewHost(...Option) (*Host, error)` for client-style hosts
- `(*Host).Connect(Address, channelCount uint8, data uint32) (*Peer, error)`
- `(*Host).Service(ctx context.Context, timeout time.Duration) (Event, error)`

Configuration should cover:

- peer count
- channel limit
- incoming/outgoing bandwidth limits
- MTU
- maximum packet size
- maximum waiting data
- duplicate peer limit
- checksum provider
- compressor
- intercept hook
- debug tracing hook
- injected clock/socket abstractions for tests

### Extensibility Interfaces

Map ENet callbacks to Go interfaces:

- `Checksum` interface for packet checksum computation
- `Compressor` interface with `Compress`, `Decompress`, and lifecycle behavior
- `InterceptFunc` or interface for raw packet interception before normal processing

These should be optional and configured per host, preserving ENet’s call sites and behavior without exposing C callback conventions.

### Errors

Use descriptive Go errors for construction, socket, and service failures. Protocol-invalid packets that ENet would ignore should generally remain non-fatal unless the upstream behavior treats them as connection-affecting.

## Testing Strategy

This project needs heavier-than-usual testing because wire compatibility is the core requirement.

### Unit Tests

Add focused tests for:

- protocol command encode/decode round-trips
- sequence number and session bit handling
- time overflow helpers
- fragment assembly/disassembly
- throttle and timeout calculations
- peer/channel queue behavior
- address parsing/formatting and IPv4/IPv6 conversion

### Behavior Tests

Add deterministic engine tests using fake clocks and fake socket transports for:

- connect handshake
- reliable resend after dropped ACK/data
- disconnect and disconnect-timeout flows
- reliable/unreliable ordering guarantees
- unsequenced window behavior
- bandwidth throttle updates
- fragment reassembly across multiple datagrams

### Interoperability Tests

The most important verification layer is cross-implementation testing against the upstream C ENet reference. The Go port should include integration tests or scripted fixtures that:

- start a C ENet server and connect from Go
- start a Go host and connect from a C ENet client
- exchange reliable, unreliable, unsequenced, fragmented, and disconnect sequences
- verify connect/disconnect events, channel behavior, payload integrity, and timeout behavior

Where possible, interoperability tests should be automated in Go tests and use compiled helper binaries from the upstream ENet test sources or a small purpose-built harness built from the upstream tree.

### Regression Fixtures

Capture golden byte fixtures for representative encoded packets and command batches so that accidental wire-format drift is caught immediately.

## Performance and Memory

The first priority is correctness and compatibility, not premature optimization. Still, the Go design should avoid obvious inefficiencies:

- avoid per-packet allocations where buffer reuse is safe
- keep packet encoding/decoding explicit and compact
- avoid exposing mutable internal slices that force defensive copying everywhere
- use internal pooling only where it does not obscure correctness

The implementation should not mimic C’s manual memory model. Instead, it should define clear ownership:

- `Packet` payload ownership belongs to the packet object
- received events hand the caller a packet that remains valid until explicitly released or naturally garbage collected, depending on final API shape
- internal command queues reference packet objects through Go pointers rather than manual reference counts, while preserving equivalent lifetime behavior

If an explicit `Release` method is introduced for performance, it should be optional and clearly documented; it should not leak ENet’s C memory-management model into ordinary usage unless profiling justifies it.

## Project Scaffolding

Mirror the proven project setup from the existing CafecitoGames Go networking projects, adapted for this library.

### Go Module

- module path: `github.com/cafecito-games/goenet`
- Go version: `1.26`

### Taskfile

Create `Taskfile.yml` with the same development workflow style:

- `build`
- `test`
- `test:cover`
- `lint`
- `fmt`
- `fmt:check`
- `tidy`
- `tidy:check`
- `ci`
- `install`

Default task should run `ci`.

`ci` should compose local checks instead of duplicating shell logic:

- format check
- tidy check
- lint
- test
- build

### Linting

Add `.golangci.yml` using the same baseline shape as `gogdproto`:

- `errcheck`
- `govet`
- `staticcheck`
- `revive`
- `gocritic`
- `misspell`
- `unused`
- `ineffassign`
- `unconvert`
- `gosec`
- `nilerr`
- `bodyclose`
- `errorlint`
- formatters `gofmt` and `goimports`

Tune exclusions only where the ENet port structure or tests genuinely require it.

### GitHub Actions

Add `.github/workflows/pr.yml` modeled on `gogdproto`:

- trigger on `pull_request`
- trigger on `push` to `main`
- use Go `1.26.x`
- install Task 3.x
- install pinned `golangci-lint` v2
- run `task ci`
- run `task test:cover`
- upload `coverage.out`

### Pre-Commit

Add `.pre-commit-config.yaml` using the same pattern:

- `pre-commit-hooks` basics
- local hooks for `task fmt:check`, `task lint`, and `task test`

## Documentation

The repository should include:

- `README.md` describing goals, compatibility target, installation, and a minimal client/server example
- package docs for the public API
- notes documenting any intentionally Go-specific API differences from C ENet

The README should make the compatibility promise precise: wire-compatible with ENet, idiomatic Go API, pure Go implementation, and current compatibility target version.

## Delivery Plan Shape

This project is large enough that implementation should be split into milestones rather than attempted as one undifferentiated patch. A reasonable implementation sequence is:

1. project scaffolding, module layout, lint/Taskfile/CI, and base package documentation
2. protocol constants, packet/header encode/decode, and golden-wire tests
3. core host/peer/channel data model plus time helpers
4. outgoing command queueing, ACK handling, resend scheduling, and flush path
5. incoming command processing, sequencing, fragment reassembly, and event dispatch
6. connect/verify/disconnect/timeout state machine behavior
7. bandwidth throttle, packet throttle, checksums, compression, and intercept support
8. interoperability harness and full cross-language compatibility suite
9. README examples and API polish

The implementation plan should decompose those milestones into TDD-sized tasks with explicit file paths and verification commands.

## Risks and Controls

### Risk: Behavioral Drift From Upstream

ENet’s wire compatibility depends on subtle queue ordering and timeout behavior, not just packet formats.

Control:

- treat the upstream source as the oracle
- add interoperability tests early, not at the end
- keep protocol constants and state-machine behavior centralized and well tested

### Risk: C-Style Internals Leaking Into Public Go API

A literal transliteration would satisfy compatibility but produce an awkward Go library.

Control:

- keep the public package intentionally small and Go-native
- isolate compatibility mechanics in internal packages
- document API differences explicitly

### Risk: Under-Specified Extension Hooks

Compression, checksum, and intercept features can be forgotten until late because they are optional.

Control:

- include them in the initial architecture and test plan
- define interfaces before engine implementation is complete

### Risk: Test Environment Complexity

Cross-language interoperability tests may require building helper binaries from the C tree and coordinating ports and timing.

Control:

- keep pure Go deterministic tests for the bulk of engine behavior
- reserve cross-language tests for end-to-end compatibility confirmation
- use Task targets or scripts to make local and CI execution reproducible

## Acceptance Criteria

The port is considered successful when all of the following are true:

- the library is pure Go and builds under Go 1.26
- the repository has Taskfile, lint config, pre-commit config, and PR workflow consistent with the `gogdproto` standard
- the public API is idiomatic Go rather than a cgo wrapper or C-shaped clone
- all ENet protocol commands and peer behaviors in scope are implemented
- automated tests verify encoding/decoding, state-machine behavior, and fragmentation/throttling/timeout logic
- interoperability tests pass against the upstream C ENet implementation in both Go-client/C-server and C-client/Go-server directions

## Open Design Decisions Already Resolved

- compatibility target: wire/protocol compatibility, not symbol-for-symbol API compatibility
- scope: full ENet feature port
- approach: protocol-first native Go port
- module path: `github.com/cafecito-games/goenet`

## Next Step

Write the implementation plan in `docs/superpowers/plans/` with milestone/task breakdown, explicit file ownership, and TDD-first execution steps.
