# slog Logging Design

**Goal:** add opt-in, component-tagged `slog` logging across the library while preserving silent default behavior for callers that do not configure logging.

## Scope

This design covers:

- adding public logger configuration through `goenet.Config`
- normalizing omitted loggers to a no-op logger once at the config boundary
- propagating a non-nil logger through the existing internal config plumbing
- deriving per-component child loggers with flat, minimal tags
- adding representative tests and a short README example

This design does not cover:

- global package-level logger state
- pluggable logging abstractions beyond `*slog.Logger`
- full protocol transcript logging or payload dumping
- log-driven behavioral changes

## Context

The library currently has no logging surface. Construction is already driven through `goenet.Config`, which is translated into `internal/core.Config` and then passed into engine and socket layers. Optional facilities such as checksum, compression, and intercept hooks already follow this dependency-injection shape.

That existing structure is the right place to add logging. The main constraints are:

- logging must be completely optional
- call sites should not accumulate repeated nil checks
- logs should identify which subsystem emitted them
- the logging API should be stable and unsurprising for callers already using Go's standard `slog`

## Approaches Considered

### 1. `Config.Logger *slog.Logger`, normalized once to no-op, child loggers per subsystem (recommended)

Expose `Logger *slog.Logger` on public config, carry it through internal config, normalize `nil` to a discard-backed logger once, and derive per-component loggers with `With("component", "...")`.

Pros:

- explicit and per-host
- no global mutable state
- no repeated nil checks throughout the implementation
- natural fit with the standard library
- works well in tests and multi-host processes

Cons:

- adds a new public config field
- internal packages take a direct dependency on `log/slog`

### 2. Package-level default logger with optional per-host override

Add a global library logger and optionally allow per-host override.

Pros:

- slightly simpler one-time app setup for some callers

Cons:

- introduces mutable global state
- makes tests and multi-tenant processes less predictable
- obscures which hosts are expected to log

This is not recommended.

### 3. Internal wrapper interface around logging

Define a small internal logging abstraction and adapt `slog` into it.

Pros:

- reduces direct `slog` references in internal code

Cons:

- adds an unnecessary abstraction layer for a standard-library dependency
- complicates the implementation without solving a concrete problem

This is not recommended.

## Recommended Design

### Public API

Add:

- `Logger *slog.Logger` to `goenet.Config`

Semantics:

- `nil` means "silent logging" and is normalized internally to a no-op logger
- callers that want logs supply their own configured `*slog.Logger`
- caller-selected level filtering, handler formatting, and output destination remain entirely under caller control

This preserves the current zero-value behavior from the caller's perspective: if no logger is configured, the library emits nothing.

### Internal Config Plumbing

Add the equivalent logger field to `internal/core.Config` and thread it through existing config translation:

- `goenet.Config` -> `internal/core.Config`
- `engine.NewHost`
- socket adapter construction where needed

Normalization happens once at the boundary where public config becomes internal config. After that point, all internal code can assume `cfg.Logger` is non-nil.

The normalization helper should construct a discard-backed logger, for example via a handler writing to `io.Discard`.

### Component Tagging

Each subsystem derives and stores one child logger during construction:

- `component=host`
- `component=engine`
- `component=socket`

The logger shape should stay flat and minimal. Representative keys include:

- `component`
- `peer_id`
- `state`
- `addr`
- `channel`
- `event`
- `command`
- `bytes`
- `err`

No namespaced keys are needed.

### Logging Policy

The library should log events that are operationally meaningful, not every hot-path step.

Recommended emission categories:

- host construction and close
- connect, disconnect, timeout, and major peer state transitions
- send/receive decisions that materially affect behavior
- packet drops or rejects when the reason matters
- socket read/write failures
- flush budget exhaustion or similar control-flow decisions
- bandwidth/throttle decisions when they materially change peer behavior

The library should avoid:

- logging packet payload contents by default
- logging every routine send/receive success at a volume that turns logs into a transcript
- duplicating the same error at multiple layers without added context

The rule is that logs should observe behavior, not shape it.

## Architectural Boundaries

### Public Host Layer

`pkg/host.go` owns:

- accepting the caller logger through config
- preserving that logger on the public config snapshot
- deriving the `component=host` child logger
- logging public lifecycle and orchestration events where that layer adds context not visible internally

Examples:

- host creation
- host close
- public API calls that fail before they enter the engine

### Engine Layer

`internal/engine` owns:

- deriving `component=engine`
- logging state-machine decisions and network-control behavior

Examples:

- peer connect/disconnect transitions
- timeout handling
- command rejection or drop reasons
- flush-budget exhaustion
- notable resend/throttle decisions

### Socket Layer

`internal/socket/udp.go` owns:

- deriving `component=socket`
- logging low-level UDP I/O outcomes when they are operationally relevant

Examples:

- non-timeout read/write errors
- error suppression decisions such as ignored ICMP surface errors when that is useful context

Routine successful reads and writes should be logged conservatively to avoid noisy output in normal operation.

## Test Strategy

### Public Configuration Tests

Add public tests proving:

- `Config.Logger` is accepted
- omitted logger still yields normal behavior and no panics
- public config snapshots preserve the logger reference if needed for inspection

### Component Tagging Tests

Add focused tests using a capture handler to assert representative records include:

- `component=host`
- `component=engine`
- `component=socket`

These tests should verify a few concrete events rather than trying to snapshot full log streams.

### Behavioral Regression Safety

Existing tests should continue to pass unchanged wherever possible. Logging should be layered on top of current behavior, not used to justify semantic changes.

The safest test pattern is:

- trigger one representative action
- assert the behavior still occurs
- assert one or two expected log records/attributes

## Documentation

Update `README.md` with one short usage example showing:

```go
logger := slog.Default()

host, err := goenet.Listen("127.0.0.1:0", goenet.Config{
    PeerCount:    4,
    ChannelLimit: 1,
    Logger:       logger,
})
```

Also state explicitly that omitting `Logger` keeps the library silent.

## Implementation Notes

- Normalize `nil` once; do not scatter nil checks throughout the codebase.
- Store child loggers on long-lived structs rather than recomputing `With(...)` at each call site.
- Keep new logging helpers small and local to the existing packages unless multiple packages truly need a shared helper.
- Avoid introducing a separate internal logging interface unless a concrete need appears during implementation.

## Risks

### Risk: Logging becomes too chatty

Mitigation:

- start with lifecycle, error, and notable decision points only
- keep assertions in tests narrow so the logging surface can be tuned without rewriting large snapshots

### Risk: Logging adds avoidable hot-path overhead

Mitigation:

- normalize to a no-op logger once
- derive child loggers once per component
- avoid payload logging and repeated attribute construction where possible

### Risk: Duplicate or low-context logs across layers

Mitigation:

- let each layer log only when it adds context specific to that layer
- prefer one well-contextualized record over multiple shallow records for the same failure path

## Acceptance Criteria

This work is complete when:

- callers can opt into logging with `goenet.Config.Logger`
- callers that omit the logger still get silent behavior
- internal code does not rely on repeated logger nil checks
- logs are tagged with flat, minimal component identifiers
- representative host, engine, and socket events are logged
- tests cover logger wiring and component tagging
- README documents the logging option and silent default
