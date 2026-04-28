# goenet Public API Layer Design

**Date:** 2026-04-28

**Goal:** expose a real usable server/client API from the `goenet` package while keeping ENet wire behavior and state-machine logic inside `internal/engine`.

## Scope

This design covers the first public API milestone only:

- usable host construction and socket lifecycle
- server and client flows
- public peer handles
- service loop and event translation
- public send/flush/disconnect/reset operations
- the minimal engine additions required to support those operations

This design does **not** broaden into unrelated protocol refactors, lint cleanup, or changing the internal engine’s role as the wire/state-machine core.

## Problem

The codebase now has a substantial internal ENet-compatible engine, but the exported `goenet` package is still not usable as a library. Users can construct values like `Config`, `Packet`, and `Event`, but they cannot:

- create a host
- listen on UDP
- connect to a remote ENet peer
- service events
- send packets
- disconnect peers

Interop coverage currently works by driving `internal/engine` directly, which is useful for development but not acceptable as the end-user API.

## Approach Options

### 1. Thin public runtime over `internal/engine` (recommended)

Expose a stable public `Host`/`Peer` API in `goenet`, with the exported layer owning the UDP socket, peer handle mapping, and event translation while the engine remains the protocol core.

**Pros**

- fastest path to a usable library
- keeps engine internals private
- allows continued parity fixes without breaking users
- minimizes duplicated networking/state logic

**Cons**

- requires a small adapter/runtime layer
- needs a few targeted engine additions for outbound connect/disconnect/reset

### 2. Directly wrap engine types

Expose the current engine more or less directly through the public package.

**Pros**

- lower initial code volume

**Cons**

- leaks internal structure into the public API
- makes future engine cleanup and parity work harder
- couples users to unstable internal shapes

### 3. Separate public orchestration stack independent from `internal/engine`

Build a larger public runtime that duplicates host/session orchestration outside the engine.

**Pros**

- very clean long-term layering

**Cons**

- too much duplication right now
- higher correctness risk
- slows delivery of a usable API

## Recommendation

Use option 1: a thin public runtime over `internal/engine`.

The exported `goenet` package should present idiomatic Go types and lifecycle while the internal engine remains responsible for:

- packet encoding/decoding
- ENet sequencing and ACK handling
- fragmentation/reassembly
- throttle/bandwidth behavior
- peer state transitions
- compatibility with the local ENet fork at `/Users/christian/CafecitoGames/enet`

Because `internal/engine` already consumes shared types currently declared in `goenet`, the implementation must first extract those engine-consumed shared types into an internal core package and make the exported `goenet` types aliases or thin wrappers. Without that step, `goenet.Host` cannot own an `internal/engine.Host` without an import cycle.

## Public API Shape

The first usable public API should expose:

- `Listen(addr string, cfg Config) (*Host, error)`
- `NewHost(cfg Config) (*Host, error)`
- `(*Host).Connect(addr string, channelCount uint8, data uint32) (*Peer, error)`
- `(*Host).Service(ctx context.Context, timeout time.Duration) (Event, error)`
- `(*Host).Flush(ctx context.Context) error`
- `(*Host).Broadcast(channelID uint8, packet *Packet) error`
- `(*Host).Close() error`
- `(*Host).Config() Config`
- `(*Peer).Send(channelID uint8, packet *Packet) error`
- `(*Peer).Disconnect(data uint32) error`
- `(*Peer).DisconnectLater(data uint32) error`
- `(*Peer).Reset()`
- `(*Peer).State() PeerState`

The current exported `Event` type remains the public event shape.

## Runtime Boundary

`goenet.Host` is the public orchestration layer, not the wire engine.

To make that compile cleanly, the dependency direction must become:

- `internal/core` owns shared transport value types used by both engine and public API
- `internal/engine` depends on `internal/core`, not `goenet`
- `goenet` depends on `internal/core` and `internal/engine`

Responsibilities of `goenet.Host`:

- own the real UDP socket
- own an internal `engine.Host`
- own lifecycle state such as closed/open and bind mode
- translate engine events into public events
- manage stable exported `*goenet.Peer` handles

Responsibilities of `goenet.Peer`:

- provide a stable exported handle for one internal peer
- forward operations like send/disconnect/reset back through its owning host
- expose only public-safe state snapshots

The public layer must not expose internal peer pointers or engine event types directly.

## Peer Mapping

Each internal peer needs a stable exported peer wrapper.

The public host will maintain a mapping from internal peer identity to exported `*goenet.Peer`. That wrapper must survive across multiple connect/receive/disconnect events for the same peer slot until the peer is reset.

Rules:

- the same internal peer should map to the same exported `*Peer` during its active lifetime
- disconnect/reset must invalidate the old active association cleanly
- future reuse of a peer slot must not accidentally present stale peer state to callers

## Event Translation

Public `Host.Service` translates internal engine events into exported `goenet.Event` values.

Mappings:

- engine connect -> `EventConnect`
- engine receive -> `EventReceive`
- engine disconnect -> `EventDisconnect`
- engine disconnect-timeout -> `EventDisconnectTimeout`

The `Peer` field on public events must always point to the stable exported peer wrapper, not an internal peer.

## Socket And Lifecycle

Add a concrete UDP socket implementation under `internal/socket` backed by `net.UDPConn`.

Lifecycle behavior:

- `Listen` binds immediately to the requested local address
- `NewHost` creates a client-capable host; it may bind an ephemeral local UDP socket at construction time
- `Close` closes the underlying socket and makes later operations fail with a normal Go error
- `Service` accepts `time.Duration`, converts it to the engine’s millisecond base, and drives one service pass
- `Flush` flushes pending traffic without pretending to be a full service iteration

## Required Engine Additions

The public layer needs a limited amount of new internal support:

- outbound connect helper:
  allocate/configure an internal peer, queue `Connect`, and seed state for the existing verify-connect path
- outbound disconnect helper:
  queue immediate `Disconnect`
- disconnect-later helper:
  mark deferred disconnect in ENet-compatible form
- reset helper:
  immediate local peer reset without wire notification

These additions should stay minimal and narrowly focused on enabling the public API.

## Error Handling

Public operations return ordinary Go errors.

Examples:

- invalid local bind address
- invalid remote connect address
- host closed
- nil packet
- channel out of range
- send attempted on invalid peer state

No C-style result enums should leak into the exported surface.

## Testing Strategy

The implementation should be driven by public API tests first.

Required coverage:

- `Listen` binds successfully
- `NewHost` creates a client-capable host
- public `Connect` drives a real connect handshake against another Go host or existing engine path
- `Service` returns translated public events with stable `*Peer` handles
- `Peer.Send` results in a receive event on the remote side
- `Disconnect`, `DisconnectLater`, and `Reset` behave consistently through public events/state
- `Close` shuts down cleanly and makes later operations fail predictably

Interop coverage already in `interop/` can remain engine-driven for now; the public API milestone does not require rewriting the existing cross-language tests immediately.

## Non-Goals

This milestone does not require:

- reworking the internal engine architecture
- exposing every ENet statistic publicly
- fixing repo-wide lint debt
- changing Task 8’s existing interop harness shape
- inventing public APIs that the current engine cannot honestly support

## Success Criteria

This subsystem is complete when:

- an application can use the exported `goenet` package to run a server host
- an application can use the exported `goenet` package to create a client host and connect
- packets can be sent and received using exported `Host` and `Peer` methods
- events are delivered through exported `Event` values
- the implementation still relies on `internal/engine` for ENet behavior instead of duplicating protocol logic
