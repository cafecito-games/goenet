# Disconnect Parity And DisconnectNow Design

## Goal

Close the remaining ENet disconnect behavior gap for handshake states and expose a public forceful disconnect API.

## Scope

- Extend `Peer.Disconnect(data)` so it matches the local ENet fork across both connected and handshake states.
- Add `Peer.DisconnectNow(data) error` as the public forceful disconnect API.
- Add focused tests for wire shape and resulting public peer state.

## Required ENet Behavior

Based on the ENet reference header used for local interop and protocol checks:

- `enet_peer_disconnect()` always resets peer queues first.
- If the peer is `CONNECTED` or `DISCONNECT_LATER`, it queues `DISCONNECT | ACKNOWLEDGE`, transitions to `DISCONNECTING`, and completes later through the normal service path.
- Otherwise, it queues `DISCONNECT | UNSEQUENCED`, flushes immediately, and resets the peer locally.
- `enet_peer_disconnect_now()` queues `DISCONNECT | UNSEQUENCED` unless the peer is already terminal, flushes immediately, and then resets locally without waiting for later service completion.

## Go API

Add:

```go
func (p *Peer) DisconnectNow(data uint32) error
```

Semantics:

- Returns an error for invalid public conditions such as nil peer or closed host.
- Otherwise performs a best-effort immediate disconnect and local reset.
- Does not wait for a later disconnect event.

`Peer.Disconnect(data)` keeps the existing graceful connected behavior, but no longer rejects handshake states. Instead it will follow the ENet non-connected disconnect branch.

## Internal Changes

- Extend `internal/engine.Host.Disconnect` with the missing handshake-state branch:
  - queue `DISCONNECT | UNSEQUENCED`
  - flush immediately
  - reset the peer locally
- Add `internal/engine.Host.DisconnectNow` for ENet-style immediate forced disconnect
- Expose public `Peer.DisconnectNow` as a thin wrapper over the engine method

The existing connected `Disconnect` and `DisconnectLater` behavior remains intact.

## Testing

Add focused tests for:

- `Disconnect` on a connecting peer:
  - emits unsequenced disconnect
  - flushes immediately
  - resets local state
- `Disconnect` on a connected peer:
  - still emits acknowledged disconnect
- `DisconnectNow` on a connected peer:
  - emits unsequenced disconnect
  - flushes immediately
  - resets local state
- `DisconnectNow` on already-terminal peers:
  - is safe and idempotent

Tests should live primarily at the public API boundary in `host_test.go`.

## Non-Goals

- No new address accessors
- No broader disconnect refactor
- No lint cleanup
- No additional public API surface beyond `DisconnectNow`
