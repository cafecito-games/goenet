# Review Findings Design

## Goal

Fix the four review findings in the public host/peer lifecycle and configuration surface without preserving legacy shims. The resulting API should make cancellation explicit for disconnect operations, report effective runtime configuration consistently, serialize close with other host operations, and reduce routine lifecycle log noise.

## Scope

- Replace the public peer disconnect methods with context-aware signatures:
  - `Peer.Disconnect(ctx context.Context, data uint32) error`
  - `Peer.DisconnectNow(ctx context.Context, data uint32) error`
  - `Peer.DisconnectLater(ctx context.Context, data uint32) error`
- Make close semantics deterministic for concurrent callers and normalize closed-socket failures to `ErrHostClosed`.
- Normalize and validate effective configuration at host construction so `Host.Config()` matches runtime behavior.
- Demote routine lifecycle logs from `Info` to `Debug`, keeping warnings and real failures visible.
- Add unit and interop coverage where the new behavior is externally observable.

## Design

### Lifecycle serialization

`Host.Close` will participate in the same mutex protocol as the other public host operations. Public entrypoints will stop relying on a lock-free closed precheck as their only guard; instead, they will verify closed state after acquiring the host lock so close and active operations cannot race past one another with inconsistent outcomes.

Socket-close errors surfacing from in-flight operations after the host has transitioned closed should be translated to `ErrHostClosed` at the public boundary. Internal socket code can continue returning `net.ErrClosed`; normalization belongs in the public host/peer layer where the API contract is defined.

### Disconnect API

The public disconnect methods will become context-bearing and breaking-change the API immediately. No compatibility wrappers will remain. This makes the synchronous flush behavior in handshake and immediate-disconnect paths explicit to callers and aligns the disconnect methods with `Service` and `Flush`.

The engine API already accepts contexts, so this is mostly a public-surface propagation plus tests and example updates.

### Configuration normalization

Host construction will validate and normalize the caller-supplied config before engine construction. The normalized config snapshot returned by `Host.Config()` will reflect the exact effective values used by the runtime, including derived defaults like `ChannelLimit`.

Validation will reject obviously invalid values early, at construction time, instead of allowing delayed failures from deep engine paths. This includes values like negative `PeerCount` and MTUs that cannot support protocol overhead.

### Logging

Routine host start/close and normal peer connect/disconnect transition records will be emitted at `Debug` instead of `Info`. Rejections, warnings, and hard failures remain at their current severity unless they are clearly ordinary lifecycle noise.

## Testing

- Public API tests for:
  - context-aware disconnect behavior
  - deterministic `ErrHostClosed` behavior after close races
  - normalized `Host.Config()` values and constructor validation errors
  - updated logging expectations
- Engine/socket tests only where they directly cover the underlying invariants needed by the public changes.
- Interop coverage for disconnect flows using the new public API signatures and any scenario where close/disconnect behavior benefits from cross-checking against C ENet behavior.

## Non-goals

- Large internal refactors unrelated to the findings
- Adding compatibility layers for the old disconnect signatures
- Changing protocol behavior beyond what is needed for these fixes
