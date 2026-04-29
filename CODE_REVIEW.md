# Code Review

## Findings

### 1. `context.Context` cancellation is not honored by the real UDP adapter
Severity: High

Files:
- `internal/socket/udp.go:22`
- `internal/socket/udp.go:38`
- `internal/engine/receive.go:93`
- `internal/engine/send.go:220`

`internal/socket.UDP` only translates `ctx.Deadline()` into socket deadlines. If the caller passes a canceled context without a deadline, `ReadPacket` and `WritePacket` still block in the underlying `net.UDPConn` call instead of returning promptly with `ctx.Err()`.

That leaks through the public API:
- `(*Host).Service` calls `receiveIncoming`, which calls `ReadPacket`.
- `(*Host).Flush` calls `WritePacket`.

So `context.WithCancel` is not sufficient to interrupt a blocked service/flush on the real socket path. The fake socket used in tests *does* honor `ctx.Done()`, which makes this easy to miss and means the production behavior diverges from the test double.

This is both a correctness issue and an idiomatic Go issue: once an API accepts `context.Context`, callers reasonably expect cancellation to work even when no explicit deadline is attached.

### 2. IPv6 scope handling is broken end-to-end despite the dedicated `Address` abstraction
Severity: High

Files:
- `host.go:257`
- `internal/engine/receive.go:344`
- `internal/core/address.go:18`
- `address.go:9`

The codebase has an explicit `Address` type and internal `scopeID` support, but the actual socket-to-core conversion path drops or rejects that information:

- Outbound connects convert `*net.UDPAddr` to `core.Address` with `scopeID` hard-coded to `0` in `coreAddressFromUDPAddr`.
- Inbound peer creation does the same in `handleConnect`.
- `core.NewAddress` rejects zoned IPv6 addresses entirely, so a link-local IPv6 address that carries a zone from the socket layer will fail construction rather than being normalized into `addr + scopeID`.

The net effect is that the code advertises IPv6 scope preservation but the runtime path does not preserve it. That is a real correctness bug for scoped IPv6 and a separation-of-concerns problem: the project has already decided that zone metadata should be represented explicitly, but the host/engine boundary bypasses that design.

The only direct tests here are constructor-level tests for `NewAddress`; there is no service/connect coverage for scoped IPv6 behavior.

### 3. The public extensibility layer is largely untested, especially the event-synthesizing intercept path
Severity: Medium

Files:
- `checksum.go:15`
- `compressor.go:11`
- `intercept.go:30`
- `event.go:30`
- `config.go:37`

The engine hook behavior is tested well at the `internal/core` layer, but the public `goenet` adapters are not. In particular:

- `checksummerAdapter` / `coreChecksummerAdapter`
- `compressorAdapter` / `coreCompressorAdapter`
- `interceptorAdapter`
- `toCoreEvent`

There are tests for internal hooks in `internal/engine/hooks_test.go`, but nothing in the public package proves that:

- a public `Checksummer` is wired through correctly
- a public `Compressor` is wired through correctly
- a public `Interceptor` can synthesize an event through `InterceptDecision.Event`
- packet and event translations preserve the intended public semantics

That leaves one of the most important API seams effectively unverified. The intercept path is the weakest point because it crosses both interface adaptation and event translation, and a bug there would not be caught by the existing engine-focused tests.

### 4. Configuration normalization is duplicated across three layers
Severity: Medium

Files:
- `host.go:154`
- `config.go:37`
- `internal/engine/host.go:51`

The same normalization/defaulting logic exists in three places:

- `goenet.normalizeConfig`
- `goenet.toCoreConfig`
- `engine.NewHost`

This is not a current behavioral bug, but it is poor separation of concerns and raises drift risk. A change to defaults or normalization rules now has to stay synchronized in multiple locations. The duplicated `ChannelLimit`, `MTU`, packet-size, waiting-data, and hook wiring logic is exactly the kind of code that quietly forks over time.

Given the architecture, there should be one authoritative normalization step and the other layers should translate data, not re-decide defaults.

### 5. There is currently unreachable adapter code around default hooks
Severity: Low

Files:
- `config.go:18`
- `checksum.go:24`
- `compressor.go:25`
- `internal/core/types.go:128`

`goenet.DefaultConfig` contains branches to wrap default internal checksum/compressor hooks back into public interfaces:

- `coreChecksummerAdapter`
- `coreCompressorAdapter`

But `internal/core.DefaultConfig()` currently returns nil for those hooks, so those branches are not reachable in the current codebase. Nothing tests them either.

This is not harmful by itself, but it is dead-code-adjacent: it increases surface area, complicates review, and suggests a capability that the current defaults do not actually exercise. Either those adapter branches should be justified by a near-term use case, or they should be removed until they are needed.

## Coverage and Test Notes

- The repo has strong protocol and engine tests, especially around packet encoding, sequencing, ACK handling, and interop scenarios.
- The weakest test areas are the public adapter layer and the real socket adapter behavior.
- `internal/socket` has no package-local tests for deadline/cancellation semantics.
- Address tests cover constructor behavior, but not real host/service flows for scoped IPv6.
- The public hook adapters are not directly exercised from `goenet` tests.

## Dead Code / Low-Signal Surface

- `coreChecksummerAdapter` and `coreCompressorAdapter` look currently unreachable from shipped defaults.
- `toCoreEvent` is only meaningful through the public interceptor synth-event path, which is not directly tested.

## Overall Assessment

The protocol core is in good shape and the internal decomposition is mostly sensible. The main concerns are at the edges:

- real socket behavior does not fully match the `context.Context` contract
- the advertised IPv6 scope model is not carried through the runtime path
- the public adapter layer has meaningful blind spots

Those are worth fixing before treating the public API layer as robust.
