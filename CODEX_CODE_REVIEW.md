# Code Review

## Summary

Review focus: correctness, idiomatic Go, separation of concerns, dead code, and untested code.

Verification performed:

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `golangci-lint run ./...`
- `go test ./... -coverprofile=/tmp/goenet.cover.out`
- `go tool cover -func=/tmp/goenet.cover.out`

All tests, vet, lint, and race checks passed. The main concerns are not broad build health; they are a small number of lifecycle/correctness issues plus several compatibility-sensitive paths that are still weakly or not directly tested.

## Findings

### 1. High: stale public `Peer` handles are not actually invalidated after disconnect

Files:

- [host.go](/Users/christian/CafecitoGames/goenet/host.go:324)
- [peer.go](/Users/christian/CafecitoGames/goenet/peer.go:49)

Details:

- `Host.translateEvent` deletes the wrapper from `h.peers` on `EventDisconnect` / `EventDisconnectTimeout`, but it does not clear `wrapped.raw` or otherwise invalidate the old `*Peer`.
- `Peer.lockedRaw` only checks whether `p.raw` is `nil`; if the old wrapper still points at the reused engine peer slot, the stale handle remains live.
- The comment in `translateEvent` says the delete is intended to prevent a stale public handle from binding to a future session, but the old wrapper still holds the same raw pointer. If the engine reuses that slot for a later connection, both the stale `*Peer` and the newly-created `*Peer` can act on the same underlying session.

Why this matters:

- This is a correctness and API-identity bug, not just a cosmetic wrapper issue.
- A caller can accidentally send, disconnect, or query state on a later peer session through an old handle.
- It also shows the public layer is too tightly coupled to engine slot reuse semantics.

Recommended fix:

- Explicitly invalidate terminal public peer handles by clearing their raw binding when a disconnect event is surfaced.
- Add a regression test that disconnects a peer, reconnects into the same slot, and verifies the old handle fails with `ErrNilPeer` while the new handle remains usable.

### 2. Medium: scoped IPv6 addresses stringify in the wrong shape, and tests currently lock in the bug

Files:

- [internal/core/address.go](/Users/christian/CafecitoGames/goenet/internal/core/address.go:45)
- [internal/core/address_test.go](/Users/christian/CafecitoGames/goenet/internal/core/address_test.go:56)

Details:

- `Address.String()` renders a scoped IPv6 endpoint as `fmt.Sprintf("%s%%%d", a.addrPort.String(), a.scopeID)`.
- For `[fe80::1]:1234` with scope `7`, that produces `[fe80::1]:1234%7`.
- Standard address formatting puts the zone on the host, not after the port: `[fe80::1%7]:1234`.
- The current test expects the malformed representation, so coverage masks the issue rather than catching it.

Why this matters:

- Any logging, debugging, or string-based round-tripping that uses `Address.String()` will emit a non-standard address.
- This is especially risky for diagnostics and any future code that tries to feed the string back into `net` parsing APIs.

Recommended fix:

- Rebuild the string from the host and port components instead of appending `%scope` to `AddrPort.String()`.
- Update the test to expect a standards-compliant scoped IPv6 form.

### 3. Low: `(*engine.Host).ServiceTime` appears to be dead code

File:

- [internal/engine/host.go](/Users/christian/CafecitoGames/goenet/internal/engine/host.go:111)

Details:

- Repository search found no callers for `ServiceTime()`.
- Coverage is `0.0%`.
- The comment says it exists primarily for tests, but no tests use it.

Why this matters:

- It adds API surface inside `internal/engine` without evidence that it is needed.
- Dead helper accessors tend to survive unnoticed and make later refactors less clear.

Recommended fix:

- Remove it if it is no longer needed, or add the intended test/consumer if it is meant to stay.

## Untested Or Weakly Tested Areas

### Public compressor receive path is untested

Files:

- [compressor.go](/Users/christian/CafecitoGames/goenet/compressor.go:20)
- [public_hooks_test.go](/Users/christian/CafecitoGames/goenet/public_hooks_test.go:58)

Details:

- Coverage shows `compressorAdapter.Decompress` at `0.0%`.
- The public hook tests verify checksum/compression on flush, but they do not validate the public `Config.Compressor` path on inbound decompression.
- Internal engine tests cover compressor behavior at the core layer, but not the public adapter boundary.

Risk:

- A mismatch between the public adapter and the internal compressor contract would not currently be caught by public-package tests.

### Several inbound protocol handlers are implemented but not directly exercised

File:

- [internal/engine/receive.go](/Users/christian/CafecitoGames/goenet/internal/engine/receive.go:719)

Details:

- Coverage shows `0.0%` for:
  - `handleSendUnreliableFragment`
  - `handleBandwidthLimit`
  - `handleThrottleConfigure`
- These are compatibility-sensitive receive-side branches.
- `handleSendUnreliableFragment` is especially worth calling out because fragment reassembly bugs are usually subtle and expensive to debug in production.

Risk:

- These paths are likely not dead, but they are effectively unverified.
- Protocol compatibility can silently regress even while the rest of the suite remains green.

Recommended follow-up:

- Add direct engine tests for:
  - successful unreliable fragment reassembly and duplicate handling
  - inbound `BandwidthLimit` updates
  - inbound `ThrottleConfigure` updates

### Real UDP adapter behavior is lightly tested

Files:

- [internal/socket/udp.go](/Users/christian/CafecitoGames/goenet/internal/socket/udp.go:25)
- [internal/socket/udp_test.go](/Users/christian/CafecitoGames/goenet/internal/socket/udp_test.go:14)

Details:

- Coverage is only `11.1%` for `ReadPacket` and `15.4%` for `WritePacket`.
- Existing tests cover canceled contexts and a few address conversion cases, but not:
  - successful read/write on real sockets
  - timeout polling behavior
  - `ECONNREFUSED` suppression on reads
  - `Close()`

Risk:

- This is OS-facing transport glue. It is exactly where platform-specific bugs tend to hide.

### Public `Host.LocalAddrPort` is exported but untested

Files:

- [host.go](/Users/christian/CafecitoGames/goenet/host.go:87)
- [README.md](/Users/christian/CafecitoGames/goenet/README.md:21)

Details:

- `LocalAddrPort()` has `0.0%` coverage.
- It is also missing from the README public API list even though it is a useful exported API.

Risk:

- Small surface area, but this is exactly the kind of convenience API that can drift or regress quietly because nothing exercises it.

## Architectural Notes

- The public API layer is generally thin and readable, but peer identity/lifecycle is still coupled too directly to internal engine slot reuse. The stale-handle bug is the clearest symptom.
- The protocol and queueing code are much stronger than the socket/public adapter edges. The test suite is doing the hard work in the transport core, but the boundaries still need more attention.
- Static analysis hygiene is good. `go vet`, `golangci-lint`, and `-race` all came back clean.

## Suggested Next Steps

1. Fix stale `Peer` invalidation first and add a reconnect-slot regression test.
2. Fix `core.Address.String()` for scoped IPv6 and correct the test expectation.
3. Remove or justify `(*engine.Host).ServiceTime`.
4. Add direct tests for public inbound decompression, inbound bandwidth/throttle commands, unreliable fragment reassembly, and real UDP adapter happy/error paths.
