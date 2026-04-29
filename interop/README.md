# Interop Harness

This directory contains the cross-language interoperability coverage for `goenet`.

## Source Of Truth

The harness builds against the vendored ENet source in `interop/vendor` by default. Override that path with `ENET_SOURCE_DIR` when needed.

The current fork layout expected by this repo is:

- `include/enet.h`
- single-header `ENET_IMPLEMENTATION` builds

## Build

```sh
./interop/build_c_harness.sh
```

To write the binary somewhere else:

```sh
ENET_SOURCE_DIR=/path/to/enet ./interop/build_c_harness.sh /tmp/enet-harness
```

## Test Coverage

`go test ./interop -count=1` currently verifies one honest end-to-end path:

- the Go internal engine acting as an ENet-compatible server over a real UDP socket
- the C harness acting as an ENet client
- one connect event
- one reliable packet from C to Go
- one reliable packet from Go back to C

## Current Limitation

The public `goenet` package does not expose host construction, listen/connect, or service-loop APIs yet. Because of that, the interop test drives `internal/engine` directly instead of pretending the public API already exists.

The current engine also does not expose a public or exported helper for initiating an outbound Go-side connect handshake, so this task covers the Go-server/C-client path only.
