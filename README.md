# goenet

`goenet` is a pure Go ENet-compatible UDP transport library.

## Goals

This repository is the baseline for a Go-native ENet-style transport layer with:

- a small public API for embedding in dedicated servers and game backends
- protocol compatibility with ENet peers over UDP
- no CGO or external native ENet dependency

## Compatibility Target

`goenet` targets wire-level interoperability with the ENet fork checked out locally at `/Users/christian/CafecitoGames/enet`. That fork's single-header `include/enet.h` is the development source of truth used by this repository for interoperability verification, including the connect and packet-exchange harness in [`interop/`](./interop). The protocol constants and layout in `goenet` are still aligned with the ENet `2.6.5` era wire format exercised by that fork.

## Installation

```sh
go get github.com/cafecito-games/goenet
```

The module currently exposes the data types and compatibility constants needed by the engine and tests. Public host construction, listen/connect, and service-loop APIs are not exported yet.

## Current Public Surface

Server-side configuration currently looks like this:

```go
cfg := goenet.DefaultConfig()
cfg.PeerCount = 64
cfg.ChannelLimit = 2
```

Packets and event values already have stable public types:

```go
packet := goenet.Packet{
	Data:  []byte("hello"),
	Flags: goenet.PacketFlagReliable,
}

event := goenet.Event{
	Type:      goenet.EventReceive,
	ChannelID: 0,
	Packet:    &packet,
}
```

The example coverage in [`examples/`](./examples) is intentionally limited to this real package surface. Interoperability tests that exercise live connect and packet exchange are in [`interop/`](./interop) and currently drive `internal/engine` directly until the public host API exists.

## Development

This repository uses [Task](https://taskfile.dev) for local automation.

```sh
task ci
```

Available baseline tasks include:

- `task fmt`
- `task lint`
- `task test`
- `task test:cover`
- `task build`

For cross-language interoperability checks against the local ENet fork:

```sh
go test ./interop -count=1
```
