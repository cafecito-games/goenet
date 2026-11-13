# goenet

[![CI](https://github.com/cafecito-games/goenet/actions/workflows/pr.yml/badge.svg)](https://github.com/cafecito-games/goenet/actions/workflows/pr.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/cafecito-games/goenet/pkg.svg)](https://pkg.go.dev/github.com/cafecito-games/goenet/pkg)
[![Go Version](https://img.shields.io/github/go-mod/go-version/cafecito-games/goenet)](./go.mod)
[![Latest Release](https://img.shields.io/github/v/release/cafecito-games/goenet)](https://github.com/cafecito-games/goenet/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

**`goenet`** is a pure Go, zero-CGO implementation of the [ENet](http://enet.bespin.org/) reliable UDP networking protocol.

Built for multiplayer games and real-time networked applications, `goenet` speaks the ENet wire protocol directly over standard Go UDP sockets—letting Go servers and clients communicate seamlessly with native C/C++ ENet peers and game engines without linking `libenet`.

---

## Highlights

- **Pure Go, Zero CGO** — Cross-compile static binaries anywhere Go runs without a C toolchain or runtime `libenet` dependency.
- **Wire-Compatible with ENet** — Continuously verified against native C ENet (`2.6.5` wire layout) via an automated cross-language interop harness.
- **Multi-Channel Reliable UDP** — Independent per-channel sequencing, reliable ordered delivery, unreliable sequenced delivery, unsequenced packets, and automatic packet fragmentation and reassembly.
- **Adaptive Congestion & Bandwidth Control** — Round-trip time (RTT) estimation, adaptive packet throttling, and configurable incoming/outgoing bandwidth caps.
- **Idiomatic, Concurrent-Safe Go API** — `context.Context` cancellation, allocation-free `netip.AddrPort` accessors, `errors.Is` sentinel errors, generation-checked `*Peer` handles, and optional structured logging via `log/slog`.
- **Pluggable Wire Hooks** — Custom checksums (`Checksummer`), payload compression (`Compressor`), and raw datagram interception (`Interceptor`).
- **Network Impairment Tooling** — Includes the `enetctl` CLI and embeddable `pkg/lagrelay` UDP proxy for testing latency, jitter, and packet loss over real sockets.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Server](#server)
  - [Client](#client)
- [Core Concepts](#core-concepts)
  - [Delivery Modes](#delivery-modes)
  - [Event Loop & Flushing](#event-loop--flushing)
  - [Peer Lifecycle & Disconnection](#peer-lifecycle--disconnection)
  - [Concurrency](#concurrency)
- [Configuration & Extensibility](#configuration--extensibility)
  - [Host Configuration](#host-configuration)
  - [Structured Logging](#structured-logging)
  - [Error Handling](#error-handling)
- [Network Impairment (`enetctl` & `lagrelay`)](#network-impairment-enetctl--lagrelay)
- [API Overview](#api-overview)
- [Protocol Compatibility](#protocol-compatibility)
- [Development](#development)
- [License](#license)

---

## Installation

Add the library to your Go module:

```sh
go get github.com/cafecito-games/goenet
```

Import the public package:

```go
import goenet "github.com/cafecito-games/goenet/pkg"
```

Optionally install the companion `enetctl` CLI for local network impairment testing:

```sh
go install github.com/cafecito-games/goenet/cmd/enetctl@latest
```

---

## Quick Start

### Server

Bind a server host with `goenet.Listen` and poll events in a `Service` loop:

```go
package main

import (
	"context"
	"log"
	"time"

	goenet "github.com/cafecito-games/goenet/pkg"
)

func main() {
	ctx := context.Background()

	config := goenet.DefaultConfig()
	config.PeerCount = 64
	config.ChannelLimit = 2

	host, err := goenet.Listen("0.0.0.0:9000", config)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	log.Printf("listening on %s", host.LocalAddrPort())

	for {
		event, err := host.Service(ctx, 50*time.Millisecond)
		if err != nil {
			log.Fatal(err)
		}

		switch event.Type {
		case goenet.EventConnect:
			log.Printf("peer connected: %s (data=%#x)", event.Peer.RemoteAddrPort(), event.Data)

		case goenet.EventReceive:
			log.Printf("channel=%d received=%q", event.ChannelID, event.Packet.Data)

			// Echo the payload back reliably on the same channel.
			reply := &goenet.Packet{
				Data:  event.Packet.Data,
				Flags: goenet.PacketFlagReliable,
			}
			if err := event.Peer.Send(event.ChannelID, reply); err != nil {
				log.Printf("send failed: %v", err)
			}

		case goenet.EventDisconnect, goenet.EventDisconnectTimeout:
			log.Printf("peer disconnected: data=%#x", event.Data)

		case goenet.EventNone:
		}
	}
}
```

### Client

Create an ephemeral client host with `goenet.NewHost`, initiate a connection with `Connect`, and service the handshake and replies:

```go
package main

import (
	"context"
	"log"
	"time"

	goenet "github.com/cafecito-games/goenet/pkg"
)

func main() {
	ctx := context.Background()

	config := goenet.DefaultConfig()
	config.ChannelLimit = 2

	host, err := goenet.NewHost(config)
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	peer, err := host.Connect("127.0.0.1:9000", 2, 0xCAFE)
	if err != nil {
		log.Fatal(err)
	}
	if err := host.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	for {
		event, err := host.Service(ctx, 50*time.Millisecond)
		if err != nil {
			log.Fatal(err)
		}

		switch event.Type {
		case goenet.EventConnect:
			log.Printf("connected to %s", peer.RemoteAddrPort())

			packet := &goenet.Packet{
				Data:  []byte("hello from goenet"),
				Flags: goenet.PacketFlagReliable,
			}
			if err := peer.Send(0, packet); err != nil {
				log.Fatal(err)
			}
			if err := host.Flush(ctx); err != nil {
				log.Fatal(err)
			}

		case goenet.EventReceive:
			log.Printf("server replied: %q", event.Packet.Data)
			if err := peer.Disconnect(ctx, 0); err != nil {
				log.Fatal(err)
			}

		case goenet.EventDisconnect, goenet.EventDisconnectTimeout:
			log.Printf("disconnected cleanly")
			return

		case goenet.EventNone:
		}
	}
}
```

More runnable examples using only the exported package surface live in [`examples/`](./examples).

---

## Core Concepts

### Delivery Modes

Packet delivery semantics are selected per packet via `Packet.Flags`:

| Flag | Mode | Ordered | Retransmitted | Fragmented Over MTU |
| :--- | :--- | :---: | :---: | :---: |
| `0` *(default)* | Unreliable Sequenced | Drops older out-of-order packets | No | Yes |
| `PacketFlagReliable` | Reliable Ordered | Strict in-order per channel | Yes | Yes |
| `PacketFlagUnsequenced` | Unreliable Unsequenced | No sequence ordering (deduplicated window) | No | No (must fit peer MTU) |

`Peer.Send` and `Host.Broadcast` copy `Packet.Data` before queuing, so callers can safely reuse or mutate their byte slice immediately after the call returns.

### Event Loop & Flushing

- **`Host.Service(ctx, timeout)`** drives protocol timers, retransmissions, acknowledgements, and socket reads, returning the next user-visible `Event`.
  - A positive `timeout` blocks up to that duration waiting for traffic when idle; `timeout <= 0` polls without blocking.
  - `EventNone` indicates internal protocol work completed or `timeout` elapsed with no user-facing event.
- **`Connect`, `Send`, `Broadcast`, and `DisconnectLater` queue commands** in memory rather than writing to the socket immediately. They are transmitted on the next `Service` tick, or immediately when you call **`Host.Flush(ctx)`**.

### Peer Lifecycle & Disconnection

`goenet` provides four ways to end a peer session, matching ENet's state machine:

| Method | Behavior |
| :--- | :--- |
| `Peer.Disconnect(ctx, data)` | Queues a graceful acknowledged disconnect for connected peers (waits for `EventDisconnect`). During handshake states, sends an immediate unsequenced disconnect, flushes, and resets locally. |
| `Peer.DisconnectLater(ctx, data)` | Waits for queued outbound reliable packets to drain before starting a graceful disconnect. |
| `Peer.DisconnectNow(ctx, data)` | Sends an immediate unsequenced disconnect, flushes synchronously, and resets the peer locally without emitting a later event. |
| `Peer.Reset()` | Drops local peer state immediately with no wire notification. |

**Generation-Safe Handles:** Every `*Peer` handle is bound to a specific session generation. Once a terminal event (`EventDisconnect` or `EventDisconnectTimeout`) is surfaced—or a peer is reset locally via `Reset`, `DisconnectNow`, or a handshake-state disconnect—that `*Peer` handle is permanently detached:
- Subsequent calls on the stale handle return `ErrNilPeer` and `State()` reports `PeerStateDisconnected`.
- If the engine later reuses the underlying peer slot for a new client, the new session receives a fresh `*Peer` instance. Stale handles are never silently rebound to another connection.

### Concurrency

All public methods on `Host` and `Peer` are **safe for concurrent use** across goroutines.

An internal mutex serializes engine access per host. While one goroutine waits inside `Host.Service`, calls such as `Peer.Send` or `Host.Broadcast` from other goroutines wait until the current `Service` tick returns—keep `Service` timeouts short (for example, `5–50ms`) when sending concurrently from separate goroutines.

---

## Configuration & Extensibility

### Host Configuration

Start from `goenet.DefaultConfig()` and customize only what you need:

| Field | Default | Description |
| :--- | :--- | :--- |
| `PeerCount` | `1` | Maximum concurrent peer slots (`1`–`4095`). Override on servers. |
| `ChannelLimit` | `255` | Maximum channels negotiated per peer (`1`–`255`). |
| `MTU` | `1392` | Initial link MTU in bytes (`576`–`4096`). |
| `MaximumPacketSize` | `32 MiB` | Maximum inbound/outbound reassembled packet size. |
| `MaximumWaitingData` | `32 MiB` | Maximum queued inbound data awaiting delivery across channels. |
| `Checksum` | `nil` | Optional `Checksummer` for wire datagram checksums (e.g., CRC32). |
| `Compressor` | `nil` | Optional `Compressor` for payload compression/decompression. |
| `Intercept` | `nil` | Optional `Interceptor` to inspect or consume raw UDP datagrams before ENet decoding. |
| `Logger` | `nil` | Optional `*slog.Logger` (`nil` disables all logging with zero overhead). |

Bandwidth limits can be configured or updated dynamically at runtime (`0` disables a limit):

```go
err := host.BandwidthLimit(incomingBytesPerSec, outgoingBytesPerSec)
```

### Structured Logging

Pass any standard library `*slog.Logger` to receive component-tagged operational logs:

```go
host, err := goenet.Listen("127.0.0.1:9000", goenet.Config{
	PeerCount:    32,
	ChannelLimit: 2,
	Logger:       slog.Default(),
})
```

### Error Handling

Send, flush, and lifecycle operations return wrapped sentinel errors so callers can branch cleanly with `errors.Is`:

```go
if err := peer.Send(0, packet); err != nil {
	switch {
	case errors.Is(err, goenet.ErrPeerNotConnected), errors.Is(err, goenet.ErrNilPeer):
		// Peer already disconnected; clean up session state.
	case errors.Is(err, goenet.ErrPacketTooLarge):
		// Payload exceeds Config.MaximumPacketSize.
	}
}
```

Exported sentinels include `ErrHostClosed`, `ErrNilPeer`, `ErrPeerNotConnected`, `ErrNilPacket`, `ErrChannelOutOfRange`, `ErrPacketTooLarge`, `ErrFragmentationLimit`, `ErrNoFragmentation`, `ErrReliableSequenceExhausted`, `ErrCommandExceedsMTU`, and `ErrShortWrite`.

---

## Network Impairment (`enetctl` & `lagrelay`)

Testing real-time netcode on loopback hides RTT estimation, adaptive throttling, and retransmission edge cases. `goenet` ships with **`lagrelay`**—a transport-agnostic UDP impairment proxy that injects one-way delay, FIFO-preserving jitter, and packet loss over real UDP sockets (one dedicated upstream socket per client session).

### CLI Usage

Start a proxy in front of any UDP or ENet server:

```sh
enetctl lagrelay --listen 127.0.0.1:7777 --upstream 127.0.0.1:9000 --preset 4g
```

`lagrelay` prints a single JSON ready line (`listen`, `upstream`, `control_url`, `profile`) to `stdout` and writes structured logs (`--log-level`, `--log-format auto|text|json`) to `stderr`.

Inspect stats or switch impairment profiles live without dropping active client connections:

```sh
enetctl lagrelay status
enetctl lagrelay set bad
enetctl lagrelay set --up-ms 80 --down-ms 80 --jitter-ms 25 --loss-pct 1
```

| Preset | Up Delay | Down Delay | Jitter (0..N) | Packet Loss |
| :--- | :---: | :---: | :---: | :---: |
| `none` | 0 ms | 0 ms | 0 ms | 0% |
| `good` | 20 ms | 20 ms | 3 ms | 0% |
| `4g` | 60 ms | 60 ms | 20 ms | 0.5% |
| `bad` | 150 ms | 150 ms | 60 ms | 2% |
| `awful` | 300 ms | 300 ms | 120 ms | 5% |
| `jitter` | 30 ms | 30 ms | 100 ms | 0% |
| `lossy` | 30 ms | 30 ms | 5 ms | 10% |

Automation scripts can also query or update the loopback control endpoint directly (`GET` / `POST` `http://127.0.0.1:7790/profile`).

### Embedding in Go Tests

Embed the impairment proxy directly inside integration tests via [`pkg/lagrelay`](./pkg/lagrelay):

```go
import "github.com/cafecito-games/goenet/pkg/lagrelay"

preset, _ := lagrelay.Preset("4g")
relay, err := lagrelay.New(lagrelay.Config{
	Upstream:       serverHost.LocalAddrPort().String(),
	InitialProfile: preset,
	Seed:           42, // deterministic jitter and loss streams
})
if err != nil {
	t.Fatal(err)
}
go relay.Run(ctx)
defer relay.Close()

// Point your client at relay.Addr().String() and adjust conditions mid-test:
_ = relay.SetProfile(lagrelay.Profile{UpDelayMS: 120, DownDelayMS: 120, LossPercent: 3})
```

---

## API Overview

Full API documentation is available on [pkg.go.dev](https://pkg.go.dev/github.com/cafecito-games/goenet/pkg).

- **Host Lifecycle & Operations:** `Listen`, `NewHost`, `(*Host).Connect`, `(*Host).Service`, `(*Host).Flush`, `(*Host).Broadcast`, `(*Host).BandwidthLimit`, `(*Host).Config`, `(*Host).LocalAddrPort`, `(*Host).LocalAddr`, `(*Host).Close`
- **Peer Operations:** `(*Peer).Send`, `(*Peer).Disconnect`, `(*Peer).DisconnectLater`, `(*Peer).DisconnectNow`, `(*Peer).Reset`, `(*Peer).State`, `(*Peer).RemoteAddrPort`, `(*Peer).RemoteAddr`, `PeerSender` (embeddable interface for consumer mocks/wrappers)
- **Core Types:** `Config`, `Packet`, `PacketFlag`, `Event`, `EventType`, `PeerState`, `Address`, `Checksummer`, `Compressor`, `Interceptor`, `InterceptDecision`, `InterceptResult`

---

## Protocol Compatibility

`goenet` targets wire compatibility with the ENet `2.6.5` protocol layout (as maintained in [`zpl-c/enet`](https://github.com/zpl-c/enet), snapshot vendored under [`internal/interop/vendor`](./internal/interop/vendor) for testing only).

- **IPv4 & IPv6:** Supports both IPv4 and IPv6 UDP endpoints (including IPv6 scope IDs).
- **Cross-Language Verification:** [`internal/interop`](./internal/interop) compiles native C ENet test binaries and verifies handshakes, multi-peer traffic, fragmentation, bandwidth limits, idle reconnects, and client/server disconnects against `goenet` end-to-end.
- **Known Nuance:** Connected-peer disconnects interoperate cleanly in both directions; disconnecting mid-handshake currently resets locally after flushing an unsequenced disconnect, which is not yet a byte-for-byte match for every edge of C ENet's handshake fast path.

---

## Development

This repository uses [Task](https://taskfile.dev) for local workflows:

| Command | Description |
| :--- | :--- |
| `task ci` | Run the full local CI pipeline (formatting, `go.mod` tidy check, lint, race tests, build) |
| `task test` | Run unit tests with the race detector (`go test -race -count=1 ./...`) |
| `task test:cover` | Run race-enabled tests and print statement coverage |
| `task interop:test` | Build the C ENet harness incrementally and run cross-language interop tests |
| `task lint` | Run `golangci-lint` (including SPDX license header checks) |
| `task lint:fix` | Run `golangci-lint --fix` to apply automatic lint and header fixes |
| `task fmt` | Format code with `gofmt` and `goimports` |
| `task build` | Build all packages and CLI binaries |

To run cross-language interop tests against an external ENet checkout instead of the vendored snapshot:

```sh
ENET_SOURCE_DIR=/path/to/enet task interop:test
```

---

## License

Released under the [MIT License](./LICENSE).

