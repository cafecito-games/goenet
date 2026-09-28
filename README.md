# goenet

`goenet` is a pure Go ENet-compatible UDP transport library.

## Goals

- Wire/protocol compatibility with ENet peers over UDP
- A Go-native public API for client/server hosts and peers
- No CGO or native ENet dependency at runtime

## Compatibility Target

`goenet` is developed against an ENet fork snapshot vendored for interop checks in [`internal/interop/vendor`](./internal/interop/vendor). Interoperability checks in [`internal/interop/`](./internal/interop) build and exercise that source directly, and the current wire layout tracks the ENet `2.6.5` era protocol used by it. The interop harness uses that vendored source by default, or `ENET_SOURCE_DIR` when set.

## Installation

```sh
go get github.com/cafecito-games/goenet
```

Import the public API from:

```go
import goenet "github.com/cafecito-games/goenet/pkg"
```

Optional logging uses the standard library's `log/slog`; omitting Logger keeps the library silent.

## Public API

Host construction:

- `Listen(addr string, cfg Config) (*Host, error)`
- `NewHost(cfg Config) (*Host, error)`
- `(*Host).Config() Config`
- `(*Host).LocalAddr() net.Addr`
- `(*Peer).RemoteAddr() net.Addr`
- `(*Host).Close() error`

Host operations:

- `(*Host).Connect(addr string, channelCount uint8, data uint32) (*Peer, error)`
- `(*Host).Service(ctx context.Context, timeout time.Duration) (Event, error)`
- `(*Host).Flush(ctx context.Context) error`
- `(*Host).Broadcast(channelID uint8, packet *Packet) error`

Peer operations:

- `Peer.State() PeerState`
- `Peer.Send(channelID uint8, packet *Packet) error`
- `Peer.Disconnect(ctx context.Context, data uint32) error`
- `Peer.DisconnectNow(ctx context.Context, data uint32) error`
- `Peer.DisconnectLater(ctx context.Context, data uint32) error`
- `Peer.Reset()`

Core value types:

- `Config`
- `Address`
- `Packet`
- `Event`
- `EventType`
- `PacketFlag`
- `PeerState`

## Quick Start

Server:

```go
cfg := goenet.DefaultConfig()
cfg.PeerCount = 64
cfg.ChannelLimit = 2

host, err := goenet.Listen("0.0.0.0:9000", cfg)
if err != nil {
	return err
}
defer host.Close()

for {
	event, err := host.Service(ctx, 50*time.Millisecond)
	if err != nil {
		return err
	}

	switch event.Type {
	case goenet.EventConnect:
		log.Printf("peer connected: state=%v", event.Peer.State())
	case goenet.EventReceive:
		log.Printf("channel=%d bytes=%d", event.ChannelID, len(event.Packet.Data))
	case goenet.EventDisconnect, goenet.EventDisconnectTimeout:
		log.Printf("peer disconnected: data=%d", event.Data)
	case goenet.EventNone:
	}
}
```

Client:

```go
cfg := goenet.DefaultConfig()
cfg.ChannelLimit = 1

host, err := goenet.NewHost(cfg)
if err != nil {
	return err
}
defer host.Close()

peer, err := host.Connect("127.0.0.1:9000", 1, 0xCAFE)
if err != nil {
	return err
}
if err := host.Flush(ctx); err != nil {
	return err
}

for {
	event, err := host.Service(ctx, 50*time.Millisecond)
	if err != nil {
		return err
	}

	switch event.Type {
	case goenet.EventConnect:
		if event.Peer != peer {
			return fmt.Errorf("unexpected peer handle")
		}

		packet := &goenet.Packet{
			Data:  []byte("hello"),
			Flags: goenet.PacketFlagReliable,
		}
		if err := peer.Send(0, packet); err != nil {
			return err
		}
		if err := host.Flush(ctx); err != nil {
			return err
		}
	case goenet.EventDisconnect, goenet.EventDisconnectTimeout:
		return nil
	case goenet.EventNone:
	}
}
```

Logging:

```go
logger := slog.Default()

host, err := goenet.Listen("127.0.0.1:0", goenet.Config{
	PeerCount:    4,
	ChannelLimit: 1,
	Logger:       logger,
})
if err != nil {
	return err
}
defer host.Close()
```

## Event And Packet Semantics

- `Service` advances the host state machine and returns the next public `Event`.
- `EventNone` means the host serviced work or timed out without a user-visible event.
- `PacketFlagReliable` maps to ENet reliable delivery.
- `PacketFlagUnsequenced` maps to ENet unsequenced delivery for outbound and inbound packets.
- `Connect`, `Send`, `Broadcast`, and `DisconnectLater` queue work. Call `Flush` to push queued outbound traffic immediately.
- `Disconnect` is graceful for connected peers. During handshake states it follows ENet's immediate unsequenced disconnect path and resets the peer locally after flush.
- `Disconnect`, `DisconnectNow`, and `DisconnectLater` take a caller-provided `context.Context` because some disconnect paths perform synchronous flush work.
- `DisconnectNow` always uses the immediate unsequenced path: notify the remote peer, flush immediately, and reset the local peer without waiting for a later disconnect event.
- Any path that resets a peer locally—including `Reset`, `DisconnectNow`, and handshake-state `Disconnect` or `DisconnectLater`—invalidates that `Peer` handle immediately. Its operations then return `ErrNilPeer`, `State` reports `PeerStateDisconnected`, and its remote address is unavailable. If the engine reuses the slot, the new session receives a fresh handle; stale handles are never rebound.

## Examples

The executable examples in [`examples/`](./examples) use the exported `goenet` package only. They cover:

- host construction with `Listen`
- client-side `Connect` and explicit `Flush`
- external-package smoke coverage for `Listen`, `NewHost`, `Connect`, `Flush`, and `Close`

## Current Limitations

- Connected-flow disconnects are usable, but handshake-state disconnect behavior is not yet a byte-for-byte match for ENet's special unsequenced fast path.

## Development

This repository uses [Task](https://taskfile.dev) for local automation.

```sh
task ci
```

Common tasks:

- `task fmt`
- `task lint`
- `task test`
- `task test:cover`
- `task build`

Cross-language interoperability against the local ENet fork:

```sh
go test ./internal/interop -count=1
```
