# goenet

`goenet` is a pure Go ENet-compatible UDP transport library.

## Goals

- Wire/protocol compatibility with ENet peers over UDP
- A Go-native public API for client/server hosts and peers
- No CGO or native ENet dependency at runtime

## Compatibility Target

`goenet` is developed against the local ENet fork at `/Users/christian/CafecitoGames/enet`. Interoperability checks in [`interop/`](./interop) build and exercise that fork directly, and the current wire layout tracks the ENet `2.6.5` era protocol used by it.

## Installation

```sh
go get github.com/cafecito-games/goenet
```

## Public API

Host construction:

- `Listen(addr string, cfg Config) (*Host, error)`
- `NewHost(cfg Config) (*Host, error)`
- `(*Host).Config() Config`
- `(*Host).Close() error`

Host operations:

- `(*Host).Connect(addr string, channelCount uint8, data uint32) (*Peer, error)`
- `(*Host).Service(ctx context.Context, timeout time.Duration) (Event, error)`
- `(*Host).Flush(ctx context.Context) error`
- `(*Host).Broadcast(channelID uint8, packet *Packet) error`

Peer operations:

- `Peer.State() PeerState`
- `Peer.Send(channelID uint8, packet *Packet) error`
- `Peer.Disconnect(data uint32) error`
- `Peer.DisconnectNow(data uint32) error`
- `Peer.DisconnectLater(data uint32) error`
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

## Event And Packet Semantics

- `Service` advances the host state machine and returns the next public `Event`.
- `EventNone` means the host serviced work or timed out without a user-visible event.
- `PacketFlagReliable` maps to ENet reliable delivery.
- `PacketFlagUnsequenced` maps to ENet unsequenced delivery.
- `Connect`, `Send`, `Broadcast`, `Disconnect`, and `DisconnectLater` queue work. Call `Flush` to push queued outbound traffic immediately.
- `Disconnect` is graceful for connected peers. During handshake states it follows ENet's immediate unsequenced disconnect path and resets the peer locally after flush.
- `DisconnectNow` always uses the immediate unsequenced path: notify the remote peer, flush immediately, and reset the local peer without waiting for a later disconnect event.

## Examples

The executable examples in [`examples/`](./examples) use the exported `goenet` package only. They cover:

- host construction with `Listen`
- client-side `Connect` and explicit `Flush`
- external-package smoke coverage for `Listen`, `NewHost`, `Connect`, `Flush`, and `Close`

## Current Limitations

- Public address/introspection helpers are still minimal. The public API does not yet expose local bound address or remote peer address accessors.
- `DisconnectNow` is available, but the public API still does not expose ENet's full disconnect method family beyond `Disconnect`, `DisconnectNow`, `DisconnectLater`, and `Reset`.

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
go test ./interop -count=1
```
