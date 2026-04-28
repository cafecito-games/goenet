# goenet

`goenet` is a pure Go ENet-compatible UDP transport library.

## Goals

This repository is the baseline for a Go-native ENet-style transport layer with:

- a small public API for embedding in dedicated servers and game backends
- protocol compatibility with ENet peers over UDP
- no CGO or external native ENet dependency

## Compatibility Target

`goenet` targets wire-level interoperability with the upstream ENet project at `https://github.com/lsalzman/enet`, aligned to the ENet `2.6.5` constants and protocol layout used by the project reference build. For local development, contributors may keep a checkout or derived single-header mirror of that upstream source, but the canonical compatibility target is the upstream ENet repository plus the `2.6.5` protocol/version details. The initial scaffolding in this repository establishes the package, CI, linting, and development workflow before protocol features are implemented.

## Installation

```sh
go get github.com/cafecito-games/goenet
```

The module currently provides only the baseline package scaffold and will grow as protocol functionality is implemented.

## Minimal Example

The transport implementation is not in place yet, but the intended package shape is a Go client/server library built around host, connect, and service loops:

```go
package main

import (
	"log"
	"time"

	"github.com/cafecito-games/goenet"
)

func main() {
	server, err := goenet.Listen(":7777", goenet.Config{
		Peers:    64,
		Channels: 2,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	client, err := goenet.NewHost(goenet.Config{
		Channels: 2,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	peer, err := client.Connect("127.0.0.1:7777", goenet.ConnectOptions{
		Channels: 2,
		Data:     42,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		if event, err := server.Service(5 * time.Millisecond); err == nil && event != nil {
			switch event.Type {
			case goenet.EventConnect:
				log.Printf("server: peer connected: %v", event.Peer)
			case goenet.EventReceive:
				log.Printf("server: packet on channel %d", event.ChannelID)
			}
		}

		if event, err := client.Service(5 * time.Millisecond); err == nil && event != nil {
			switch event.Type {
			case goenet.EventConnect:
				_ = peer.Send(0, []byte("hello"))
			case goenet.EventDisconnect:
				return
			}
		}
	}
}
```

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
