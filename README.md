# goenet

`goenet` is a pure Go ENet-compatible UDP transport library.

## Goals

This repository is the baseline for a Go-native ENet-style transport layer with:

- a small public API for embedding in dedicated servers and game backends
- protocol compatibility with ENet peers over UDP
- no CGO or external native ENet dependency

## Compatibility Target

`goenet` targets wire-level interoperability with the ENet protocol family so Go services can participate in ENet-based client/server topologies. The initial scaffolding in this repository establishes the package, CI, linting, and development workflow before protocol features are implemented.

## Installation

```sh
go get github.com/cafecito-games/goenet
```

The module currently provides only the baseline package scaffold and will grow as protocol functionality is implemented.

## Minimal Example

The transport implementation is not in place yet, but the intended package shape is a Go client/server library:

```go
package main

import "github.com/cafecito-games/goenet"

func main() {
	clientCfg := goenet.Config{}
	serverCfg := goenet.Config{}

	_, _ = clientCfg, serverCfg
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
