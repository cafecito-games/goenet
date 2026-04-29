# Interop Harness

This directory contains the cross-language interoperability coverage for `goenet`.

## Setup

The harness builds against the vendored ENet source in `interop/vendor` by default.
If you want to override that, create the local env file first:

```sh
cp interop/.env.example interop/.env
```

Then set `ENET_SOURCE_DIR` in `interop/.env` to your preferred ENet source tree, or export it from your shell before running commands. If you need a different env file location, set `GOENET_INTEROP_ENVFILE`.
Then set `ENET_SOURCE_DIR` in `interop/.env` to your preferred ENet source tree, or export it from your shell before running commands. If you need a different env file location, set `GOENET_INTEROP_ENVFILE`.

The expected ENet layout is:

- `include/enet.h`
- single-header `ENET_IMPLEMENTATION` builds

## Run The Harness

Use the task entrypoint for the normal operator workflow:

```sh
task interop:test
```

If you prefer to override the vendored source directly from your shell:

```sh
ENET_SOURCE_DIR=/absolute/path/to/enet task interop:test
```

The build step still runs on every invocation, but only rebuilds scenarios whose inputs changed. Unchanged scenarios are evaluated and skipped, so repeated runs stay incremental without requiring a separate clean/build phase.

To build one scenario explicitly:

```sh
ENET_SOURCE_DIR=/absolute/path/to/enet ./interop/scripts/build_harness.sh go_server_reliable_exchange
```

## Scenario Layout

The harness is scenario-based:

- `interop/cases/` contains one scenario entrypoint per `.c` file.
- `interop/lib/harness.c` and `interop/include/harness.h` contain the shared C harness helpers used by each scenario.
- `interop/bin/` contains the compiled scenario executables.
- `interop/scripts/build_harness.sh` evaluates the scenario set and rebuilds only what changed.

The Go tests in this directory launch those compiled scenario binaries and exercise the corresponding Go host/client behavior end to end.
