# goenet Public API Under `pkg` Design

**Date:** 2026-04-29

**Goal:** move the repository's supported public Go API from the module root to `pkg/`, relocate the interop harness under `internal/`, and align local pre-commit hooks with the same lint entrypoint used in CI.

## Scope

This design covers:

- moving the exported `goenet` package from the module root to `pkg/`
- updating tests, examples, and documentation to import `github.com/cafecito-games/goenet/pkg`
- moving the Go interop harness from `interop/` to `internal/interop/`
- updating build scripts, Task targets, and repo references for the new interop location
- ensuring pre-commit uses the same lint command path as CI and installing hooks with `prek`

This design does not cover:

- preserving backwards compatibility for the old root import path
- splitting interop into a separate module
- changing the underlying lint toolchain used in CI

## Problem

The repository currently exposes its public package from the module root while also keeping a top-level `interop/` area that is test-only and not intended to be consumed as API. That shape makes the public boundary less clear than it should be.

The repo also already has a `.pre-commit-config.yaml`, but the requested outcome is stronger than simply having the file present: the installed hook set should execute the same linting entrypoint as CI so local checks and CI checks stay aligned.

## Approach Options

### 1. Move public API to `pkg/` and move interop to `internal/interop/` (recommended)

This makes the intended API boundary explicit and keeps the interop harness out of the public-looking top-level namespace.

**Pros**

- clear public import path
- clear non-public placement for interop
- minimal conceptual ambiguity for future contributors
- keeps one module and one toolchain

**Cons**

- repo-wide path churn
- requires careful script and test path updates

### 2. Move only the public API to `pkg/`

This gives the new import path, but leaves the interop harness at the top level.

**Pros**

- slightly less file movement

**Cons**

- still leaves non-API repo surface in a public-looking location
- does not satisfy the goal of preventing accidental export pressure around interop

### 3. Keep root package and add forwarding wrappers from `pkg/`

This avoids breaking imports, but the user explicitly wants a breaking change and there are no compatibility constraints to preserve.

**Pros**

- low migration burden for consumers

**Cons**

- keeps two API paths alive
- adds maintenance noise
- does not create a clean boundary

## Recommendation

Use option 1.

The module root should stop containing the supported public package implementation. The supported import path becomes `github.com/cafecito-games/goenet/pkg`. The interop harness should live under `internal/interop/` so it is structurally marked as non-public and cannot be imported by external consumers.

## Package Layout

After the refactor:

- public API source files move from the module root to `pkg/`
- public-package tests that validate the exported surface move with that package or update imports accordingly
- `examples/` and any external-package tests import `github.com/cafecito-games/goenet/pkg`
- internal implementation packages remain under `internal/...`
- the interop harness tree moves from `interop/` to `internal/interop/`

The module path in `go.mod` remains `github.com/cafecito-games/goenet`.

## Interop Relocation

`internal/interop/` should remain runnable as a repo-local test harness, not a supported external package.

Required updates:

- `Taskfile.yml` changes `task interop:test` to build from `./internal/interop/scripts/build_harness.sh` and run `go test ./internal/interop -count=1`
- harness shell scripts compute paths relative to `internal/interop`
- Go test helpers compute fixture, script, binary, and vendor paths relative to the relocated directory
- README and any test assertions referencing `interop/...` update to `internal/interop/...`

The vendored ENet source stays with the harness under `internal/interop/vendor`.

## Pre-commit

`.pre-commit-config.yaml` should continue to use `task lint` as the lint hook entry because CI runs lint through `task ci` and `task lint` is the authoritative lint target there. If any hook currently diverges from CI for linting, it should be changed to match `task lint`.

The repository should then have hooks installed with:

`prek install`

If `prek` exposes an install mode specific to pre-commit hooks in this repo, that command should be used so the checked-in `.pre-commit-config.yaml` becomes active immediately.

## Testing Strategy

Drive the refactor with path-sensitive tests first where needed, then verify the whole repository with the new layout.

Required verification:

- targeted package-path tests fail before path updates and pass after
- `go test ./...`
- `task lint`
- `task interop:test`
- a direct pre-commit run against all files, or an equivalent `prek`-installed hook invocation, to confirm the hook set is functional

## Risks

- string/path assumptions in interop tests or scripts may break after relocation
- examples and external-package tests may retain stale imports
- README assertions may fail if prose and tests drift during the move
- pre-commit installation may succeed while hook execution still fails if local tool prerequisites are missing

## Success Criteria

- consumers must import `github.com/cafecito-games/goenet/pkg` for the public API
- no top-level `interop/` Go package remains
- interop harness commands and tests pass from `internal/interop`
- local hooks are installed with `prek` and run the same lint target as CI
