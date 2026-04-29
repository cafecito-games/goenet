# Package Public API Under `pkg` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** move the supported public API from the module root to `pkg/`, relocate interop to `internal/interop/`, and keep local hooks aligned with CI linting.

**Architecture:** preserve the current code structure and behavior, but change the package boundary. The root package implementation moves into `pkg/`, all external-style tests/examples import `github.com/cafecito-games/goenet/pkg`, and the interop harness moves under `internal/interop` with task/script/doc updates.

**Tech Stack:** Go 1.26, Task, golangci-lint, pre-commit via `prek`, shell path updates, existing internal packages.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `pkg/*.go` | Supported public package implementation moved from the repo root. |
| `pkg/*_test.go` | Package-local tests that should stay with the public package. |
| `examples/*.go` | External usage examples importing `github.com/cafecito-games/goenet/pkg`. |
| `internal/interop/**` | Non-public interop harness, scripts, C sources, vendor snapshot, and tests. |
| `Taskfile.yml` | Interop task path updates. |
| `.pre-commit-config.yaml` | Hook entrypoints; lint stays on `task lint`. |
| `README.md` | Public import path and interop location updates. |

## Task 1: Move the public package to `pkg/`

**Files:**
- Create: `pkg/`
- Move: root public `.go` files and package-local tests into `pkg/`
- Modify: imports in examples and external-package tests

- [ ] Write a failing import-path smoke test by running `go test ./...` after moving only one representative external import.
- [ ] Move the root `goenet` package source files into `pkg/`.
- [ ] Update external-package tests/examples to import `github.com/cafecito-games/goenet/pkg`.
- [ ] Run `go test ./...` and fix compile breaks until the new import path is clean.

## Task 2: Move interop under `internal/interop`

**Files:**
- Move: `interop/**` -> `internal/interop/**`
- Modify: `Taskfile.yml`, README references, path-sensitive Go tests, shell scripts

- [ ] Move the interop tree under `internal/interop/`.
- [ ] Update Task targets and runtime path helpers to the new location.
- [ ] Run `go test ./internal/interop -count=1` and fix path/reference failures.

## Task 3: Align and install pre-commit hooks

**Files:**
- Verify: `.pre-commit-config.yaml`
- Install: repo hooks via `prek`

- [ ] Confirm the lint hook still uses `task lint`, matching CI.
- [ ] Install hooks with `prek`.
- [ ] Run the hook set against the repo to confirm it executes successfully.

## Task 4: Final verification and delivery

**Files:**
- Modify as needed from verification fallout

- [ ] Run `go test ./...`.
- [ ] Run `task lint`.
- [ ] Run `task interop:test`.
- [ ] Run formatting/tidy if verification requires it.
- [ ] Commit the branch changes.
- [ ] Push the branch and create a PR.
