# CLAUDE.md

This file provides guidance to coding agents working with code in this repository. `AGENTS.md` is a symlink to this file, so agents that look for either name get the same guidance.

## Project Overview

Oxia is a distributed metadata store and coordination system written in Go, designed for large-scale distributed systems. It features a sharding architecture optimized for cloud-native applications.

## Build Commands

```bash
make build          # Build bin/oxia binary
make test           # Run all tests with coverage and race detection
make lint           # Run golangci-lint across all workspace modules
make proto          # Regenerate gRPC code from .proto files
make docker         # Build Docker image (oxia:latest)
make license-check  # Verify copyright headers
make license-format # Auto-format copyright headers
```

## Running a Single Test

```bash
go test -v -run TestName ./path/to/package/...
```

## Project Structure

The codebase uses Go workspaces with 5 modules:

- **cmd/** - CLI entry points (Cobra-based). Commands: `server`, `coordinator`, `client`, `admin`, `standalone`, `health`, `wal`, `perf`
- **common/** - Shared code: gRPC proto definitions (`common/proto/`), RPC utilities, metrics, security
- **oxia/** - Client library exposing `SyncClient` and `AsyncClient` interfaces
- **oxiad/** - Server implementations:
  - `coordinator/` - Cluster management, shard orchestration, leader election
  - `dataserver/` - Data storage using Pebble (LSM-tree KV store)
  - `dataserver/wal/` - Write-ahead log implementation
- **tests/** - Integration tests

## Architecture

```
Client (oxia/) ──gRPC──> Coordinator (oxiad/coordinator/)
                              │
                              ▼
                        Data Servers (oxiad/dataserver/)
                        [Sharded, Pebble-backed storage]
```

Key components:
- **Coordinator**: Manages cluster state, shard assignments, and leader balancing
- **Data Servers**: Handle actual data storage/retrieval with WAL for durability
- **Client Library**: Provides sync/async APIs with automatic request batching

## Code Style

- Line length limit: 120 characters
- Import grouping: stdlib, external, then `github.com/oxia-db/oxia` (enforced by goimports)
- All Go files require Apache 2.0 license header
- Cyclomatic complexity limit: 15
- Cognitive complexity limit: 25
- Comments explain why, not what: don't restate the code, and keep them short

## Errors and Retries

- **Return errors, don't retry inside**: a function that does the work returns its error to the caller, and never
  retries in place. The caller decides whether and how to retry.
- **Retry in a wrapper**: when an operation must be retried, wrap it in `xxxWithRetries`, which calls `xxx`, e.g.
  `deleteShardWithRetries` and `deleteShard`. Retry with `oxiatime.NewBackOff(ctx)` so that it stops when the
  component closes, and mark the errors that must not be retried with `backoff.Permanent`.
- **Don't block an event loop**: a handler of an event loop never retries in place. If it fails, it sends its
  message to the channel again later, e.g. with `time.AfterFunc(time.Second, ...)`, or leaves it to the next
  periodic round. A request that has a caller waiting for it, e.g. a change of ensemble, fails fast instead and its
  caller retries it.

## Coordinator Metadata

- **Handle the errors**: the reads and writes of `metadata.Metadata` return their errors without retrying.
- **Borrowed values**: a `commonobject.Borrowed` value is shared with every other reader. Only read it, and only
  while you use it; clone it to keep it or to modify it.
- **Write only what you own**: update a namespace status with `PatchNamespaceStatus`, and change only the fields
  that the caller owns in the status it gets, never by storing back a copy read earlier, which would revert the
  concurrent writes of the other components.

## Test Structure

- **One test file per source file**: the tests of `xxx.go` live in `xxx_test.go`, in the same package. Don't add
  test files per feature or per scenario (e.g. `xxx_leadership_test.go`); if a test file grows too large, split the
  source file, and its tests follow.
- **Naming**: name the tests `Test<Type><Method><Behavior>`, e.g. `TestRuntimeCreateNamespaceRepairsCommittedWrite`,
  or `Test<Function><Behavior>` for a function, without underscores.
- **Layout**: order the tests by the method they test, and put the fakes and helpers at the end of the file.
- **Fakes**: prefer one fake per interface, with a hook for each method a test overrides, over a fake type per test.
- **End-to-end tests**: the tests that run a cluster or go through the public endpoints live in the top-level
  `tests/` module, not in the package.

## Commits and Pull Requests

- Sign off every commit with `git commit -s`: the DCO check fails without it.
- Prefix the titles with the type of change: `fix:`, `feat:`, `perf:`, `refactor:`, `test:`, `docs:`, `ci:`, `chore:`.
- Keep a pull request to one change, and its description short. Split a larger change into stacked pull requests.

## Concurrency: Avoid New Shared Mutable State

Shared mutable state (a struct field, package-level variable, map, or slice that more than one goroutine reads
or writes) is the main source of data races and lock-ordering bugs in this codebase. Treat adding one as a design
decision, not a convenience.

Before introducing new shared state, stop and look for a design that avoids it:

- **Confine ownership**: keep the state inside a single goroutine (e.g. an event loop or controller run loop) and
  have other goroutines talk to it through channels or by submitting work to that loop.
- **Pass, don't share**: pass values as function arguments or return values, or send immutable copies/snapshots
  instead of exposing a pointer to live data.
- **Derive instead of cache**: recompute the value from existing authoritative state (e.g. metadata, the WAL,
  Pebble) rather than mirroring it in a second variable that has to be kept in sync.
- **Reuse existing state**: extend a structure that already has a clear owner and synchronization rather than adding
  a parallel one.
- **Make it immutable**: build it once before any goroutine starts and never modify it afterwards.

Only when none of these fit should you add shared mutable state, and then make ownership explicit:

- **Exactly one owner**: name the single component or goroutine that owns the state. Only the owner creates,
  mutates, and tears it down.
- **Everyone else is a borrower**: borrowers only read, through an accessor the owner provides (a locked getter, a
  snapshot copy, or a read-only view). A borrower never writes the state, never keeps a reference past the call
  that lent it, and never hands it on to a third party.
- **Borrowers ask, they don't write**: when a borrower needs a change, it asks the owner (method call, channel
  message, or submitted task) and the owner applies it.
- **Document it on the field**: write down the owner, the borrowers, and the lock or mechanism that protects it.
  If you cannot name a single owner, the design is not ready.
- Prefer guarding it with an existing lock of the owning struct over adding a new lock; never acquire locks in an
  order that differs from existing code.
- Use `atomic` types only for independent single values; do not combine several atomics to represent one invariant.
- Cover the concurrent access in tests and run them with `-race`.
- Call out the new shared state in the PR description: its owner, its borrowers, and why it could not be avoided.

## Proto Files

Protocol buffer definitions are in `common/proto/`. After modifying `.proto` files:
```bash
make proto
```

Requires: `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-go-vtproto`

## Key Dependencies

- **CockroachDB Pebble**: LSM-tree key-value store for data persistence
- **gRPC + vtprotobuf**: Optimized RPC communication
- **Cobra/Viper**: CLI framework and configuration
- **OpenTelemetry**: Metrics and tracing
