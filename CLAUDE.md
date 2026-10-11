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
