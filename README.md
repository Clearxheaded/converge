# Converge

[![CI](https://github.com/Clearxheaded/converge/actions/workflows/ci.yml/badge.svg)](https://github.com/Clearxheaded/converge/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.22%2B-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A lightweight, local-first distributed synchronization engine built in Go. Converge enables independent devices to modify data completely offline and converge to bit-for-bit identical state whenever connectivity is restored.

---

## Highlights

- **Offline-First Local Writes:** Immediate local commits to SQLite WAL with zero network latency.
- **Strong Eventual Consistency (SEC):** Mathematically proven join semi-lattice CRDT ensures convergence regardless of packet arrival order.
- **Hybrid Logical Clocks (HLC):** Causally ordered timestamps combining physical wall time with logical Lamport counters.
- **Crash Recovery Monotonicity:** HLC clock state is seeded from persisted SQLite records on restart, preventing clock inversion even across backward physical jumps.
- **Incremental Watermark Synchronization:** Replicas track per-peer watermarks to exchange only newly modified deltas during reconnects.
- **Transitive Multi-Hop Gossip:** Winning records propagate across line, star, and mesh topologies with automatic cycle termination.
- **Durable Tombstone Semantics:** Deletions persist as tombstones to prevent resurrection of stale records across network partitions.
- **High Performance:** Sub-microsecond in-memory reads and 30,000+ local transactional writes/sec on commodity hardware.

---

## Architecture at a Glance

Converge separates concerns across focused packages:

```
converge/
├── cmd/converge/          CLI executable (interactive demo and standalone node REPL)
├── internal/
│   ├── crdt/              LWW-Element-Set CRDT map and total order comparator
│   ├── hlc/               Hybrid Logical Clock with monotonic tie-breaking
│   ├── storage/           SQLite WAL storage engine and crash recovery
│   ├── protocol/          Typed replication frames and validation envelopes
│   ├── sync/              WebSocket peer transport, watermark sync, and gossip
│   └── simulation/        Network proxy and partition fault-injection harness
└── docs/
    └── ARCHITECTURE.md    Deep dive on algorithms, invariants, and trade-offs
```

For full architectural details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Quick Start

### Prerequisites

- Go 1.22 or higher
- C toolchain (GCC / Clang) for SQLite CGO support

### 1. Run the Interactive Multi-Node Demo

The demo runs an automated 3-node cluster with simulated split-brain partitions, concurrent writes, and automatic convergence:

```bash
# Automated run with pacing
go run ./cmd/converge demo --auto

# Interactive step-by-step walkthrough
go run ./cmd/converge demo
```

### 2. Run Standalone Replicas Across Terminals

You can spin up independent live nodes that replicate across WebSockets in real time:

**Terminal 1 (Node A on port 8001):**
```bash
go run ./cmd/converge node --id node-a --listen :8001
```

**Terminal 2 (Node B on port 8002, peering with Node A):**
```bash
go run ./cmd/converge node --id node-b --listen :8002 --peers ws://localhost:8001
```

**Interactive REPL Commands:**
```text
> set user:101 {"name":"Alice","role":"admin"}
OK (HLC: 1793661642561:0)

> list
  user:101             = {"name":"Alice","role":"admin"}

> dump
  Key: user:101        | Val: {"name":"Alice"...} | HLC: 1793661642561:0 | Replica: node-a | ACTIVE

> delete user:101
OK (Tombstoned at HLC: 1793661642570:0, wasActive: true)

> peers
  - node-a       (Watermark: 1793661642570:0, Inbound: true)
```

---

## Benchmarks

Measured on an Intel Core Ultra 9 275HX processor:

### CRDT In-Memory Operations (`internal/crdt`)

| Operation | Latency | Throughput | Allocations |
| :--- | :--- | :--- | :--- |
| `Record Comparison` | 1.95 ns/op | ~512,000,000 ops/sec | 0 B/op, 0 allocs |
| `In-Memory Get` | 50.25 ns/op | ~19,900,000 ops/sec | 13 B/op, 1 alloc |
| `In-Memory Set` | 67.12 ns/op | ~14,900,000 ops/sec | 13 B/op, 1 alloc |
| `Merge 100 Disjoint Records` | 6.97 μs/op | ~14,300,000 records/sec | 23.9 KB/op |
| `Merge 1,000 Disjoint Records` | 88.13 μs/op | ~11,300,000 records/sec | 392.7 KB/op |
| `Merge 1,000 Conflicting Records` | 30.60 μs/op | ~32,600,000 records/sec | 0 B/op, 0 allocs |
| `Merge 1,000 Tombstone Records` | 30.11 μs/op | ~33,200,000 records/sec | 0 B/op, 0 allocs |

### SQLite Storage Engine (`internal/storage`)

| Operation | Latency | Details |
| :--- | :--- | :--- |
| `Single Set (WAL Transaction)` | 33.82 μs/op | Commits record + HLC clock state to SQLite disk (~29,500 tx/sec) |
| `In-Memory Get` | 50.53 ns/op | Lock-free query without SQLite disk contention |
| `Merge 100 Batch Records` | 167.57 μs/op | Single atomic SQLite transaction (~596,000 records/sec) |
| `Merge 1,000 Batch Records` | 2.48 ms/op | Single atomic SQLite transaction (~402,000 records/sec) |
| `Concurrent Set (8 Workers)` | 35.84 μs/op | Parallel mutations serialized via in-process write mutex |
| `RecordsSince (Watermark Query)` | 103.73 μs/op | Querying delta modifications across 1,000 SQLite records |

### WebSocket Replication (`internal/sync`)

| Operation | Latency | Details |
| :--- | :--- | :--- |
| `Streaming Single Write` | 1.52 ms/op | Local write + WebSocket frame + remote commit to SQLite WAL |
| `Catch-Up Sync (500 records)` | 5.68 ms/op | Handshake, delta query, transmission, and SQLite batch commit |

Run the benchmarks yourself:
```bash
go test -run='^$' -bench='.' ./...
```

---

## Verification & Testing

Converge includes comprehensive unit, property, concurrency, and simulation tests.

```bash
# Run all unit and integration tests
go test ./...

# Run with Go race detector enabled
go test -race ./...

# Run static analysis
go vet ./...
```

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
