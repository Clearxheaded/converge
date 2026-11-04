# Converge Architecture & Design

Converge is a lightweight, local-first distributed synchronization engine built in Go. It enables independent devices to read and modify data offline and converge to identical state whenever network connectivity is established.

This document details the architectural design, mathematical foundations, storage strategies, replication protocols, trade-offs, and system invariants.

---

## 1. System Overview

Traditional cloud-centric architectures treat remote servers as the authoritative source of truth. If a client device loses network connectivity, operations fail or block.

Converge follows the **local-first** paradigm:
- **Local Autonomy:** All read and write operations execute immediately against project-local SQLite storage without network round trips.
- **Strong Eventual Consistency (SEC):** Replicas that have received the same set of updates reach identical state, regardless of message delivery order, network latency, or transient partitions.
- **Conflict-Free Merging:** Concurrent conflicting updates across partitioned nodes resolve deterministically without user intervention or centralized coordinators.

```
+-------------------------------------------------------------------+
|                        Converge Replica                           |
|                                                                   |
|  +--------------------+         +------------------------------+  |
|  |  Client Application |         |   WebSocket Network Layer    |  |
|  +---------+----------+         +--------------+---------------+  |
|            |                                   |                  |
|            v                                   v                  |
|  +--------------------+         +------------------------------+  |
|  | Hybrid Logical     |         | SyncManager                  |  |
|  | Clock (HLC)        |         |  - Watermark tracking        |  |
|  +---------+----------+         |  - Transitive gossip relay   |  |
|            |                    |  - Dedicated read/write pump |  |
|            v                    +--------------+---------------+  |
|  +---------------------------------------------+               |  |
|  | Storage Engine (SQLite WAL)                 |               |  |
|  |  - In-memory LWW CRDT Map (sub-microsecond) |               |  |
|  |  - Atomic multi-statement write transactions|               |  |
|  |  - Lamport monotonicity crash recovery      |               |  |
|  +---------------------------------------------+               |  |
+-------------------------------------------------------------------+
```

---

## 2. Hybrid Logical Clocks (HLC)

### The Problem with Physical Clocks

Distributed systems cannot rely purely on physical wall clocks (such as `time.Now()` or NTP):
1. **Clock Skew and Drift:** Quartz oscillators oscillate at slightly different rates due to temperature and hardware variations.
2. **NTP Step Adjustments:** NTP daemons can adjust the clock backwards, violating temporal monotonicity.
3. **Causality Violations:** If Node A writes at $t=100$ and sends a message to Node B, Node B's clock might read $t=95$. An event caused by A would appear to have happened in the past.

### Pure Lamport Clocks vs HLC

Lamport clocks assign an incrementing integer counter $c$ to every event. If event $A \to B$ (A causes B), then $c(A) < c(B)$. However, Lamport clocks:
- Bear no correlation to physical human time.
- Cannot be used to answer queries like "give me records modified in the last 10 minutes".

### The HLC Algorithm

Converge implements Kulkarni et al.'s Hybrid Logical Clock (`internal/hlc/hlc.go`). An HLC timestamp consists of:
- `WallTime` ($l$): Physical time in milliseconds since Unix epoch.
- `Counter` ($c$): A 32-bit logical counter breaking ties for events occurring during the same physical millisecond.

#### Local Event / Send Rule
When a replica generates a local mutation:
$$\text{phys} = \text{PhysicalClock.Now()}$$
$$l' = \max(l, \text{phys})$$
$$c' = \begin{cases} c + 1 & \text{if } l' = l \\ 0 & \text{if } l' > l \end{cases}$$

#### Remote Receive Rule
When receiving a message with remote timestamp $(l_{\text{remote}}, c_{\text{remote}})$:
$$\text{phys} = \text{PhysicalClock.Now()}$$
$$l' = \max(l, l_{\text{remote}}, \text{phys})$$
$$c' = \begin{cases} 
\max(c, c_{\text{remote}}) + 1 & \text{if } l' = l = l_{\text{remote}} \\
c + 1 & \text{if } l' = l \\
c_{\text{remote}} + 1 & \text{if } l' = l_{\text{remote}} \\
0 & \text{otherwise}
\end{cases}$$

### Crash Recovery Monotonicity Guarantee

If a node crashes and restarts with a physical clock that jumped backwards, an HLC clock could theoretically issue timestamps smaller than those generated before the crash.

Converge eliminates this vulnerability during startup:
```go
maxClockTS := max(persistedClockState, maxRecordTimestampInDB)
clock.Seed(maxClockTS)
```
This guarantees that post-restart timestamps are strictly greater than all pre-crash timestamps.

---

## 3. Conflict-Free Replicated Data Types (CRDT)

Converge models its key-value state as an **LWW-Element-Set** register map (`internal/crdt/lww_map.go`).

### State-Based CRDT Semi-Lattice

Mathematically, Converge forms a join semi-lattice $(\mathcal{S}, \sqcup)$ equipped with a partial order $\le$ and a join operator $\sqcup$:

1. **Commutativity:**
   $$A \sqcup B = B \sqcup A$$
   The order in which two updates arrive does not affect the merged result.

2. **Associativity:**
   $$(A \sqcup B) \sqcup C = A \sqcup (B \sqcup C)$$
   Grouping of update batches across network hops does not alter the outcome.

3. **Idempotence:**
   $$A \sqcup A = A$$
   Duplicate delivery of messages has no effect.

### Strict Total Order Over Records

To guarantee deterministic conflict resolution when concurrent updates occur, `CompareRecords` establishes a total order:

1. **Timestamp Comparison:** Higher `WallTime` wins. If tied, higher `Counter` wins.
2. **Replica ID Tie-Breaking:** If HLC timestamps tie exactly, lexicographically larger `ReplicaID` wins.
3. **Deletion Flag:** If timestamps and replica IDs tie, `Deleted = true` (tombstone) wins over `Deleted = false`.
4. **Value Tie-Breaking:** Lexicographical string comparison of `Value`.
5. **Key Tie-Breaking:** Lexicographical string comparison of `Key`.

```
Winner Selection:
  TS_A > TS_B                  => A wins
  TS_A == TS_B && ID_A > ID_B  => A wins
  Identical ID && Del_A        => A (tombstone) wins
```

### Tombstone Deletions

In distributed environments, deleting a record cannot simply delete the database row. If Node A deletes a key while offline and Node B modifies the key, simply dropping the row would cause Node B to resurrect the deleted record upon sync.

Converge creates a **durable tombstone** (`Deleted = true`) with a fresh HLC timestamp. The tombstone propagates through replication and supersedes any update with a lower timestamp.

---

## 4. SQLite Storage & Concurrency Architecture

Durable local state is persisted using SQLite with Write-Ahead Logging (`internal/storage/sqlite.go`).

### Schema

```sql
CREATE TABLE IF NOT EXISTS records (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    wall_time INTEGER NOT NULL,
    counter INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS clock_state (
    replica_id TEXT PRIMARY KEY,
    wall_time INTEGER NOT NULL,
    counter INTEGER NOT NULL
);
```

### Performance & Concurrency Strategy

SQLite allows concurrent readers but serializes writes. Converge employs a dual-tier design:
- **In-Memory Cache:** All active records are cached in an in-memory `crdt.LWWMap` protected by a `sync.RWMutex`.
- **Read Latency:** Lookups (`Get`, `Has`, `Keys`, `Values`) execute against memory in under 60 nanoseconds without taking SQLite locks.
- **Write Serialization:** An in-process `writeMu` serializes database write transactions, completely eliminating `SQLITE_BUSY` contention.
- **Atomic Commits:** Every mutation persists the record and the updated HLC clock state in the same SQLite transaction before updating memory.

---

## 5. Replication Protocol & WebSocket Transport

Replication coordinates across WebSockets using typed JSON frames (`internal/protocol/` and `internal/sync/`).

### Protocol Frames

1. **`Handshake`:** Exchanged immediately on connection. Transmits local replica ID and validates protocol compatibility. Rejects self-connections.
2. **`SyncRequest`:** Sent during initial connection or periodic catch-up. Contains the local watermark (highest observed HLC timestamp for that peer).
3. **`SyncResponse`:** Contains all records modified since the requested watermark (`RecordsSince(watermark)`).
4. **`UpdatePush`:** Real-time push notification streaming newly committed local mutations.
5. **`ErrorMessage`:** Diagnostic feedback for protocol errors.

### Transitive Multi-Hop Gossip

Converge replicas form arbitrary peer-to-peer topologies (line, star, mesh):
- When Node B receives a winning record from Node A, Node B commits it to SQLite and invokes `BroadcastExcept(nodeA, record)`.
- Node B relays the update to Node C.
- Combined with CRDT idempotence and HLC ordering, cycles terminate naturally without infinite echo loops.

```
+--------+   Write   +--------+   Relay   +--------+
| Node A | --------> | Node B | --------> | Node C |
+--------+           +--------+           +--------+
    ^                                         |
    |------------- (Cycle Terminated) --------+
```

---

## 6. Trade-offs and Limitations

| Decision | Advantage | Trade-off / Limitation |
| :--- | :--- | :--- |
| **LWW Register Map** | Minimal memory overhead, simple deterministic conflict resolution. | Drops concurrent edits on the same key. Not intended for collaborative text editing. |
| **Tombstones** | Prevents deleted records from resurrecting across partitions. | Tombstones accumulate in SQLite. Compaction requires a stable global vector clock watermark. |
| **Hybrid Logical Clock** | Captures causality while maintaining wall-clock proximity. | Extreme forward clock drift from a rogue machine can artificially inflate timestamps. |
| **JSON Protocol Encoding** | Human-readable, easy debugging and cross-language support. | Larger payload size compared to Protobuf or CBOR. |
| **WebSocket Transport** | Bidirectional streaming, widespread browser/firewall compatibility. | Requires persistent TCP connections. |

---

## 7. System Invariants

The following invariants are strictly tested and maintained:

| Invariant | Description | Enforcing Component |
| :--- | :--- | :--- |
| **Clock Monotonicity** | Timestamps allocated by a node are strictly monotonic ($ts_{i+1} > ts_i$). | `internal/hlc` |
| **Restart Monotonicity** | Post-restart clock is strictly greater than any persisted record or clock state. | `internal/storage` |
| **CRDT LWW Determinism** | For any two records with distinct values, `CompareRecords` produces identical results on all replicas. | `internal/crdt` |
| **Tombstone Precedence** | A deletion tombstone with timestamp $t$ defeats any insertion with timestamp $t' \le t$. | `internal/crdt` |
| **Transactional Atomicity** | A record is never committed to SQLite without its corresponding HLC clock state. | `internal/storage` |
| **Anti-Resurrection** | A remote write with an older timestamp cannot overwrite a newer local tombstone. | `internal/storage` |
| **Echo Loop Termination** | Nodes do not re-broadcast records back to the peer that sent them. | `internal/sync` |
| **Convergence** | After all network partitions heal and messages are delivered, all nodes reach bit-for-bit identical state. | `internal/simulation` |

