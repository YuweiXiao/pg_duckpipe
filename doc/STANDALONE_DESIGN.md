# pg_duckpipe Standalone Engine: Design Document

## Rust-based CDC Engine for PostgreSQL → DuckLake Synchronization

### Date: 2026-02-09

---

## 1. Motivation

The current pg_duckpipe runs as a PostgreSQL background worker (BGWorker). While this works, it has fundamental limitations:

| Problem | Impact |
|---------|--------|
| **Crash coupling** | A bug in the worker can crash the entire PostgreSQL instance |
| **Single-threaded** | One worker per database, cannot parallelize across tables/groups |
| **Deployment friction** | Requires `shared_preload_libraries`, PG restart to load/upgrade |
| **Debugging** | No independent logging, hard to attach profilers/debuggers |
| **Scaling** | Cannot scale CDC processing independently of the database |
| **Upgrades** | Extension upgrade requires PG restart or `ALTER EXTENSION UPDATE` |
| **Testing** | Tests require a full PG instance with the extension loaded |
| **Memory** | Shares PostgreSQL's memory space, limited by PG memory contexts |

A standalone engine solves all of these while preserving the same CDC semantics.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Instance                          │
│                                                                  │
│  ┌───────────────┐         ┌──────────────────────────────────┐ │
│  │  Heap Tables   │         │  DuckLake Tables                 │ │
│  │  (OLTP writes) │         │  (OLAP reads via pg_ducklake)   │ │
│  └───────┬───────┘         └──────────────▲───────────────────┘ │
│          │ WAL                             │ libpq INSERT/DELETE │
│          ▼                                 │                     │
│  ┌──────────────────┐                     │                     │
│  │ Replication Slot  │                     │                     │
│  │ (pgoutput)        │                     │                     │
│  └────────┬─────────┘                     │                     │
│           │ Streaming Replication Protocol │                     │
└───────────┼───────────────────────────────┼─────────────────────┘
            │                               │
            │  TCP (replication connection)  │  TCP (normal connection)
            ▼                               │
┌───────────────────────────────────────────┴─────────────────────┐
│                     duckpipe daemon (Rust)                       │
│                                                                  │
│  ┌─────────────┐  ┌──────────┐  ┌─────────┐  ┌──────────────┐ │
│  │  WAL Stream  │→│ Decoder   │→│ Batcher  │→│   Applier     │ │
│  │  Consumer    │  │ (pgoutput │  │ (per-   │  │ (SQL gen +   │ │
│  │              │  │  binary)  │  │  table)  │  │  libpq exec) │ │
│  └─────────────┘  └──────────┘  └─────────┘  └──────────────┘ │
│                                                                  │
│  ┌──────────────┐  ┌────────────┐  ┌────────────────────────┐  │
│  │ State Store   │  │ Checkpoint │  │ API Server             │  │
│  │ (metadata DB) │  │ Manager    │  │ (HTTP/gRPC + metrics)  │  │
│  └──────────────┘  └────────────┘  └────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

**Key change**: Instead of consuming WAL via SPI calling `pg_logical_slot_get_binary_changes()` inside PostgreSQL, the standalone engine connects via the **streaming replication protocol** over TCP and writes back via standard **libpq** connections.

---

## 3. Why Rust

| Factor | Assessment |
|--------|------------|
| **Performance** | Zero-cost abstractions, no GC pauses — matches C for throughput-critical CDC path |
| **Safety** | Ownership model prevents the memory bugs that plague C extensions (use-after-free, double-free, buffer overruns) |
| **Concurrency** | `tokio` async runtime provides efficient multi-group parallelism without the complexity of pthreads |
| **Ecosystem** | `tokio-postgres` has native streaming replication support; `postgres-protocol` crate parses pgoutput messages |
| **Deployment** | Single static binary, no shared library dependencies |
| **Observability** | First-class `tracing` crate, Prometheus metrics via `metrics` crate |
| **Testing** | Built-in test framework, property testing via `proptest`, no PG instance needed for unit tests |

---

## 4. Component Design

### 4.1 WAL Stream Consumer

**Current (BGWorker)**:
```c
// Polling via SPI — consumes slot in batch, returns when empty
SPI_execute_with_args(
    "SELECT lsn, data FROM pg_logical_slot_get_binary_changes($1, NULL, $2, ...)",
    slot_name, batch_size_per_group
);
```

**Standalone (Rust)**:
```
Replication connection → START_REPLICATION SLOT slot LOGICAL lsn
                         (proto_version '1', publication_names 'pub')
→ Continuous stream of CopyData messages containing pgoutput binary
→ Periodic StandbyStatusUpdate sent back to advance confirmed_flush_lsn
```

The streaming protocol is fundamentally different from polling:

| Aspect | Polling (current) | Streaming (proposed) |
|--------|-------------------|---------------------|
| Latency | poll_interval (1s default) | Near-zero (push-based) |
| Connection type | Normal SPI | Replication connection |
| Feedback | Implicit (slot advances on consume) | Explicit StandbyStatusUpdate |
| Backpressure | Batch size limit | TCP flow control + application-level |
| WAL retention | Only during poll gap | Until StandbyStatusUpdate confirms |

**Rust implementation approach**:

```rust
use tokio_postgres::replication::LogicalReplicationStream;

// Establish replication connection
let (client, connection) = tokio_postgres::connect(
    "host=localhost dbname=mydb replication=database", NoTls
).await?;

// Start streaming from last confirmed LSN
let stream = client
    .copy_both_simple::<bytes::Bytes>(&format!(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
        slot_name, start_lsn, publication
    ))
    .await?;

// Process messages
while let Some(event) = stream.next().await {
    match event {
        XLogData { data, wal_start, wal_end, .. } => {
            decode_and_batch(data, wal_start)?;
        }
        PrimaryKeepAlive { reply_requested, .. } => {
            if reply_requested {
                send_standby_status_update(confirmed_lsn).await?;
            }
        }
    }
}
```

**Key consideration**: The streaming protocol pushes data continuously. We need application-level flow control — if the applier falls behind, we must either buffer in memory (bounded) or pause reading from the stream (TCP backpressure propagates to walsender).

### 4.2 pgoutput Decoder

**Current (BGWorker)**: Calls PostgreSQL's internal `logicalrep_read_*` C functions directly — these are server-private symbols not available outside the PostgreSQL process.

**Standalone (Rust)**: Must reimplement the pgoutput binary protocol parser. The protocol is stable and well-documented:

```
Message format: [type: u8] [payload: bytes]

Type 'B' (BEGIN):
  - final_lsn:   u64
  - commit_time:  i64
  - xid:          u32

Type 'R' (RELATION):
  - relation_id:  u32
  - namespace:    string (null-terminated)
  - relname:      string (null-terminated)
  - replica_ident: u8
  - num_columns:  u16
  - columns[]:    { flags: u8, name: string, type_oid: u32, type_modifier: i32 }

Type 'I' (INSERT):
  - relation_id:  u32
  - 'N' marker:   u8
  - tuple_data:   TupleData

Type 'U' (UPDATE):
  - relation_id:  u32
  - ['K'|'O']:    u8 (optional old key/old tuple)
  - old_tuple:    TupleData (if K/O present)
  - 'N' marker:   u8
  - new_tuple:    TupleData

Type 'D' (DELETE):
  - relation_id:  u32
  - ['K'|'O']:    u8
  - old_tuple:    TupleData

Type 'C' (COMMIT):
  - flags:        u8
  - commit_lsn:   u64
  - end_lsn:      u64
  - commit_time:  i64

Type 'T' (TRUNCATE):
  - num_relations: u32
  - options:       u8 (CASCADE, RESTART_IDENTITY)
  - relation_ids:  u32[]

TupleData:
  - num_columns:  u16
  - columns[]:    { status: u8, [length: u32, data: bytes] }
    status: 'n' = NULL, 'u' = unchanged TOAST, 't' = text value
```

This is roughly 400-500 lines of Rust. Existing crates like `pg_replicate` or `postgres-protocol` provide partial implementations that can be referenced or used directly.

**Design**: Define a `Decoder` trait so we can swap implementations or support protocol version 2+ later:

```rust
pub enum DecodedMessage {
    Begin(BeginMessage),
    Relation(RelationMessage),
    Insert(InsertMessage),
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Commit(CommitMessage),
    Truncate(TruncateMessage),
}

pub trait Decoder {
    fn decode(&self, data: &[u8]) -> Result<DecodedMessage>;
}

pub struct PgOutputV1Decoder;

impl Decoder for PgOutputV1Decoder {
    fn decode(&self, data: &[u8]) -> Result<DecodedMessage> {
        let msg_type = data[0];
        let payload = &data[1..];
        match msg_type {
            b'B' => Ok(DecodedMessage::Begin(parse_begin(payload)?)),
            b'R' => Ok(DecodedMessage::Relation(parse_relation(payload)?)),
            b'I' => Ok(DecodedMessage::Insert(parse_insert(payload)?)),
            b'U' => Ok(DecodedMessage::Update(parse_update(payload)?)),
            b'D' => Ok(DecodedMessage::Delete(parse_delete(payload)?)),
            b'C' => Ok(DecodedMessage::Commit(parse_commit(payload)?)),
            b'T' => Ok(DecodedMessage::Truncate(parse_truncate(payload)?)),
            _ => Err(Error::UnknownMessageType(msg_type)),
        }
    }
}
```

### 4.3 Relation Cache

Same concept as the current `RelationCacheEntry` hash table, but in Rust with `HashMap`:

```rust
pub struct RelationCache {
    /// relation_id → schema info from RELATION messages
    relations: HashMap<u32, RelationInfo>,
    /// relation_id → resolved TableMapping (lazy, cached)
    mappings: HashMap<u32, Option<Arc<TableMapping>>>,
}

pub struct RelationInfo {
    pub relation_id: u32,
    pub schema: String,
    pub table: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnInfo>,
}

pub struct ColumnInfo {
    pub flags: u8,  // bit 0 = part of key
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}
```

When a RELATION message arrives, invalidate the cached mapping for that relation_id (same as current `entry->mapping = NULL`). The mapping is re-resolved on the next DML message for that relation.

### 4.4 Batcher

Conceptually identical to current `batch.c`. Accumulate `SyncChange` entries per target table, flush on:

1. Per-table threshold (`batch_size_per_table`)
2. COMMIT message boundary
3. TRUNCATE (flush before executing)

```rust
pub struct Batcher {
    batches: HashMap<String, SyncBatch>,  // key: "schema.table"
    config: BatchConfig,
}

pub struct SyncBatch {
    target_schema: String,
    target_table: String,
    changes: Vec<SyncChange>,
    column_names: Vec<String>,
    key_column_indices: Vec<usize>,
    mapping_id: i32,
}

pub enum SyncChange {
    Insert { values: Vec<Option<String>> },
    Delete { key_values: Vec<Option<String>> },
}
```

**UPDATE decomposition** remains the same: an UPDATE becomes a Delete + Insert at decode time, before entering the batcher.

### 4.5 Applier

Generates and executes SQL via a normal (non-replication) libpq connection. Same strategy as current `apply.c`:

- **INSERTs**: Batched multi-row `INSERT INTO target VALUES (...), (...), ...`
- **DELETEs**: Individual `DELETE FROM target WHERE pk1 = $1 AND pk2 = $2`
- Flush pending INSERTs before each DELETE to maintain ordering

```rust
pub struct Applier {
    client: tokio_postgres::Client,
}

impl Applier {
    pub async fn apply_batch(&self, batch: &SyncBatch) -> Result<u64> {
        let mut pending_inserts: Vec<String> = Vec::new();
        let mut rows_applied: u64 = 0;

        for change in &batch.changes {
            match change {
                SyncChange::Insert { values } => {
                    pending_inserts.push(format_values_tuple(values));
                    if pending_inserts.len() >= INSERT_BATCH_SIZE {
                        rows_applied += self.flush_inserts(batch, &mut pending_inserts).await?;
                    }
                }
                SyncChange::Delete { key_values } => {
                    // Flush pending inserts first (ordering)
                    rows_applied += self.flush_inserts(batch, &mut pending_inserts).await?;
                    rows_applied += self.exec_delete(batch, key_values).await?;
                }
            }
        }
        // Flush remaining inserts
        rows_applied += self.flush_inserts(batch, &mut pending_inserts).await?;
        Ok(rows_applied)
    }
}
```

**Connection pooling**: Use `deadpool-postgres` or `bb8-postgres` for the apply connection pool. When running multiple sync groups in parallel, each group gets its own connection from the pool.

**Transaction semantics**: Each batch apply should run within a single transaction. On commit boundary (COMMIT message from WAL), all batches for all tables in that transaction are applied and committed together. This preserves cross-table consistency within a source transaction.

### 4.6 State Machine

Identical to the current state machine, but tracked in a durable store instead of PostgreSQL tables:

```
add_table(copy_data=true):   SNAPSHOT → CATCHUP → STREAMING
add_table(copy_data=false):  directly STREAMING
resync_table():              reset to SNAPSHOT
```

**CATCHUP logic** is the same: skip WAL changes with `lsn <= snapshot_lsn`.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncState {
    Pending,
    Snapshot,
    Catchup,
    Streaming,
}
```

### 4.7 Snapshot (Initial Copy)

The snapshot process requires careful coordination. **Implemented in `duckpipe-core/src/snapshot.rs`** using SQL functions (not the replication protocol):

```
1. Connection A: BEGIN ISOLATION LEVEL REPEATABLE READ
2. Connection A: SELECT pg_create_logical_replication_slot('duckpipe_snap_N', 'pgoutput', true)
   → consistent_point (this is snapshot_lsn)
3. Connection A: SELECT pg_export_snapshot() → snapshot_name
4. Connection B: BEGIN ISOLATION LEVEL REPEATABLE READ
5. Connection B: SET TRANSACTION SNAPSHOT '<snapshot_name>'
6. Connection B: DELETE FROM target_table + INSERT INTO target SELECT * FROM source
7. Connection B: COMMIT
8. Connection A: COMMIT (temp slot auto-drops when connection closes)
9. Update state → CATCHUP with snapshot_lsn
10. Resume WAL streaming — skip changes with lsn ≤ snapshot_lsn
```

**Why SQL-based?** `tokio-postgres` 0.7 does not support the `replication=database` startup parameter needed for the replication protocol's `CREATE_REPLICATION_SLOT` command. The SQL function `pg_create_logical_replication_slot()` (PG14+) provides the same `consistent_point` LSN. The REPEATABLE READ snapshot taken at the slot creation query is coordinated with the slot's consistent_point.

**Why not `pg_current_wal_lsn()`?** Using `pg_current_wal_lsn()` alone would be unsafe — in-flight transactions may have WAL records before the snapshot LSN but commit after, causing data loss during CATCHUP. The slot creation provides proper coordination.

Each snapshot task uses a unique slot name (`duckpipe_snap_{task_id}`) to allow concurrent snapshots for different tables.

### 4.8 State Store

Replace the current `duckpipe.sync_groups` and `duckpipe.table_mappings` PostgreSQL tables with a durable metadata store. Three options:

**Option A: PostgreSQL tables (recommended for v1)**

Keep metadata in PostgreSQL but managed by the standalone engine via libpq instead of SPI. This preserves compatibility with existing deployments and allows the thin PG extension wrapper (Section 6) to query the same tables.

```sql
-- Same schema as current, managed by the daemon
duckpipe.sync_groups    (id, name, publication, slot_name, enabled, confirmed_lsn, ...)
duckpipe.table_mappings (id, group_id, source_schema, source_table, target_schema, ...)
```

**Option B: Embedded SQLite**

Fully self-contained, no PG dependency for metadata. But loses the ability to query state from SQL.

**Option C: YAML/JSON config file + WAL position file**

Simplest, but no transactional guarantees.

**Recommendation**: Option A for the initial version. The metadata tables are tiny (dozens of rows) and keeping them in PG makes the PG extension wrapper (Section 6) trivial. Can migrate to SQLite later if needed.

### 4.9 Checkpoint Manager

Replaces the current `UPDATE sync_groups SET confirmed_lsn = ...` + implicit slot advancement.

In the standalone engine, checkpoint has two parts:

1. **Persist confirmed LSN** to the state store (so we know where to resume after restart)
2. **Send StandbyStatusUpdate** to PostgreSQL via the replication connection (so PG can reclaim WAL)

```rust
pub struct CheckpointManager {
    state_store: Arc<StateStore>,
    standby_status_interval: Duration,  // e.g., 10 seconds
}

impl CheckpointManager {
    pub async fn checkpoint(
        &self,
        group: &SyncGroup,
        confirmed_lsn: Lsn,
        stream: &mut ReplicationStream,
    ) -> Result<()> {
        // 1. Persist to state store
        self.state_store.update_confirmed_lsn(group.id, confirmed_lsn).await?;

        // 2. Send feedback to PostgreSQL
        stream.standby_status_update(
            confirmed_lsn,  // write_lsn
            confirmed_lsn,  // flush_lsn
            confirmed_lsn,  // apply_lsn
            SystemTime::now(),
            false,           // reply_requested
        ).await?;

        Ok(())
    }
}
```

**Checkpoint frequency**: After each COMMIT boundary that was successfully applied, or periodically (whichever comes first). This matches the current behavior but is more explicit.

---

## 5. Concurrency Model

The current BGWorker is single-threaded: it processes sync groups sequentially in a loop. The standalone engine can parallelize at multiple levels:

### 5.1 Per-Group Parallelism

Each sync group has its own replication slot and publication. Groups are fully independent and can run as separate tokio tasks:

```
┌─────────────────────────────────────────────┐
│              duckpipe daemon                 │
│                                              │
│  ┌────────────────────┐                     │
│  │ Group "default"     │  ← tokio task      │
│  │ slot_1 → decode →  │                     │
│  │ batch → apply       │                     │
│  └────────────────────┘                     │
│                                              │
│  ┌────────────────────┐                     │
│  │ Group "analytics"   │  ← tokio task      │
│  │ slot_2 → decode →  │                     │
│  │ batch → apply       │                     │
│  └────────────────────┘                     │
│                                              │
│  ┌────────────────────┐                     │
│  │ API + Metrics       │  ← tokio task      │
│  └────────────────────┘                     │
└─────────────────────────────────────────────┘
```

This is a direct improvement over the current sequential processing. A slow group no longer blocks other groups.

### 5.2 Within-Group Parallelism (Future)

Within a single group, changes must be applied in WAL order for correctness. However, changes to *different tables* within the same transaction are independent. A future optimization could fan out apply operations per table within a commit boundary:

```
COMMIT boundary received
  ├─ apply batch for table_a  (task 1)
  ├─ apply batch for table_b  (task 2)
  └─ apply batch for table_c  (task 3)
  → await all → send checkpoint
```

Not needed for v1, but the architecture should not preclude it.

### 5.3 Snapshot Parallelism

Snapshots for different tables can run in parallel. Currently they're sequential. With the standalone engine, each snapshot can be a separate tokio task using its own PG connection.

---

## 6. Thin PostgreSQL Extension (Optional)

To preserve the ergonomic SQL API (`SELECT duckpipe.add_table('public.orders')`), a thin PG extension can act as a proxy:

```
┌────────────────────────────────────────────┐
│ PostgreSQL                                  │
│                                             │
│  SELECT duckpipe.add_table('orders');       │
│          │                                  │
│          ▼                                  │
│  ┌──────────────────────────────┐          │
│  │ pg_duckpipe_proxy extension  │          │
│  │  - Reads/writes metadata     │          │
│  │    tables directly           │          │
│  │  - Sends HTTP to daemon for  │          │
│  │    operations requiring it   │          │
│  │    (start group, etc.)       │          │
│  └──────────────────────────────┘          │
└────────────────────────────────────────────┘
```

The thin extension would:
- **Metadata operations** (add_table, remove_table, etc.): Write directly to `duckpipe.sync_groups` / `duckpipe.table_mappings` tables + create publications + create replication slots. This is the same logic as current `api.c` but the daemon picks up changes on next poll.
- **Daemon control** (start, stop, status): HTTP call to the daemon's API endpoint.

Alternatively, skip the extension entirely and provide a CLI tool:

```bash
duckpipe add-table --source public.orders --target public.orders_ducklake
duckpipe status
duckpipe groups
```

Both approaches can coexist.

---

## 7. API Design

### 7.1 HTTP REST API

```
POST   /api/v1/groups                    Create sync group
DELETE /api/v1/groups/:name              Drop sync group
PUT    /api/v1/groups/:name/enable       Enable group
PUT    /api/v1/groups/:name/disable      Disable group

POST   /api/v1/tables                    Add table to sync
DELETE /api/v1/tables/:source_table      Remove table from sync
PUT    /api/v1/tables/:source_table/resync   Resync table
PUT    /api/v1/tables/:source_table/move     Move to different group

GET    /api/v1/groups                    List groups (like duckpipe.groups())
GET    /api/v1/tables                    List tables (like duckpipe.tables())
GET    /api/v1/status                    Detailed status (like duckpipe.status())

GET    /metrics                          Prometheus metrics endpoint
GET    /health                           Health check
```

### 7.2 CLI Tool

```bash
duckpipe --config /etc/duckpipe/config.toml daemon     # Run the daemon
duckpipe --config /etc/duckpipe/config.toml add-table public.orders
duckpipe --config /etc/duckpipe/config.toml status
duckpipe --config /etc/duckpipe/config.toml groups
duckpipe --config /etc/duckpipe/config.toml tables
```

### 7.3 Metrics (Prometheus)

```
# WAL lag
duckpipe_wal_lag_bytes{group="default"}                    # pg_current_wal_lsn() - confirmed_lsn
duckpipe_wal_lag_seconds{group="default"}                  # time since last successful apply

# Throughput
duckpipe_rows_applied_total{group="default", table="orders_ducklake"}
duckpipe_batches_applied_total{group="default"}
duckpipe_transactions_applied_total{group="default"}

# Latency
duckpipe_apply_duration_seconds{group="default", quantile="0.99"}
duckpipe_decode_duration_seconds{group="default", quantile="0.99"}

# State
duckpipe_table_state{group="default", table="orders", state="streaming"}  # gauge: 1 if in this state
duckpipe_group_enabled{group="default"}

# Snapshot
duckpipe_snapshot_rows_copied{table="orders"}
duckpipe_snapshot_duration_seconds{table="orders"}

# Errors
duckpipe_decode_errors_total{group="default"}
duckpipe_apply_errors_total{group="default"}
```

---

## 8. Configuration

```toml
# /etc/duckpipe/config.toml

[source]
# PostgreSQL connection for replication (needs REPLICATION privilege)
host = "localhost"
port = 5432
dbname = "mydb"
user = "replicator"
password_file = "/etc/duckpipe/pgpass"   # or use PGPASSWORD env

[target]
# PostgreSQL connection for applying changes (needs write access to DuckLake tables)
# Can be the same PG instance or different
host = "localhost"
port = 5432
dbname = "mydb"
user = "duckpipe_writer"
password_file = "/etc/duckpipe/pgpass"

[engine]
# Batch tuning (same semantics as current GUC parameters)
batch_size_per_table = 1000
batch_size_per_group = 10000

# Checkpoint interval — how often to send StandbyStatusUpdate
checkpoint_interval_ms = 5000

# Connection pool size for apply connections
apply_pool_size = 4

[api]
listen_addr = "127.0.0.1:8432"
metrics_addr = "127.0.0.1:9432"

[logging]
level = "info"            # trace, debug, info, warn, error
format = "json"           # json or text
```

**Key difference from current GUCs**: No `poll_interval` — the streaming protocol is push-based. The `checkpoint_interval_ms` replaces it as the primary timing knob.

---

## 9. Error Handling and Recovery

### 9.1 Replication Connection Failure

```
WAL stream disconnected
  → Log error with context
  → Exponential backoff retry (1s, 2s, 4s, 8s, ... max 60s)
  → Resume from last confirmed LSN (persisted in state store)
  → No data loss — replication slot retains WAL until confirmed
```

### 9.2 Apply Connection Failure

```
Apply SQL failed
  → Rollback current transaction
  → Retry entire batch (idempotent: DELETE WHERE pk=... is safe to retry)
  → If persistent failure, pause the group and emit alert metric
  → Log the failed SQL and error for debugging
```

### 9.3 Poison Message

A WAL change that consistently fails to apply (e.g., type mismatch after DDL):

```
Apply fails 3 times for same batch
  → Move group to ERROR state
  → Log the problematic change with full context
  → Emit duckpipe_apply_errors_total metric
  → Skip and continue (configurable: skip vs. halt)
  → Operator can resync_table() to recover
```

### 9.4 Graceful Shutdown

```
SIGTERM received
  → Stop accepting new WAL data
  → Flush all pending batches
  → Checkpoint confirmed LSN
  → Send final StandbyStatusUpdate
  → Close connections
  → Exit 0
```

### 9.5 Crash Recovery

```
Daemon restarts after crash
  → Read last confirmed LSN from state store
  → Reconnect replication stream from confirmed LSN
  → PostgreSQL replays WAL from that point
  → Some changes may be re-applied (DELETE WHERE pk=... is idempotent)
  → INSERTs could cause duplicates if crash between apply and checkpoint
    → Mitigation: apply + checkpoint in same PG transaction on the target side
```

---

## 10. Project Structure

```
duckpipe/
├── Cargo.toml
├── Cargo.lock
├── config.example.toml
├── src/
│   ├── main.rs                  # CLI entry point, arg parsing, daemon startup
│   ├── config.rs                # Configuration loading and validation
│   ├── engine/
│   │   ├── mod.rs
│   │   ├── group_worker.rs      # Per-group async task: stream → decode → batch → apply
│   │   ├── snapshot.rs          # Initial data copy (COPY TO → INSERT INTO)
│   │   └── state_machine.rs     # SyncState transitions, CATCHUP skip logic
│   ├── replication/
│   │   ├── mod.rs
│   │   ├── stream.rs            # Replication connection, START_REPLICATION, keepalive
│   │   └── decoder.rs           # pgoutput binary protocol parser
│   ├── batch/
│   │   ├── mod.rs
│   │   └── batcher.rs           # Per-table change accumulation, flush triggers
│   ├── apply/
│   │   ├── mod.rs
│   │   ├── applier.rs           # SQL generation (INSERT/DELETE), execution via libpq
│   │   └── sql_gen.rs           # SQL string building, identifier/value quoting
│   ├── state/
│   │   ├── mod.rs
│   │   ├── store.rs             # Trait for state persistence
│   │   ├── pg_store.rs          # PostgreSQL-backed state store (sync_groups, table_mappings)
│   │   └── checkpoint.rs        # LSN checkpointing + StandbyStatusUpdate
│   ├── api/
│   │   ├── mod.rs
│   │   ├── server.rs            # HTTP server (axum)
│   │   ├── routes.rs            # REST endpoint handlers
│   │   └── metrics.rs           # Prometheus metrics registration and export
│   └── types.rs                 # Shared types: Lsn, SyncState, SyncGroup, TableMapping, etc.
├── tests/
│   ├── integration/
│   │   ├── mod.rs
│   │   ├── streaming_test.rs    # INSERT/UPDATE/DELETE CDC end-to-end
│   │   ├── snapshot_test.rs     # Initial copy with concurrent writes
│   │   ├── decoder_test.rs      # pgoutput binary parsing unit tests
│   │   ├── batcher_test.rs      # Batch accumulation and flush logic
│   │   └── applier_test.rs      # SQL generation correctness
│   └── fixtures/
│       └── pgoutput_messages/   # Captured binary messages for decoder tests
└── doc/
    └── STANDALONE_DESIGN.md     # This document
```

---

## 11. Rust Crate Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `tokio-postgres` | PostgreSQL client with replication protocol support |
| `deadpool-postgres` | Connection pooling for apply connections |
| `postgres-protocol` | Low-level PG protocol types (useful for pgoutput parsing) |
| `bytes` | Efficient byte buffer handling for binary protocol parsing |
| `axum` | HTTP API server |
| `serde` / `serde_json` | Config and API serialization |
| `toml` | Config file parsing |
| `tracing` / `tracing-subscriber` | Structured logging |
| `metrics` / `metrics-exporter-prometheus` | Prometheus metrics |
| `clap` | CLI argument parsing |
| `thiserror` / `anyhow` | Error handling |

---

## 12. Migration Path

### Phase 1: Core Engine (MVP)

Build the standalone streaming consumer + decoder + batcher + applier. Target feature parity with the current BGWorker for the basic CDC path:

- [ ] pgoutput binary decoder (all 7 message types)
- [ ] Streaming replication consumer
- [ ] Per-table batching with flush on COMMIT
- [ ] SQL apply via libpq (INSERT batching, individual DELETEs)
- [ ] State store (PostgreSQL-backed, reuse existing tables)
- [ ] Checkpoint manager (StandbyStatusUpdate)
- [ ] State machine (SNAPSHOT → CATCHUP → STREAMING)
- [ ] Snapshot via COPY + INSERT
- [ ] TOML configuration
- [ ] Structured logging

### Phase 2: Operations

- [ ] HTTP API for management and monitoring
- [ ] Prometheus metrics
- [ ] CLI tool (`duckpipe add-table`, `duckpipe status`, etc.)
- [ ] Per-group parallelism (multiple tokio tasks)
- [ ] Graceful shutdown with flush
- [ ] Health checks

### Phase 3: Thin PG Extension (Optional)

- [ ] Minimal C extension providing SQL API that proxies to daemon
- [ ] `duckpipe.add_table()` → writes metadata + HTTP call to daemon
- [ ] `duckpipe.status()` → reads from shared metadata tables

### Phase 4: Advanced Features

- [ ] Within-group parallel apply (per-table fan-out)
- [ ] Snapshot parallelism
- [ ] Multiple source database support (one daemon, many PG sources)
- [ ] Row filtering (`WHERE` clause on publications, PG 15+)
- [ ] Column filtering (column lists on publications, PG 15+)
- [ ] Schema change detection and handling
- [ ] Direct DuckDB/Parquet writes (bypass PG for target)

---

## 13. Testing Strategy

### 13.1 Unit Tests

- **Decoder**: Feed captured pgoutput binary blobs, verify parsed output. No PG required.
- **Batcher**: Verify flush triggers, ordering, change accumulation. No PG required.
- **SQL generator**: Verify generated SQL for edge cases (NULLs, special characters, multi-column PKs). No PG required.
- **State machine**: Verify state transitions and CATCHUP skip logic. No PG required.

### 13.2 Integration Tests

- **End-to-end CDC**: Start PG in Docker, create table, start daemon, INSERT/UPDATE/DELETE, verify target table. Same coverage as current regression tests.
- **Snapshot + concurrent writes**: Same as `snapshot_updates.sql` test.
- **Multiple tables**: Same as `multiple_tables.sql` test.
- **TRUNCATE**: Same as `truncate.sql` test.
- **Crash recovery**: Kill daemon mid-apply, restart, verify no data loss/duplication.

### 13.3 Test Infrastructure

Use `testcontainers-rs` to manage PostgreSQL instances in Docker:

```rust
#[tokio::test]
async fn test_insert_cdc() {
    let pg = PostgresContainer::new("postgres:16")
        .with_init_sql("CREATE EXTENSION pg_duckdb")
        .start()
        .await;

    // Create source table, add to publication, create slot
    // Start duckpipe engine pointing at container
    // INSERT INTO source, wait, SELECT FROM target
    // Assert equality
}
```

---

## 14. Deployment

### 14.1 Systemd Service

```ini
[Unit]
Description=duckpipe CDC engine
After=postgresql.service

[Service]
Type=simple
ExecStart=/usr/local/bin/duckpipe daemon --config /etc/duckpipe/config.toml
Restart=always
RestartSec=5
User=duckpipe

[Install]
WantedBy=multi-user.target
```

### 14.2 Docker

```dockerfile
FROM rust:1.83 AS builder
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /target/release/duckpipe /usr/local/bin/
ENTRYPOINT ["duckpipe", "daemon"]
```

### 14.3 PostgreSQL Setup Requirements

The PostgreSQL instance needs:

```sql
-- postgresql.conf (same as current)
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- pg_duckdb must be installed (for DuckLake target tables)
-- shared_preload_libraries = 'pg_duckdb'
-- NOTE: pg_duckpipe NO LONGER needs to be in shared_preload_libraries

-- Create replication user
CREATE ROLE duckpipe_replicator WITH REPLICATION LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE mydb TO duckpipe_replicator;

-- Grant SELECT on source tables (for snapshot COPY)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO duckpipe_replicator;

-- Create apply user (or reuse same user with additional grants)
GRANT INSERT, DELETE ON ALL TABLES IN SCHEMA public TO duckpipe_writer;
```

---

## 15. Comparison: BGWorker vs. Standalone

| Aspect | BGWorker (current) | Standalone (proposed) |
|--------|-------------------|----------------------|
| **Crash isolation** | Shares PG process | Separate process |
| **Deployment** | `shared_preload_libraries` + restart | Single binary, systemd/Docker |
| **Upgrades** | PG restart or ALTER EXTENSION | Restart daemon only |
| **Parallelism** | Single-threaded, sequential groups | Async, parallel groups |
| **WAL consumption** | Polling (1s latency) | Streaming (sub-second) |
| **Memory** | PG memory contexts | Independent heap, no PG limits |
| **Observability** | `elog()` to PG log | Structured logging + Prometheus |
| **Debugging** | Attach to PG process | Standard Rust tooling |
| **Testing** | Needs full PG + extension | Unit tests without PG |
| **Multi-source** | One PG only | Could support multiple PGs |
| **SQL API** | Native SQL functions | HTTP API + optional thin extension |
| **Setup complexity** | `CREATE EXTENSION` | Config file + service management |

---

## 16. Decision: Shared Core Library (Revised)

> **Update:** The original decision was "No Shared Core Library" (Approach C: separate codebases). This was revised when the C extension was rewritten in Rust using pgrx. The v2 implementation uses **Approach A** — a shared `duckpipe-core` Rust library crate used by both the PG bgworker (`duckpipe-pg`) and standalone daemon (`duckpipe-daemon`). See DESIGN_V2.md Section 16 for the current architecture.

## 16 (Original). Decision: No Shared Core Library Between BGWorker and Standalone

### Context

A natural question is whether the standalone Rust engine and the existing C BGWorker can share a common core library to avoid duplicating logic. We evaluated this and decided against it.

### What's Shareable vs. Not

| Component | Shareable? | Why |
|-----------|-----------|-----|
| Types (SyncState, SyncChange, etc.) | Yes | Pure data definitions |
| State machine (transitions, CATCHUP skip) | Yes | Pure logic, no I/O |
| SQL generation (INSERT VALUES, DELETE WHERE) | Yes | String building |
| Batcher (accumulation, flush triggers) | Mostly | Logic is same, memory allocation differs |
| pgoutput decoder | **No** | BGWorker uses PG-internal `logicalrep_read_*`; standalone must reimplement |
| WAL consumption | **No** | SPI polling vs. streaming replication protocol |
| Apply execution | **No** | SPI vs. libpq |
| State persistence | **No** | SPI queries vs. libpq queries |
| Memory management | **No** | `palloc`/MemoryContext vs. Rust heap |
| Connection lifecycle | **No** | PG backend context vs. tokio connections |

The I/O boundary — where most complexity and most code lives — is fundamentally different between the two runtimes. The shareable parts (types, state machine, SQL gen) total roughly 500-600 lines of logic.

### Approaches Considered

**Approach A: Rust core lib + two frontends (pgrx for PG extension)**

```
duckpipe-core/          (Rust library crate — shared logic)
├── types.rs
├── state_machine.rs
├── sql_gen.rs
└── batcher.rs

duckpipe-daemon/        (Rust binary — standalone)
├── wal_stream.rs       # streaming replication via tokio-postgres
├── decoder.rs          # pgoutput binary parser (Rust-native)
├── applier.rs          # libpq execution
└── main.rs

duckpipe-pgext/         (PG extension via pgrx — BGWorker)
├── worker.rs           # BGWorker using SPI + logicalrep_read_*
├── decoder.rs          # wraps PG's logicalrep_read_* into core types
├── applier.rs          # SPI execution
└── lib.rs
```

Problems:
- pgrx is a large dependency with its own build system and PG version pinning
- The decoder (one of the meatiest components) can't be shared anyway
- The BGWorker side still needs `palloc`, SPI, `MemoryContext` — essentially C-in-Rust through pgrx FFI
- Two I/O layers (SPI + libpq) in the same language is more code than maintaining one of each

**Approach B: Rust core exposed to C via FFI**

```
duckpipe-core/          (Rust cdylib — shared logic)
├── sql_gen.rs
├── batcher.rs
├── state_machine.rs
└── ffi.rs              # extern "C" functions

src/                    (C PG extension — links duckpipe-core.so)
├── worker.c            # unchanged
├── decoder.c           # unchanged, uses logicalrep_read_*
├── apply.c             # calls into Rust sql_gen via FFI
└── batch.c             # calls into Rust batcher via FFI
```

Problems:
- FFI overhead and complexity for sharing the simplest parts of the codebase
- The hard-to-maintain C code (memory management, SPI, error handling) stays in C
- Adds a Rust build dependency to the C extension's build process

**Approach C: Keep them separate (chosen)**

```
src/                    (C PG extension — current code, maintained as-is)
├── worker.c, decoder.c, batch.c, apply.c, api.c

duckpipe-daemon/        (Rust standalone — clean rewrite)
├── src/engine/, src/replication/, src/batch/, src/apply/, ...
```

### Decision: Separate Codebases (Approach C)

Rationale:

1. **The C extension already works.** It's ~2000 lines, battle-tested by the regression suite. Keeping it as-is for existing users costs nothing.

2. **The Rust standalone benefits from a clean slate.** Designing around tokio/async from the ground up is better than being constrained to share types with a C/pgrx codebase.

3. **The overlap is small.** The actual shared logic (state machine, SQL quoting, batch threshold logic) is trivial to reimplement. The hard parts (WAL consumption, apply, error recovery) can't be shared.

4. **Migration path is cleaner.** Users run either the C extension OR the Rust daemon against the same metadata tables (`duckpipe.sync_groups`, `duckpipe.table_mappings`). Once the Rust daemon proves itself, deprecate the C extension. No need for both to coexist in the same build.

5. **Avoids unnecessary dependencies.** pgrx is a solid project, but adopting it just to share 500 lines of logic adds a dependency with its own maintenance burden and PG version compatibility matrix.

### Coexistence Strategy

During the transition period, the C extension and Rust daemon share the **same metadata tables** and **same replication slots**:

```
Phase 1: C extension (BGWorker) is production, Rust daemon in development
Phase 2: Rust daemon validated against same regression scenarios
Phase 3: Users choose C extension OR Rust daemon (not both for same group)
Phase 4: C extension deprecated, thin PL/pgSQL extension replaces it for SQL API
```

The replication slot ensures only one consumer at a time per group, preventing conflicts. The metadata table schema is the shared interface between the two implementations.

---

## 17. Open Questions

1. **Protocol version**: pgoutput v1 is sufficient for current features. pgoutput v2 (PG 15+) adds streaming of in-progress transactions — worth supporting from the start?

2. **Direct DuckDB writes**: The standalone engine could bypass PostgreSQL entirely for writing to DuckLake, writing Parquet/Iceberg files directly. This eliminates the apply connection overhead but requires understanding DuckLake's internal catalog format. Worth exploring after v1?

3. **Multi-source**: A single duckpipe daemon could consume from multiple PostgreSQL instances and write to a central DuckLake. This is a natural extension of the standalone architecture. Should the config/state model support this from the start?

4. **Thin extension scope**: Should the optional PG extension handle publication/slot management (create_group, add_table) or should that move entirely to the daemon? The current `add_table()` creates publications and slots via SQL — this could stay in a thin extension or move to the daemon's API.

5. **Apply target flexibility**: With a standalone engine, the target doesn't have to be DuckLake-in-PostgreSQL. It could be a standalone DuckDB file, an Iceberg catalog, or any SQL-compatible target. How much generality to build in vs. keeping it focused on the DuckLake use case?
