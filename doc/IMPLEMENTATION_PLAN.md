# pg_duckpipe v2: Implementation Plan

## Overview

This plan breaks down the v2 redesign into concrete implementation phases. Each phase builds on the previous one, with the first working end-to-end pipeline delivered by Phase 3. Later phases add snapshot support, the standalone binary, and production hardening.

The implementation follows the architecture defined in [DESIGN_V2.md](./DESIGN_V2.md).

---

## Project Structure

```
pg_duckpipe/
├── Cargo.toml                    # Workspace root
├── Cargo.lock
├── duckpipe-core/                # Core sync engine (shared by both modes)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                # Public API
│       ├── types.rs              # StagingRow, OpType, TableState, SyncGroup, TableMapping
│       ├── error.rs              # DuckPipeError, ErrorClass
│       ├── decoder.rs            # pgoutput decoding via postgres-replication crate
│       ├── queue.rs              # Per-table VecDeque<StagingRow> staging queues
│       ├── slot_consumer.rs      # WAL consumption, dispatch to queues, backpressure
│       ├── flush_worker.rs       # DuckDB buffer + compact + MERGE into DuckLake
│       ├── snapshot_worker.rs    # Temporary replication slot + bulk copy
│       ├── checkpoint.rs         # min(applied_lsn) → StandbyStatusUpdate
│       ├── state.rs              # Table state machine transitions
│       ├── metadata.rs           # Read/write duckpipe.sync_groups, table_mappings
│       └── service.rs            # Orchestrator: spawns slot consumer, flush workers, etc.
├── duckpipe-pg/                  # PostgreSQL bgworker (Mode 1)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                # pgrx entry point, _PG_init, bgworker registration
│       └── api.rs                # SQL functions: add_table, remove_table, create_group, etc.
├── duckpipe-standalone/          # Standalone binary (Mode 2)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs               # CLI entry point (clap)
│       └── config.rs             # TOML config loading
├── test/
│   ├── unit/                     # cargo test (decoder, state machine, queue)
│   └── regression/               # SQL regression tests (reuse + extend v1 tests)
└── doc/
    ├── DESIGN_V2.md
    └── IMPLEMENTATION_PLAN.md    # This file
```

---

## Dependencies

**duckpipe-core/Cargo.toml:**

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-postgres = { version = "0.7", features = ["runtime"] }
postgres-replication = { git = "https://github.com/MaterializeInc/rust-postgres" }
duckdb = { version = "1.0" }
deadpool-postgres = "0.14"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2.0"
serde = { version = "1.0", features = ["derive"] }
```

**duckpipe-pg/Cargo.toml:**

```toml
[dependencies]
duckpipe-core = { path = "../duckpipe-core" }
pgrx = "0.12"
```

**duckpipe-standalone/Cargo.toml:**

```toml
[dependencies]
duckpipe-core = { path = "../duckpipe-core" }
clap = { version = "4", features = ["derive"] }
toml = "0.8"
anyhow = "1.0"
```

---

## Phase 1: Foundation — Types, Decoder, Queues

Goal: Core data structures, pgoutput decoding, and in-memory staging queues. No I/O yet — everything is unit-testable.

### 1.1 Core Types (`types.rs`)

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpType {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

#[derive(Debug, Clone)]
pub struct StagingRow {
    pub seq: u64,
    pub lsn: u64,
    pub op_type: OpType,
    pub pk_values: Vec<Value>,
    pub col_values: Vec<Option<Value>>,
    pub col_unchanged: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableState {
    Pending,
    Snapshot,
    Catchup { snapshot_lsn: u64 },
    Streaming,
    Errored { message: String },
}

#[derive(Debug, Clone)]
pub struct SyncGroup {
    pub id: i32,
    pub name: String,
    pub publication: String,
    pub slot_name: String,
    pub conninfo: Option<String>,
    pub enabled: bool,
    pub confirmed_flush_lsn: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TableMapping {
    pub id: i32,
    pub group_id: i32,
    pub source_oid: u32,
    pub source_schema: String,
    pub source_table: String,
    pub target_schema: String,
    pub target_table: String,
    pub state: TableState,
    pub snapshot_lsn: Option<u64>,
    pub applied_lsn: Option<u64>,
    pub enabled: bool,
}

/// Typed column value (not VARCHAR — matches PG types from pgoutput)
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Bytea(Vec<u8>),
    Numeric(String),     // Decimal as string to preserve precision
    Timestamp(i64),      // Microseconds since epoch
    Date(i32),           // Days since epoch
    Uuid(uuid::Uuid),
    Json(String),
}

/// Cached schema info from pgoutput RELATION messages
pub struct TableSchema {
    pub rel_id: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub pk_columns: Vec<usize>,  // Indices into columns
}

pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub is_pk: bool,
}
```

### 1.2 Error Types (`error.rs`)

```rust
#[derive(thiserror::Error, Debug)]
pub enum DuckPipeError {
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("duckdb: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("decoder: {0}")]
    Decoder(String),
    #[error("state transition: {from:?} → {to:?}")]
    InvalidTransition { from: TableState, to: TableState },
    #[error("table not found: oid={0}")]
    TableNotFound(u32),
    #[error("snapshot: {0}")]
    Snapshot(String),
    #[error("replication: {0}")]
    Replication(String),
}

#[derive(Debug)]
pub enum ErrorClass {
    Transient,      // Auto-retry with backoff
    Configuration,  // Manual fix required
    Resource,       // Manual fix required
}

impl DuckPipeError {
    pub fn classify(&self) -> ErrorClass { ... }
}
```

### 1.3 pgoutput Decoder (`decoder.rs`)

Decode binary pgoutput messages using `postgres-replication` crate. Maintain a relation cache (rel_id → TableSchema). Convert tuple data into typed `Value` enum.

Key responsibilities:
- Parse RELATION messages → cache TableSchema
- Parse INSERT/UPDATE/DELETE → produce `StagingRow` with typed values
- Track `col_unchanged` for TOAST-unchanged columns (`TupleData::UnchangedToast`)
- Parse BEGIN/COMMIT → track transaction LSN boundaries (for seq numbering)
- Parse TRUNCATE → produce truncate events

Unit tests:
- Round-trip encoding/decoding for each message type
- TOAST unchanged detection
- Relation cache updates on schema change
- Composite PK extraction

### 1.4 In-Memory Staging Queues (`queue.rs`)

```rust
pub struct TableQueue {
    queue: Mutex<VecDeque<StagingRow>>,
    schema: Arc<TableSchema>,
}

impl TableQueue {
    pub fn append(&self, row: StagingRow) {
        self.queue.lock().unwrap().push_back(row);
    }

    pub fn drain(&self) -> Vec<StagingRow> {
        let mut q = self.queue.lock().unwrap();
        q.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

/// Registry of all per-table queues, keyed by source OID
pub struct QueueRegistry {
    queues: RwLock<HashMap<u32, Arc<TableQueue>>>,
}
```

Unit tests:
- Concurrent append + drain
- Queue len accuracy
- Registry add/remove tables

### 1.5 State Machine (`state.rs`)

Implement `TableState` transitions with validation:

| From | To | Condition |
|------|----|-----------|
| Pending | Snapshot | copy_data=true |
| Pending | Streaming | copy_data=false |
| Snapshot | Catchup | snapshot_lsn recorded |
| Catchup | Streaming | encountered lsn > snapshot_lsn |
| Any | Errored | error encountered |
| Errored | Pending | manual reset / resync |

Unit tests:
- All valid transitions succeed
- All invalid transitions fail
- Catchup → Streaming promotion logic

---

## Phase 2: Flush Worker — Persistent Per-Table DuckDB Sessions

Goal: Implement per-table flush workers with persistent embedded DuckDB sessions. Each table gets its own long-lived `FlushWorker` that pays the INSTALL+LOAD+ATTACH cost once at creation time.

### 2.1 Flush Worker (`flush_worker.rs`)

**Design: Per-table persistent sessions.** Each synced table gets its own `FlushWorker` holding a long-lived `duckdb::Connection`. The expensive one-time setup (INSTALL ducklake, LOAD ducklake, ATTACH to PostgreSQL) happens once when the worker is created. Subsequent flushes only create/drop the lightweight buffer table.

```rust
pub struct FlushWorker {
    target_key: String,                // "schema.table"
    mapping_id: i32,
    db: duckdb::Connection,            // Persistent per-table DuckDB instance
    lake_info: Option<LakeTableInfo>,  // Cached column types (invalidated on DDL)
    attnames: Vec<String>,
    key_attrs: Vec<usize>,
}
```

**Registry.** A `HashMap<String, FlushWorker>` keyed by target_key lives in the service layer. Workers are created lazily on first flush and destroyed when the table is removed from sync.

```rust
pub struct FlushWorkerRegistry {
    workers: HashMap<String, FlushWorker>,
    config: FlushConfig,
}

impl FlushWorkerRegistry {
    /// Get or create a FlushWorker for a target table.
    /// On first call for a target_key, creates a DuckDB instance and runs
    /// INSTALL+LOAD+ATTACH (one-time ~50ms cost).
    pub fn get_or_create(&mut self, target_key: &str, ...) -> Result<&mut FlushWorker>;

    /// Remove a worker (when table is removed from sync).
    /// Drops the DuckDB connection, releasing the ATTACH PG connection.
    pub fn remove(&mut self, target_key: &str);
}
```

**Lifecycle:**
- **Created**: Lazily, on first flush for a table (after first WAL changes arrive)
- **Reused**: Every subsequent flush for that table reuses the same DuckDB connection
- **Invalidated**: On RELATION message with changed schema → clear cached `lake_info`
- **Destroyed**: When table is removed from sync, or worker process shuts down

**Per-flush work (what happens each cycle):**
1. `CREATE TABLE buffer (...)` — lightweight, uses cached column types
2. `INSERT INTO buffer VALUES ...` — batched multi-row inserts
3. `CREATE TEMP TABLE compacted AS ...` — ROW_NUMBER compaction
4. TOAST resolution (UPDATE compacted SET ... from target)
5. `BEGIN; DELETE+INSERT; COMMIT` — apply to DuckLake
6. `DROP TABLE buffer, compacted` — cleanup

**What is NOT repeated per flush:**
- ~~`Connection::open_in_memory()`~~ — done once
- ~~`INSTALL ducklake; LOAD ducklake;`~~ — done once
- ~~`ATTACH 'ducklake:postgres:...'`~~ — done once (persistent PG connection from DuckDB)
- ~~`SET ducklake_retry_wait_ms = ...`~~ — done once
- ~~`discover_lake_table_info()`~~ — cached, only re-queried on schema change

**Connection overhead:** Each FlushWorker's ATTACH holds one PG backend connection (DuckDB→PostgreSQL via libpq). With N synced tables, that's N persistent PG connections. These are idle between flushes. For typical deployments (5-20 tables), this is acceptable. The tradeoff is N idle connections vs. N×(connect+auth+close) per poll cycle.

**ATTACH health:** The persistent DuckDB→PG connection from ATTACH may drop (PG restart, idle timeout, network issues). On flush failure, the FlushWorker should be destroyed and lazily recreated on the next attempt. The error recovery path (consecutive_failures → ERRORED) handles this naturally.

### 2.2 PG-to-DuckDB Type Mapping

Map PostgreSQL type OIDs (from pgoutput RELATION messages) to DuckDB column types for buffer table creation:

| PostgreSQL | DuckDB |
|-----------|--------|
| bool | BOOLEAN |
| int2 | SMALLINT |
| int4 | INTEGER |
| int8 | BIGINT |
| float4 | FLOAT |
| float8 | DOUBLE |
| numeric | DECIMAL |
| text, varchar | VARCHAR |
| bytea | BLOB |
| timestamp | TIMESTAMP |
| timestamptz | TIMESTAMPTZ |
| date | DATE |
| uuid | UUID |
| json, jsonb | JSON |

### 2.3 Apply SQL (DELETE+INSERT)

DuckLake's MERGE only supports a single action per MERGE statement. We use separate DELETE+INSERT wrapped in a transaction:

1. **Compact**: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _seq DESC)` deduplicates by PK, keeping latest change
2. **TOAST resolve**: For UPDATE rows with unchanged flags, fill in current target values before delete
3. **DELETE**: Remove rows matching any compacted row's PK
4. **INSERT**: Re-insert rows for INSERT and UPDATE ops (op_type IN (0, 1))

DELETE+INSERT is wrapped in `BEGIN/COMMIT` with `ROLLBACK` on failure for atomicity.

### 2.4 DuckLake Concurrent Access

Configure retry settings for optimistic concurrency conflicts (set once at worker creation):

```sql
SET ducklake_retry_wait_ms = 100;
SET ducklake_retry_backoff = 2.0;
SET ducklake_max_retry_count = 10;
```

**Current**: Flushes are sequential (DuckLake catalog lock prevents concurrent writes). Per-table sessions do not enable parallelism today, but the architecture is ready for when DuckLake adds fine-grained locking.

Integration tests:
- Flush worker creates buffer, appends rows, compacts, applies via DELETE+INSERT
- TOAST unchanged columns preserved correctly
- Compaction deduplicates by PK (keep last by seq)
- Flush trigger by size (batch_size_per_table)
- FlushWorker reuse across multiple flush cycles (persistent session)
- FlushWorker recreation after ATTACH connection failure

---

## Phase 3: Slot Consumer + Checkpoint — End-to-End Pipeline

Goal: Connect the slot consumer to flush workers and checkpoint manager. First working end-to-end CDC pipeline.

### 3.1 Slot Consumer (`slot_consumer.rs`)

Single async task per sync group. Uses `START_REPLICATION` protocol via `tokio-postgres`.

```rust
pub struct SlotConsumer {
    replication_conn: ReplicationClient,  // tokio-postgres replication connection
    decoder: PgoutputDecoder,
    queue_registry: Arc<QueueRegistry>,
    config: SlotConsumerConfig,
    seq_counter: AtomicU64,
    lsn_rx: mpsc::Receiver<(u32, u64, usize)>,  // from flush workers
    applied_lsns: HashMap<u32, u64>,             // per-table tracking
    buffer_row_counts: HashMap<u32, usize>,      // per-table buffer depth
}
```

Implementation tasks:

**a) Replication connection setup:**
- Connect with `replication=database` parameter
- `CREATE_REPLICATION_SLOT <name> LOGICAL pgoutput` (if not exists)
- `START_REPLICATION SLOT <name> LOGICAL <start_lsn> (proto_version '1', publication_names '<pub>')`

**b) WAL message loop:**
- Read `XLogData` messages from replication stream
- Decode via `decoder.decode(data)`
- For each INSERT/UPDATE/DELETE: create `StagingRow`, append to appropriate `TableQueue`
- For CATCHUP tables: skip rows where `lsn <= snapshot_lsn`
- Assign monotonically increasing `seq` to each row

**c) Backpressure:**
- Periodically collect `(oid, applied_lsn, buffer_row_count)` from flush worker channel
- Compute `total_pending = sum(queue.len() + buffer_row_count)`
- If `total_pending > backpressure_max_staging_rows`: stop reading from replication stream
- Resume when total drops below threshold

**d) Keepalive:**
- Respond to `PrimaryKeepalive` messages from PostgreSQL
- Include current `write_lsn` in `StandbyStatusUpdate` responses

### 3.2 Checkpoint Manager (`checkpoint.rs`)

```rust
pub struct CheckpointManager {
    slot_consumer: Arc<SlotConsumer>,
    metadata_conn: deadpool_postgres::Pool,
    applied_lsns: HashMap<u32, u64>,  // per-table
}
```

Implementation tasks:
- Receive `applied_lsn` updates from flush workers
- Compute `confirmed_flush_lsn = min(all applied_lsn)`
- Send `StandbyStatusUpdate` via replication connection with confirmed LSN
- Persist `confirmed_flush_lsn` to `duckpipe.sync_groups`

### 3.3 Service Orchestrator (`service.rs`)

Ties all components together:

```rust
pub struct DuckPipeService {
    config: ServiceConfig,
    metadata_pool: deadpool_postgres::Pool,
}

impl DuckPipeService {
    pub async fn run(&self) -> Result<(), DuckPipeError> {
        // 1. Load groups and table mappings from duckpipe.* metadata
        // 2. For each group:
        //    a. Create QueueRegistry with queues for each STREAMING/CATCHUP table
        //    b. Spawn SlotConsumer task
        //    c. Spawn FlushWorker task per table
        //    d. Spawn CheckpointManager task
        // 3. For each PENDING table with copy_data=true:
        //    a. Spawn SnapshotWorker task
        // 4. Monitor for new add_table / remove_table requests
        // 5. Handle graceful shutdown
    }
}
```

### 3.4 Metadata Access (`metadata.rs`)

Read/write `duckpipe.sync_groups` and `duckpipe.table_mappings` in source PostgreSQL:

```rust
pub struct MetadataStore {
    pool: deadpool_postgres::Pool,
}

impl MetadataStore {
    pub async fn load_groups(&self) -> Result<Vec<SyncGroup>>;
    pub async fn load_table_mappings(&self, group_id: i32) -> Result<Vec<TableMapping>>;
    pub async fn update_table_state(&self, id: i32, state: &TableState) -> Result<()>;
    pub async fn update_applied_lsn(&self, id: i32, lsn: u64) -> Result<()>;
    pub async fn update_confirmed_flush_lsn(&self, group_id: i32, lsn: u64) -> Result<()>;
}
```

### 3.5 End-to-End Integration Test

At this point, test the full pipeline:

1. Start PostgreSQL with `wal_level=logical`
2. Create source table with PK, insert rows
3. Call `create_group()`, `add_table()` with `copy_data=false`
4. Start service
5. INSERT/UPDATE/DELETE on source
6. Verify changes appear in DuckLake target
7. Verify `applied_lsn` advances
8. Verify TOAST unchanged columns preserved on UPDATE

---

## Phase 4: Snapshot Workers

Goal: Support `copy_data=true` for initial full table copy with consistent CATCHUP handoff.

### 4.1 Snapshot Worker (`duckpipe-core/src/snapshot.rs`)

**Implemented** using SQL functions instead of the replication protocol (because `tokio-postgres` 0.7 does not support the `replication=database` startup parameter).

```rust
pub async fn process_snapshot_task(
    source_schema: &str, source_table: &str,
    target_schema: &str, target_table: &str,
    connstr: &str, timing: bool, task_id: i32,
) -> Result<u64, String>
```

Implementation (actual):

**a) Create temporary replication slot via SQL:**
- Connection A: `BEGIN ISOLATION LEVEL REPEATABLE READ`
- Connection A: `SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', true)` → `consistent_point`
- Connection A: `SELECT pg_export_snapshot()` → `snapshot_name`
- Unique slot name per task: `duckpipe_snap_{task_id}`

**b) Import snapshot and copy data:**
- Connection B: `BEGIN ISOLATION LEVEL REPEATABLE READ`
- Connection B: `SET TRANSACTION SNAPSHOT '<snapshot_name>'`
- Connection B: `DELETE FROM target; INSERT INTO target SELECT * FROM source`
- Connection B: `COMMIT`

**c) State transition:**
- Record `snapshot_lsn = consistent_point` in metadata
- Set table state to CATCHUP
- The service handles CATCHUP → STREAMING promotion (skip lsn <= snapshot_lsn)

**d) Cleanup:**
- Connection A: `COMMIT` (ends the REPEATABLE READ transaction)
- Temporary replication slot auto-drops when Connection A closes

### 4.2 CATCHUP Skip Logic (in flush worker)

When table is in CATCHUP state:
- For each row from the buffer: if `lsn <= snapshot_lsn`, skip (already in snapshot)
- When first row with `lsn > snapshot_lsn` is applied, promote to STREAMING
- Update state in metadata

### 4.3 Tests

- Snapshot + concurrent writes → no data loss (port v1's `snapshot_race.sql`)
- CATCHUP → STREAMING promotion timing
- Multiple tables snapshot simultaneously
- Large table snapshot (verify Appender bulk loading)

---

## Phase 5: PostgreSQL Extension (Mode 1)

Goal: Package the sync service as a PostgreSQL background worker using pgrx.

### 5.1 pgrx Extension (`duckpipe-pg/src/lib.rs`)

```rust
use pgrx::prelude::*;

pgrx::pg_module_magic!();

#[pg_guard]
pub extern "C" fn _PG_init() {
    // Register GUC parameters
    // Register background worker
}
```

### 5.2 Background Worker

The bgworker spawns a tokio runtime and runs `DuckPipeService`:

```rust
#[pg_guard]
pub extern "C" fn duckpipe_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorkerBlockSignals();
    BackgroundWorkerUnblockSignals();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let service = DuckPipeService::new(config_from_gucs()).await.unwrap();
        service.run().await.unwrap();
    });
}
```

### 5.3 SQL API Functions

Port v1 SQL functions to pgrx:

```rust
#[pg_extern]
fn create_group(name: &str) -> i32 { ... }

#[pg_extern]
fn add_table(
    group_name: &str,
    source_table: &str,
    target_table: Option<&str>,
    copy_data: Option<bool>,
) -> i32 { ... }

#[pg_extern]
fn remove_table(group_name: &str, source_table: &str) { ... }

#[pg_extern]
fn start_worker() { ... }

#[pg_extern]
fn stop_worker() { ... }

#[pg_extern]
fn resync_table(group_name: &str, source_table: &str) { ... }
```

### 5.4 Monitoring SRFs

```rust
#[pg_extern]
fn groups() -> TableIterator<'static, (
    name!(id, i32),
    name!(name, String),
    name!(slot_name, String),
    name!(confirmed_flush_lsn, Option<String>),
    name!(enabled, bool),
)> { ... }

#[pg_extern]
fn tables() -> TableIterator<'static, (
    name!(group_name, String),
    name!(source_table, String),
    name!(target_table, String),
    name!(state, String),
    name!(applied_lsn, Option<String>),
    name!(rows_synced, i64),
)> { ... }

#[pg_extern]
fn status() -> TableIterator<'static, (
    name!(group_name, String),
    name!(tables_streaming, i32),
    name!(tables_snapshot, i32),
    name!(tables_errored, i32),
    name!(total_queue_depth, i64),
    name!(total_buffer_rows, i64),
)> { ... }
```

### 5.5 GUC Parameters

```rust
static POLL_INTERVAL: GucSetting<i32> = GucSetting::new(1000);
static BATCH_SIZE_PER_TABLE: GucSetting<i32> = GucSetting::new(1000);
static BATCH_MAX_FILL_MS: GucSetting<i32> = GucSetting::new(5000);
static ENABLED: GucSetting<bool> = GucSetting::new(true);
static DEBUG_LOG: GucSetting<bool> = GucSetting::new(false);
static BACKPRESSURE_ENABLED: GucSetting<bool> = GucSetting::new(true);
static BACKPRESSURE_MAX_STAGING_ROWS: GucSetting<i32> = GucSetting::new(100000);
static DATA_INLINING_ROW_LIMIT: GucSetting<i32> = GucSetting::new(1000);
static MAX_SNAPSHOT_WORKERS: GucSetting<i32> = GucSetting::new(4);
```

### 5.6 Publication Management

`add_table()` and `remove_table()` must update the publication:

- `ALTER PUBLICATION <pub> ADD TABLE <schema.table>`
- `ALTER PUBLICATION <pub> DROP TABLE <schema.table>`
- `create_group()` → `CREATE PUBLICATION <pub>` (initially empty)
- Auto-create target DuckLake table: `CREATE TABLE IF NOT EXISTS ... (LIKE source) USING ducklake`

### 5.7 Regression Tests

Port and extend v1 regression tests:

| Test | Status | Description |
|------|--------|-------------|
| `api.sql` | Port from v1 | Group/table management API |
| `monitoring.sql` | Port from v1 | groups()/tables()/status() SRFs |
| `streaming.sql` | Port from v1 | INSERT/UPDATE/DELETE CDC sync |
| `snapshot_updates.sql` | Port from v1 | Initial copy + concurrent updates |
| `catchup_handoff.sql` | Port from v1 | CATCHUP → STREAMING handoff |
| `multiple_tables.sql` | Port from v1 | Multiple tables in same group |
| `data_types.sql` | Port from v1 | Various PG data types |
| `resync.sql` | Port from v1 | resync_table() functionality |
| `truncate.sql` | Port from v1 | TRUNCATE propagation |
| `toast_unchanged.sql` | New | Verify MERGE preserves TOAST columns correctly |
| `snapshot_race.sql` | Port from v1 | Concurrent writes during snapshot |
| `backpressure.sql` | New | Verify throttling under load |
| `auto_start.sql` | Port from v1 | Worker auto-start on add_table() |

---

## Phase 6: Standalone Binary (Mode 2) — **IMPLEMENTED**

Goal: Same `duckpipe-core` engine, but runs as a separate process connecting via TCP.

### 6.1 CLI Entry Point (`duckpipe-daemon/src/main.rs`)

**Implemented** with `clap` derive API:

```
duckpipe --connstr "host=localhost port=5432 dbname=mydb user=replicator"
         [--poll-interval 1000]
         [--batch-size-per-table 1000]
         [--batch-size-per-group 10000]
         [--ducklake-schema ducklake]
         [--debug]
```

CLI args validated with minimum values matching GUC constraints (poll_interval >= 100, batch_size_per_table >= 1, batch_size_per_group >= 100).

### 6.2 Connection

Uses libpq-style `key=value` connection strings parsed into individual fields for `pgwire-replication` (TCP):

```rust
fn parse_connstr(connstr: &str) -> ConnParams { ... }
```

- `SlotConsumer::connect_tcp()` for streaming replication (new method added to core)
- `tokio-postgres` for metadata and snapshot connections
- `tracing-subscriber` with `env-filter` for structured logging
- `signal::ctrl_c()` for graceful shutdown

### 6.3 Main Loop

Both the bgworker and daemon delegate to `duckpipe_core::service::run_sync_cycle()`:
1. Connect to PG via tokio-postgres
2. Get enabled sync groups from metadata
3. For each group: process snapshots (parallel), then try streaming WAL (fall back to SQL polling)
4. Close connection

The daemon passes `SlotConnectParams::Tcp` for TCP connections; the bgworker passes `SlotConnectParams::Unix` for Unix socket connections. Uses `tokio::main` (multi-threaded runtime) instead of bgworker's single-thread runtime.

### 6.4 Integration Tests

- Manual smoke test: Start PG with `wal_level=logical`, set up metadata via SQL API, stop bgworker, run daemon, verify sync works
- Automated regression tests verify bgworker path (all 18 pass)

---

## Phase 7: Production Hardening

Goal: Error handling, retry policies, monitoring, and operational tooling.

### 7.1 Error Classification and Retry

Implement `ErrorClass` in each component:
- **Transient** (connection timeout, DuckDB busy, DuckLake conflict) → exponential backoff retry
- **Configuration** (schema mismatch, table not found) → ERRORED state, manual fix
- **Resource** (disk full, slot limit) → ERRORED state, manual fix

### 7.2 Schema Change Detection

When the slot consumer receives a RELATION message that differs from the cached TableSchema:
1. Transition table to ERRORED with descriptive message
2. Stop flush worker for that table
3. Log the schema diff

### 7.3 DuckLake Data Inlining

Configure data inlining to avoid small files problem:
- Set `ducklake_data_inlining_row_limit` matching `batch_size_per_table`
- Periodically call `ducklake.flush_inlined_data()` for tables with accumulated inline data

### 7.4 Restart Recovery

On service restart:
1. In-memory queues are empty (new process)
2. DuckDB buffer tables are empty (in-memory DuckDB, new instance per worker)
3. Load `applied_lsn` for each table from metadata
4. Slot consumer resumes from `confirmed_flush_lsn`
5. Flush workers skip rows with `lsn <= applied_lsn`
6. Rows near boundary may be re-applied — idempotent via MERGE

### 7.5 Operational Monitoring

- `duckpipe.status()` SRF: queue depths, buffer rows, lag, error counts
- Tracing integration: structured logs with span context per table
- Optional: Prometheus metrics export (standalone mode)

---

## Phase 8: Migration from v1

Goal: Upgrade path from the existing C-based v1 extension.

### 8.1 Schema Migration

```sql
-- Add new columns
ALTER TABLE duckpipe.table_mappings
    ADD COLUMN IF NOT EXISTS source_oid OID,
    ADD COLUMN IF NOT EXISTS source_uri TEXT,
    ADD COLUMN IF NOT EXISTS applied_lsn PG_LSN,
    ADD COLUMN IF NOT EXISTS error_message TEXT,
    ADD COLUMN IF NOT EXISTS retry_at TIMESTAMPTZ;

-- Populate source_oid from pg_class
UPDATE duckpipe.table_mappings tm
SET source_oid = c.oid
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = tm.source_schema AND c.relname = tm.source_table;

-- Populate applied_lsn from group's confirmed_flush_lsn
UPDATE duckpipe.table_mappings tm
SET applied_lsn = sg.confirmed_flush_lsn
FROM duckpipe.sync_groups sg
WHERE tm.group_id = sg.id AND tm.applied_lsn IS NULL;
```

### 8.2 Extension Upgrade

```sql
ALTER EXTENSION duckpipe UPDATE TO '2.0';
```

This replaces the C background worker with the Rust-based service. Existing replication slots and publications are reused — no data loss.

---

## Milestones

| Milestone | Criteria |
|-----------|----------|
| **M1: Foundation** | Core types, decoder, queues — all unit tests pass |
| **M2: Flush Worker** | DuckDB buffer + MERGE working in isolation |
| **M3: End-to-End** | Slot consumer → queues → flush → DuckLake pipeline works |
| **M4: Snapshots** | copy_data=true with CATCHUP handoff |
| **M5: PG Extension** | pgrx bgworker, SQL API, regression tests pass |
| **M6: Standalone** | CLI binary with remote PG support |
| **M7: Production** | Error handling, retry, monitoring, restart recovery |
| **M8: Migration** | v1 → v2 upgrade path tested |

---

## Dependencies Between Phases

```
Phase 1 (Foundation)
    │
    ├──► Phase 2 (Flush Worker)
    │        │
    │        └──► Phase 3 (Slot Consumer + Checkpoint = End-to-End)
    │                 │
    │                 ├──► Phase 4 (Snapshot Workers)
    │                 │
    │                 ├──► Phase 5 (PG Extension)
    │                 │
    │                 └──► Phase 6 (Standalone Binary)
    │
    Phases 4, 5, 6 can proceed in parallel after Phase 3
    │
    └──► Phase 7 (Hardening) — after Phases 4-6
             │
             └──► Phase 8 (Migration) — after Phase 7
```
