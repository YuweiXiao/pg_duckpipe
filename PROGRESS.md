# PROGRESS.md — pg_duckpipe v2 Implementation Progress

## Done

### Core Engine
- [x] Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- [x] Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- [x] Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- [x] DuckDB embedded flush (buffer → compact → DELETE+INSERT via Appender API)
- [x] Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- [x] Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- [x] Reconnect-per-cycle design (~1ms Unix socket reconnect)
- [x] Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- [x] OID-based WAL routing (handles ALTER TABLE RENAME)
- [x] TOAST unchanged column preservation on UPDATE
- [x] TRUNCATE propagation (per-table drain + DELETE FROM)
- [x] Per-table error isolation (one table failing doesn't block others)
- [x] ERRORED state with exponential backoff auto-retry (30s × 2^n, max ~30min)
- [x] Fully decoupled WAL/flush — self-triggered flush (batch threshold OR time interval), backpressure via AtomicI64, flush threads own tokio runtime + PG metadata updates
- [x] `REPLICA IDENTITY FULL` enforced — `add_table()` auto-sets identity; no TOAST-unchanged fallback needed

### Snapshots
- [x] Chunked COPY pipeline — direct DuckDB connections per snapshot task, replacing PG-side INSERT INTO (which crashed on concurrent COMMIT via pg_ducklake)
- [x] Parallel snapshot tasks (concurrent tokio::spawn per table)
- [x] Decoupled snapshots from sync cycle — SnapshotManager runs fire-and-forget; WAL changes buffered in paused flush queues during snapshot
- [x] CATCHUP skip logic (skip WAL changes ≤ snapshot_lsn, promote when pending_lsn ≥ snapshot_lsn)

### Performance
- [x] WAL consumer inline hot path �� eliminated collect-all-then-process Vec buffer; bookkeeping via `run_heartbeat` every 10k commits or 500ms
- [x] Large catch-up batch stall — fixed via `may_have_conflicts` flag skipping DELETE scan on pure-insert path
- [x] Flush-thread drain capped at `batch_threshold` for incremental progress visibility
- [x] Mixed DML correctness — flush DELETE used all-column WHERE instead of PK-only; fixed by caching `pk_key_attrs` per relation

### Observability
- [x] `status()` SRF: `consecutive_failures`, `retry_at`, `applied_lsn`, `queued_changes`, `snapshot_duration_ms`, `snapshot_rows`
- [x] `worker_status()` SRF: `total_queued_changes`, `is_backpressured`
- [x] Standardized logging: shared `init_subscriber`, all output via `tracing` macros
- [x] `rows_synced` credited during snapshot
- [x] Benchmark suite (`bench_suite.sh`) — 4 scenarios with automated analysis

### Infrastructure
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] 24 regression tests all passing
- [x] Dockerfile for self-contained playground env

### Bug Fixes
- [x] `confirmed_lsn` resets to 0 on table re-add — fixed via `COALESCE(applied_lsn, snapshot_lsn)` floor
- [x] `lag_bytes` flat during catch-up — fixed: `StandbyStatusUpdate` sent each cycle even when no new WAL

## TODO

### Performance / Scalability
- [ ] Snapshot WAL buffering memory — unbounded accumulation in paused flush queues during long snapshots; consider spill-to-disk or snapshot-aware backpressure
- [ ] Multi-table streaming lag 20x single-table — likely leftover flush threads from prior bench_suite scenarios; standalone runs show normal lag
- [x] Snapshot detection delay up to `poll_interval` — LISTEN/NOTIFY wakeup: `add_table()`, `resync_table()`, `enable_group()` fire `NOTIFY duckpipe_wakeup`; bgworker LISTENs and wakes immediately
- [ ] Flush thread pool — 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; need fixed-size pool for 50+ tables
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap; bottleneck for large catch-up batches
- [ ] DELETE phase dominates mixed DML flush — cross-catalog EXISTS join scans full Parquet; consider DuckLake-native delete-by-PK
- [ ] DuckLake commit contention in multi-table flushes — commit jumps from 1.7ms (1T) to 10.9ms (4T); metadata lock contention
- [ ] Mixed DML catch-up throughput appears as 0 rows/s — `rows_synced` only reflects net change; need `changes_applied` counter
- [ ] Mixed DML replication lag 50-100x append — WAL amplification from REPLICA IDENTITY FULL + Parquet-scan DELETE phase

### Features
- [ ] Dedicated bgworker per group — one worker per sync group for full isolation (own FlushCoordinator, SnapshotManager, SlotState)
- [ ] Per-group GUC overrides — nullable config columns in `sync_groups`; NULL falls back to global GUC
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)
- [ ] Sync tables with no PK (ensure e2e EOS)
- [ ] CI: `cargo chef` pattern for cached Rust dependency compilation

### Monitoring / Observability
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP — should be set to `snapshot_lsn` after snapshot completes
- [ ] `worker_state` not updated during snapshot processing — stale metrics while snapshots run

### Bugs
- [ ] Benchmark suite cleanup incomplete — orphaned mappings for sbtest2-4 remain after multi→single-table scenario transition

### Robustness
- [ ] Snapshot failures have no retry backoff — risk of thrash on repeated failures
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates
- [ ] Regression tests for crash / error cases
