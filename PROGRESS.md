# PROGRESS.md — pg_duckpipe v2 Implementation Progress

## Done

- [x] Pure-Rust pgoutput decoder (RELATION, INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT)
- [x] Typed `Value` enum with OID-based parsing (Bool, Int16, Int32, Int64, Float32, Float64, Text)
- [x] Per-table state machine (PENDING, SNAPSHOT, CATCHUP, STREAMING, ERRORED)
- [x] DuckDB embedded flush (buffer → compact → TOAST resolve → DELETE+INSERT via Appender API)
- [x] Persistent per-table FlushWorker sessions (reuse DuckDB connection + cached schema)
- [x] Streaming replication via `START_REPLICATION` (pgwire-replication crate)
- [x] Reconnect-per-cycle design (~1ms Unix socket reconnect)
- [x] Crash-safe slot advancement (`confirmed_lsn = min(applied_lsn)`)
- [x] Snapshot via SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`)
- [x] Parallel snapshot tasks (concurrent tokio::spawn per table)
- [x] CATCHUP skip logic (skip WAL changes <= snapshot_lsn, promote when pending_lsn >= snapshot_lsn)
- [x] OID-based WAL routing (handles ALTER TABLE RENAME)
- [x] TOAST unchanged column preservation on UPDATE
- [x] TRUNCATE propagation (per-table drain + DELETE FROM)
- [x] Per-table error isolation (one table failing doesn't block others)
- [x] ERRORED state with exponential backoff auto-retry (30s * 2^n, max ~30min)
- [x] Fully decoupled WAL/flush — self-triggered flush (batch threshold OR time interval), backpressure via AtomicI64, flush threads own tokio runtime + PG metadata updates
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] 19 regression tests all passing
- [x] Observability: `status()` SRF exposes `consecutive_failures`, `retry_at`, `applied_lsn`, `queued_changes` per table
- [x] Observability: `worker_status()` SRF exposes `total_queued_changes`, `is_backpressured`
- [x] Standardized logging: shared `init_subscriber`, all `eprintln!` replaced with `tracing` macros
- [x] `rows_synced` credited during snapshot
- [x] `confirmed_lsn` resets to 0 on table re-add — fixed via `COALESCE(applied_lsn, snapshot_lsn)` floor
- [x] Large catch-up batch stall (pure-insert path) — fixed via `may_have_conflicts` flag skipping DELETE scan
- [x] `lag_bytes` flat during catch-up — fixed: `StandbyStatusUpdate` sent each cycle even when no new WAL
- [x] Flush-thread drain capped at `batch_threshold` for incremental progress visibility

## TODO

### Performance / Scalability
- [ ] Flush thread pool — 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; fixed-size pool needed for 50+ tables
- [ ] Batch compaction tuning — reduce Parquet file proliferation under sustained small-batch writes
- [ ] Inline data flush
- [ ] Parquet-over-PG write throughput — ~10k rows/sec cap; bottleneck for large catch-up batches
- [ ] WAL consumer: merge `poll_messages` + `process_wal_messages` into a single inline streaming loop

  **Background:** The WAL consumer is split into two sequential stages per cycle:
  (1) `poll_messages` — buffers up to `batch_size_per_group` (default 100k) pgoutput messages
  into a `Vec`, blocking for ~3.3s at 10k TPS before returning anything.
  (2) `process_wal_messages` — decodes the buffered `Vec` and calls `push_change` for each row.

  This "collect-all-then-process" design means the flush coordinator queue receives rows in
  bursts (33k rows every ~3.3s) rather than as a continuous ~10k rows/sec trickle. The burst
  pattern produces irregular Parquet file sizes: with `flush_batch_threshold=10000` and
  `oltp_insert` (3 pgoutput messages per txn: BEGIN+INSERT+COMMIT), each burst delivers exactly
  `floor(100000/3) = 33333` rows → three 10k size-triggered flushes + one 3333-row
  time-triggered flush. The trailing 1-row flush (observed in benchmarks) is the straddle
  effect: since `100000 mod 3 = 1`, every cycle ends with a dangling BEGIN; the next cycle's
  first decoded row is isolated by the time-trigger (flush_interval=1s << poll cycle=3.3s).

  **Why the design exists today:**
  - `resolve_mapping` / `ensure_coordinator_queue` are async PG queries; without restructuring
    the borrow graph they can't easily be `.await`-ed inside a live `recv()` loop.
  - Per-cycle metadata ops (`seed_table_lsns`, `transition_catchup_to_streaming`,
    `update_worker_state`, `collect_results`) are naturally amortized at batch boundaries.
  - Crash-safe `confirmed_lsn` snapshot (min of all tables' applied_lsn) is computed cleanly
    once per batch; mid-stream checkpointing requires more careful LSN bookkeeping.

  **Refactor approach:** Inline decode+dispatch inside the `recv()` loop; keep a single
  hot path of `recv → decode → push_change` with no intermediate Vec. Move the bookkeeping
  (LSN checkpoint, worker-state update, CATCHUP→STREAMING transition) to a periodic heartbeat
  triggered by elapsed time or COMMIT count rather than batch boundaries.
  `batch_size_per_group` becomes a max-messages-before-checkpoint limit, not a delivery buffer.

### Features
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)
- [ ] **`REPLICA IDENTITY FULL` opt-in** — Per-table option to issue `ALTER TABLE <src> REPLICA IDENTITY FULL` on add. Eliminates TOAST unchanged columns from WAL, removing the correlated-UPDATE resolution step in `duckdb_flush.rs` and the `col_unchanged` bookkeeping on `Change`. Trade-off: higher WAL volume on source.
- [ ] Dockerfile for setting up a self-contained playground env

### Monitoring / Observability
- [ ] `applied_lsn` stays NULL during SNAPSHOT/CATCHUP — should be set to `snapshot_lsn` after snapshot completes
- [ ] `worker_state` not updated during snapshot processing — stale metrics while snapshots run
- [ ] A script for parsing perf log from benchmark and generate readable output

### Robustness
- [ ] Snapshot failures have no retry backoff — risk of thrash on repeated failures
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates (currently short-lived connections per flush)
- [ ] Regression tests for crash / error cases
