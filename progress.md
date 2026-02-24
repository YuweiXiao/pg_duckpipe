# pg_duckpipe — Progress

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
- [x] Producer-consumer flush architecture (persistent per-table OS threads)
- [x] **Fully decoupled WAL/flush** — no synchronous barrier between WAL consumer and flush threads
  - [x] Self-triggered flush (batch threshold OR time interval)
  - [x] Backpressure via AtomicI64 (WAL consumer pauses when queues full)
  - [x] Flush threads handle PG metadata updates independently (own tokio runtime each)
  - [x] Per-table drain for TRUNCATE; drain_and_wait_all retained for shutdown only
- [x] PostgreSQL extension (pgrx): SQL API, GUCs, bgworker, bootstrap DDL
- [x] Standalone daemon (duckpipe-daemon) over TCP
- [x] 19 regression tests all passing

## TODO

### Performance / Scalability
- [ ] Flush thread pool — currently 1 OS thread + 1 tokio runtime + 1 DuckDB connection per table; for 50+ tables, a fixed-size thread pool would be more efficient
- [ ] Batch compaction tuning — explore DuckLake-level compaction to reduce Parquet file proliferation under sustained small-batch writes

### Features
- [ ] `source_uri` column for pg_mooncake compatibility
- [ ] `conninfo` column in sync_groups for remote PG support
- [ ] Schema DDL sync (ALTER TABLE ADD/DROP COLUMN propagation)
- [ ] Monitoring: expose backpressure state, flush thread metrics, queue depths in status() SRF

### Robustness
- [ ] Graceful handling of DuckLake schema drift (target table altered outside duckpipe)
- [ ] Connection pooling for flush thread PG metadata updates (currently short-lived connections per flush)
