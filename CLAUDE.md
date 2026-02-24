# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_duckpipe is a PostgreSQL extension that provides automatic CDC (Change Data Capture) synchronization from heap tables (row store) to pg_ducklake columnar tables (column store). It enables HTAP (Hybrid Transactional/Analytical Processing) within a single PostgreSQL instance.

## Build Commands

```bash
make                            # Build the extension
make install                    # Install to PostgreSQL
make installcheck               # Build + install + run regression tests
make check-regression           # Run regression tests only
make check-regression TEST=api  # Run a single test
make clean-regression           # Remove test artifacts
```

Tests run on a temporary PostgreSQL instance (port 5555) with special config (wal_level=logical, shared_preload_libraries). Test infrastructure lives in `test/regression/` with its own Makefile. 19 regression tests; see `test/regression/schedule` for the full list.

## Architecture

### Data Flow
```
Heap Tables → WAL → Logical Decoding Slot (pgoutput) → Sync Engine → DuckLake Tables
```

The sync engine runs as either a PostgreSQL background worker (Mode 1) or a standalone daemon (Mode 2).

### Workspace Structure (Rust)

```
pg_duckpipe/
├── Cargo.toml                  # Workspace root
├── duckpipe-core/              # Shared sync engine library
│   └── src/
│       ├── types.rs            # Value enum, Change, SyncGroup, TableMapping, LSN helpers
│       ├── decoder.rs          # pgoutput binary decoding via pgwire-replication
│       ├── slot_consumer.rs    # START_REPLICATION streaming (Unix + TCP)
│       ├── snapshot.rs         # Pure Rust snapshot (SQL-based slot + pg_export_snapshot)
│       ├── service.rs          # Orchestrator: process groups, WAL consumption, flush
│       ├── metadata.rs         # Read/write duckpipe.sync_groups, table_mappings
│       ├── duckdb_flush.rs     # Per-table DuckDB flush workers (Appender → buffer → compact → apply)
│       ├── flush_coordinator.rs # Decoupled producer-consumer coordinator with self-triggered flush threads
│       ├── flush_worker.rs     # Metrics update, error state handling via PG connection
│       ├── queue.rs            # Per-table TableQueue staging
│       ├── state.rs            # Table state machine
│       └── error.rs            # Error types
├── duckpipe-pg/                # PostgreSQL extension (Mode 1: bgworker)
│   └── src/
│       ├── lib.rs              # pgrx entry, _PG_init, GUCs, bgworker registration
│       ├── api.rs              # SQL API: add_table, remove_table, create_group, etc.
│       └── worker.rs           # BGWorker main loop — calls duckpipe_core
├── duckpipe-daemon/            # Standalone daemon (Mode 2: TCP)
│   └── src/
│       └── main.rs             # CLI entry (clap), tokio::main, same duckpipe_core logic
└── test/regression/            # SQL regression tests
```

### Key Data Structures (duckpipe-core/src/types.rs)

- **Value**: Typed column value enum (Null, Bool, Int16, Int32, Int64, Float32, Float64, Text). Parsed from pgoutput text representation using RELATION message type OIDs. Unrecognized types fall back to Text, which DuckDB auto-casts.
- **Change**: A single decoded WAL change (INSERT/UPDATE/DELETE with typed `Vec<Value>` column values)
- **RelCacheEntry**: Cached schema info from pgoutput RELATION messages (column names, PK indices, type OIDs)
- **SyncGroup**: Represents a publication + replication slot pair (multiple tables share one group)
- **TableMapping**: Maps source table to target DuckLake table with state and snapshot_lsn

### Target Table Auto-Creation

`add_table()` always auto-creates the target DuckLake table using `CREATE TABLE IF NOT EXISTS ... (LIKE source) USING ducklake`.

Default naming: source `public.lineitem` → target `public.lineitem_ducklake` (same schema, `_ducklake` suffix). The `target_table` parameter can override this.

### Table State Machine

State is tracked **per table** (not per group). Each table in a group transitions independently.

When adding a new table with copy_data=true: SNAPSHOT (copy data, record WAL LSN) → CATCHUP (skip WAL changes ≤ snapshot_lsn, apply changes > snapshot_lsn) → STREAMING (normal operation)

When adding with copy_data=false: directly STREAMING

**CATCHUP → STREAMING transition**: A CATCHUP table is promoted to STREAMING only when the group's WAL consumption has advanced past the table's `snapshot_lsn` (i.e., `snapshot_lsn <= pending_lsn`). This prevents premature promotion when `batch_size_per_group` limits cause partial WAL consumption across multiple poll rounds.

## GUC Parameters

All GUC parameters are `PGC_SIGHUP` — must be changed via `ALTER SYSTEM SET` + `SELECT pg_reload_conf()`, not session-level `SET`.

```sql
duckpipe.poll_interval              -- ms between polls (default 1000, min 100)
duckpipe.batch_size_per_group       -- max WAL messages per group per sync cycle (default 100000, min 100)
duckpipe.enabled                    -- enable/disable worker (default on)
duckpipe.debug_log                  -- emit critical-path timing logs (default off)
duckpipe.data_inlining_row_limit   -- max rows to inline in INSERT VALUES (default 0)
duckpipe.flush_interval             -- ms for self-triggered flush interval (default 1000, min 100)
duckpipe.flush_batch_threshold      -- queued changes that trigger immediate flush (default 10000, min 100)
duckpipe.max_queued_changes         -- max total queued changes before backpressure (default 500000, min 1000)
```

## Worker Lifecycle

- **Auto-start**: `add_table()` automatically starts a background worker if one is not already running for the current database. No explicit `start_worker()` call is needed.
- **Manual start**: `start_worker()` is still available as an explicit override.
- **Stop**: `stop_worker()` terminates the worker. The worker has `bgw_restart_time = 10` so it auto-restarts after crash; to pause, use `ALTER SYSTEM SET duckpipe.enabled = off; SELECT pg_reload_conf();`

## WAL Consumption and Flush

WAL consumption uses streaming replication exclusively: `START_REPLICATION` protocol via `pgwire-replication` crate. pgoutput binary messages are decoded by a pure-Rust decoder in `duckpipe-core/src/decoder.rs`.

Each sync cycle: connect → poll up to `batch_size_per_group` WAL messages (with ~500ms timeout) → decode → push changes to `FlushCoordinator` shared queues → disconnect. No synchronous barrier between WAL processing and flush.

**Decoupled flush architecture**: `FlushCoordinator` manages persistent per-table flush threads that are fully decoupled from the WAL consumer. Each flush thread:
- Owns its own `FlushWorker` (DuckDB connection) and tokio runtime (for PG metadata updates)
- Self-triggers flush based on queue size (`flush_batch_threshold`) or time (`flush_interval`)
- Handles PG metadata updates independently (applied_lsn, metrics, error state)
- On error: drops worker (lazily recreated), records error in PG, transitions to ERRORED after `ERRORED_THRESHOLD` consecutive failures

**Backpressure**: `BackpressureState` tracks total queued changes via `AtomicI64`. When `total_queued >= max_queued_changes`, the WAL consumer skips polling to let flush threads catch up. Backpressure counter is incremented on `push_change()` and decremented when flush threads drain from the shared queue.

**Crash safety**: The replication slot never advances past what all tables have durably flushed. Flush threads write `applied_lsn` to PG metadata after each successful DuckDB flush. The WAL consumer periodically reads `min(applied_lsn)` from PG and uses it as `confirmed_lsn` for the `StandbyStatusUpdate`. On crash, in-flight changes in flush thread buffers are lost but replay from WAL.

**TRUNCATE**: Uses per-table synchronous drain (`drain_and_wait_table()`) to ensure pending changes are flushed before `DELETE FROM` clears the target table.

**Snapshot**: SQL functions (`pg_create_logical_replication_slot` + `pg_export_snapshot`) in a REPEATABLE READ transaction. Shared by bgworker and daemon.

## Requirements

- PostgreSQL 14+ with `wal_level=logical`
- pg_duckdb extension installed
- Source tables must have PRIMARY KEY
- Extension in `shared_preload_libraries`

## Development Guidelines

- **TDD for bug fixes**: Write a failing regression test first → verify it fails → implement fix → verify it passes.
- **Regression verification**: Always run `make installcheck` after every major change. All 19 tests must pass before considering a change complete.
- **Update docs after major changes**: `CLAUDE.md` (architecture, GUCs), `doc/CODE_WALKTHROUGH.md` (detailed walkthrough), `PROCESS.md` (completed work, TODOs, test coverage).
- **Update `PROCESS.md`** after completing any implementation work.
