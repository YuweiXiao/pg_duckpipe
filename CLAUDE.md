# CLAUDE.md

## Project

pg_duckpipe: PostgreSQL CDC extension — syncs heap tables to pg_ducklake columnar tables for HTAP.

## Build & Test

```bash
make installcheck               # Build + install + run all 19 regression tests
make check-regression TEST=api  # Run a single test
```

Tests use a temporary PG instance on port 5555 (wal_level=logical). See `test/regression/schedule`.

## Workspace

```
duckpipe-core/     # Shared engine: decoder, DuckDB flush, streaming replication, metadata
duckpipe-pg/       # PG extension (pgrx): GUCs, SQL API, bgworker
duckpipe-daemon/   # Standalone daemon (TCP, clap CLI)
test/regression/   # SQL regression tests
```

## Architecture

```
Heap Tables → WAL → Replication Slot (pgoutput) → Decoder → FlushCoordinator → DuckLake
```

- **WAL consumer** (main thread): streaming replication via `START_REPLICATION`, pushes decoded changes to per-table shared queues
- **Flush threads** (per-table OS threads): self-trigger on queue size or time interval, own DuckDB connection + tokio runtime, handle PG metadata updates independently
- **Backpressure**: AtomicI64 counter pauses WAL consumer when queues exceed `max_queued_changes`
- **Crash safety**: `confirmed_lsn = min(applied_lsn)` — slot never advances past durably flushed data
- **State machine** (per table): PENDING → SNAPSHOT → CATCHUP → STREAMING (or ERRORED with auto-retry)

## Key Rules

- Source tables must have PRIMARY KEY
- Target auto-created as `{table}_ducklake` via `LIKE source USING ducklake`
- `add_table()` auto-starts the bgworker
- TRUNCATE uses per-table drain before DELETE (DuckLake ignores TRUNCATE)

## Dev Guidelines

- **TDD**: failing test first → fix → `make installcheck` (all must pass)
- **Docs**: update `CLAUDE.md`, `doc/CODE_WALKTHROUGH.md`, `progress.md` after major changes
- Detailed code walkthrough: `doc/CODE_WALKTHROUGH.md`
- Implementation history: `PROCESS.md`
