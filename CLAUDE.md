# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_ducklake_sync is a PostgreSQL extension that provides automatic CDC (Change Data Capture) synchronization from heap tables (row store) to pg_ducklake columnar tables (column store). It enables HTAP (Hybrid Transactional/Analytical Processing) within a single PostgreSQL instance.

**Key design principle**: Reuse PostgreSQL's production `pgoutput` plugin and built-in `logicalrep_read_*` parsing functions rather than implementing custom decoding.

## Build Commands

```bash
make                    # Build the extension
make install            # Install to PostgreSQL
make installcheck       # Run regression tests (creates temp instance with wal_level=logical)
make format             # Format C code with clang-format
```

Tests run on a temporary PostgreSQL instance (port 5555) with special config (wal_level=logical, shared_preload_libraries).

## Architecture

### Data Flow
```
Heap Tables → WAL → Logical Decoding Slot (pgoutput) → Background Worker → DuckLake Tables
```

### Source Files (src/)

- **pg_ducklake_sync.c**: Extension entry point, GUC parameters, background worker registration
- **worker.c**: Background worker main loop, polls sync groups for changes
- **decoder.c**: Message dispatcher - calls PostgreSQL's `logicalrep_read_*` functions to parse binary pgoutput messages ('B'=BEGIN, 'R'=RELATION, 'I'=INSERT, 'U'=UPDATE, 'D'=DELETE, 'C'=COMMIT)
- **batch.c**: Accumulates changes per table into batches before applying
- **apply.c**: Generates and executes SQL to apply batched changes to DuckLake tables (DELETE then INSERT for UPDATEs)
- **api.c**: SQL function implementations (add_table, remove_table, create_group, etc.)

### Key Data Structures (pg_ducklake_sync.h)

- **SyncGroup**: Represents a publication + replication slot pair (multiple tables share one group)
- **TableMapping**: Maps source table to target DuckLake table with sync state (PENDING/SNAPSHOT/CATCHUP/STREAMING)
- **SyncBatch**: Accumulated changes for one target table
- **RelationCacheEntry**: Caches schema info from RELATION messages

### Table State Machine

When adding a new table: PENDING → SNAPSHOT (copying data) → CATCHUP (applying buffered changes) → STREAMING (normal operation)

## GUC Parameters

```sql
ducklake_sync.poll_interval         -- ms between polls (default 1000)
ducklake_sync.batch_size_per_table  -- changes per table per flush
ducklake_sync.batch_size_per_group  -- total changes per group per round
ducklake_sync.enabled               -- enable/disable worker
```

## Test Files

Tests are in `test/sql/` with expected output in `test/expected/`:
- **api.sql**: Tests add_table, remove_table, group management
- **streaming.sql**: Tests INSERT/UPDATE/DELETE synchronization
- **snapshot_updates.sql**: Tests initial sync and concurrent updates

## PostgreSQL Internals Used

This extension heavily uses PostgreSQL logical replication internals from `replication/logicalproto.h`:
- `logicalrep_read_begin()`, `logicalrep_read_commit()`
- `logicalrep_read_rel()`, `logicalrep_read_insert()`, `logicalrep_read_update()`, `logicalrep_read_delete()`
- `LogicalRepRelation`, `LogicalRepTupleData`, `LogicalRepBeginData`, `LogicalRepCommitData`

Changes are fetched via SPI calling `pg_logical_slot_get_binary_changes()`.

## Requirements

- PostgreSQL 14+ (uses `logicalrep_read_*` API)
- pg_ducklake extension must be installed
- Source tables must have PRIMARY KEY
- PostgreSQL config: `wal_level=logical`, extension in `shared_preload_libraries`
