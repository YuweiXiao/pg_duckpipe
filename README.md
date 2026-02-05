# pg_ducklake_sync

PostgreSQL extension for HTAP (Hybrid Transactional/Analytical Processing) synchronization from heap tables to pg_ducklake columnar tables.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL                                                  │
│                                                              │
│  ┌─────────────┐     automatic      ┌──────────────────┐   │
│  │ Heap Tables │  ─────sync─────►   │ DuckLake Tables  │   │
│  │ (OLTP)      │     (CDC)          │ (OLAP)           │   │
│  └─────────────┘                    └──────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Write to heap tables (row store), query from DuckLake tables (column store).**

## Key Features

- **Production-ready CDC**: Uses pgoutput (PostgreSQL's production plugin)
- **Resource efficient**: Multiple tables share one publication/slot
- **Built-in parsing**: Reuses PostgreSQL's `logicalrep_read_*` functions
- **Flexible grouping**: Organize tables into sync groups as needed
- **Low OLTP overhead**: No triggers, async processing

## Quick Start

```sql
-- Install extensions
CREATE EXTENSION pg_ducklake;
CREATE EXTENSION pg_ducklake_sync;

-- Add tables to sync (all use default group = 1 slot)
SELECT ducklake_sync.add_table('public.orders');
SELECT ducklake_sync.add_table('public.customers');
SELECT ducklake_sync.add_table('public.products');

-- OLTP operations work normally
INSERT INTO orders (customer_id, amount) VALUES (1, 99.99);

-- Analytics on columnar storage
SELECT customer_id, sum(amount), count(*)
FROM ducklake.orders
GROUP BY customer_id;
```

## Resource Efficiency

Multiple tables share a single publication and replication slot:

| Tables | Publications | Slots |
|--------|--------------|-------|
| 10 | 1 | 1 |
| 50 | 1 | 1 |
| 100 (2 groups) | 2 | 2 |

Compare to naive approach: 100 tables = 100 publications + 100 slots (hits `max_replication_slots` limit).

## Sync Groups

Group tables to manage resources and isolation:

```sql
-- Default: all tables in one group
SELECT ducklake_sync.add_table('public.orders');
SELECT ducklake_sync.add_table('public.customers');

-- Create separate group for high-volume tables
SELECT ducklake_sync.create_group('analytics');
SELECT ducklake_sync.add_table('public.events', sync_group := 'analytics');
SELECT ducklake_sync.add_table('public.logs', sync_group := 'analytics');
```

## API Reference

```sql
-- Sync groups
ducklake_sync.create_group(name) → TEXT
ducklake_sync.drop_group(name)
ducklake_sync.enable_group(name)
ducklake_sync.disable_group(name)

-- Table management
ducklake_sync.add_table(source_table, [target_table], [sync_group])
ducklake_sync.remove_table(source_table)
ducklake_sync.move_table(source_table, new_group)
ducklake_sync.resync_table(source_table)

-- Monitoring
ducklake_sync.groups() → TABLE
ducklake_sync.tables() → TABLE
ducklake_sync.status() → TABLE
```

## Configuration

```sql
SET ducklake_sync.poll_interval = 1000;        -- ms
SET ducklake_sync.batch_size_per_table = 1000; -- fairness between tables
SET ducklake_sync.batch_size_per_group = 10000; -- fairness between groups
```

## Requirements

- PostgreSQL 14+
- pg_ducklake extension
- Source tables must have PRIMARY KEY

## Performance

| Metric | Typical Value |
|--------|---------------|
| Sync latency | 1-3 seconds |
| Throughput | 50,000-100,000 rows/sec |
| OLTP overhead | <1% |

## Documentation

See [DESIGN.md](DESIGN.md) for technical architecture and design decisions.

## License

Same as PostgreSQL (PostgreSQL License).
