# pg_ducklake_sync: PostgreSQL HTAP Sync Extension

## Technical Design Document v3.0

### Date: 2026-02-04

---

## 1. Executive Summary

**pg_ducklake_sync** enables automatic synchronization from PostgreSQL heap tables (row store) to pg_ducklake DuckLake tables (column store), achieving HTAP within a single PostgreSQL instance.

### Design Philosophy: Simple AND Production-Ready

Key insight: PostgreSQL already provides **both** the production output plugin (pgoutput) **and** the parsing functions (`logicalrep_read_*`). We reuse both.

| Component | Our Approach |
|-----------|--------------|
| Output plugin | **pgoutput** (production, not test_decoding) |
| Protocol parsing | Reuse PostgreSQL's `logicalrep_read_*` functions |
| Data access | `pg_logical_slot_get_binary_changes()` via SPI |
| Execution | Single background worker with polling |

---

## 2. Why This Approach?

### Why NOT test_decoding?

test_decoding is explicitly documented as:
> "an **example** of a logical decoding output plugin. **It doesn't do anything especially useful**, but can serve as a starting point for developing your own output plugin."

It's not meant for production.

### Why pgoutput + built-in parsers?

| Aspect | pgoutput | test_decoding |
|--------|----------|---------------|
| Purpose | Production replication | Example/testing |
| Format | Efficient binary | Human-readable text |
| Parsing | Built-in `logicalrep_read_*` | Custom parser needed |
| Schema info | Rich (RELATION messages) | Basic (inline types) |
| Streaming | Full support | Limited |

**Key realization**: We don't need to implement binary parsing ourselves. PostgreSQL's `logicalrep_read_*` functions (in `proto.c`) do all the work.

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Instance                          │
│                                                                   │
│  ┌───────────────┐              ┌────────────────────────────┐   │
│  │  Heap Tables  │              │  DuckLake Tables           │   │
│  │  (OLTP)       │              │  (OLAP via pg_ducklake)    │   │
│  └───────┬───────┘              └─────────────▲──────────────┘   │
│          │                                    │                   │
│          │ WAL                                │ INSERT/DELETE     │
│          ▼                                    │                   │
│  ┌─────────────────────┐                     │                   │
│  │ Logical Decoding    │                     │                   │
│  │ Slot (pgoutput)     │                     │                   │
│  └──────────┬──────────┘                     │                   │
│             │                                 │                   │
│             │ Binary (via SPI)                │                   │
│             ▼                                 │                   │
│  ┌───────────────────────────────────────────┴──────────────────┐│
│  │                   Background Worker                           ││
│  │                                                               ││
│  │  1. SPI: pg_logical_slot_get_binary_changes()                ││
│  │  2. Parse: logicalrep_read_insert/update/delete (built-in!)  ││
│  │  3. Batch: Accumulate changes in memory                      ││
│  │  4. Apply: INSERT/DELETE to DuckLake via SPI                 ││
│  │                                                               ││
│  └───────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Protocol Message Flow

pgoutput sends these message types (single-byte identifiers):

```
'B' = BEGIN        - Transaction start
'R' = RELATION     - Table schema definition (cached)
'I' = INSERT       - Row insert
'U' = UPDATE       - Row update (old key + new tuple)
'D' = DELETE       - Row delete (old key)
'T' = TRUNCATE     - Table truncate
'C' = COMMIT       - Transaction commit with LSN
```

Our worker processes them:

```c
while ((msg = get_next_message())) {
    switch (msg->type) {
        case 'B':
            logicalrep_read_begin(&buf, &begin_data);
            break;
        case 'R':
            rel = logicalrep_read_rel(&buf);  // Cache schema
            cache_relation(rel);
            break;
        case 'I':
            relid = logicalrep_read_insert(&buf, &newtup);
            batch_insert(relid, &newtup);
            break;
        case 'U':
            relid = logicalrep_read_update(&buf, &has_old, &oldtup, &newtup);
            batch_delete(relid, &oldtup);  // Delete old
            batch_insert(relid, &newtup);  // Insert new
            break;
        case 'D':
            relid = logicalrep_read_delete(&buf, &oldtup);
            batch_delete(relid, &oldtup);
            break;
        case 'C':
            logicalrep_read_commit(&buf, &commit_data);
            flush_batch();  // Apply all accumulated changes
            update_checkpoint(commit_data.end_lsn);
            break;
    }
}
```

---

## 5. Core Components

### 5.1 Metadata Schema

**Key insight**: Separate "sync groups" (publication + slot) from "table mappings".

```sql
CREATE SCHEMA ducklake_sync;

-- Sync groups: each group = 1 publication + 1 replication slot
-- Multiple tables can share one group (resource efficient)
CREATE TABLE ducklake_sync.sync_groups (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    publication     TEXT NOT NULL UNIQUE,    -- PostgreSQL publication name
    slot_name       TEXT NOT NULL UNIQUE,    -- Replication slot name
    enabled         BOOLEAN DEFAULT true,
    confirmed_lsn   PG_LSN,                  -- Checkpoint for this group
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Table mappings: which source tables sync to which target tables
-- Many tables can belong to one sync group
CREATE TABLE ducklake_sync.table_mappings (
    id              SERIAL PRIMARY KEY,
    group_id        INTEGER NOT NULL REFERENCES ducklake_sync.sync_groups(id),
    source_schema   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,

    -- Sync state machine
    state           TEXT NOT NULL DEFAULT 'PENDING',
                    -- PENDING: registered, not started
                    -- SNAPSHOT: copying initial data
                    -- CATCHUP: applying buffered + new changes
                    -- STREAMING: normal operation
    snapshot_lsn    PG_LSN,          -- LSN when snapshot started (for CATCHUP)

    enabled         BOOLEAN DEFAULT true,
    rows_synced     BIGINT DEFAULT 0,
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(source_schema, source_table)      -- Each source table synced once
);

-- Cache relation schemas from RELATION messages (per group)
CREATE TABLE ducklake_sync.relation_cache (
    group_id        INTEGER REFERENCES ducklake_sync.sync_groups(id),
    remote_relid    INTEGER,                 -- pgoutput relation ID
    source_schema   TEXT,
    source_table    TEXT,
    columns         JSONB,                   -- [{name, type_oid, is_key}]
    PRIMARY KEY (group_id, remote_relid)
);

-- Default sync group (created on extension install)
INSERT INTO ducklake_sync.sync_groups (name, publication, slot_name)
VALUES ('default', 'ducklake_sync_pub', 'ducklake_sync_slot');
```

**Resource usage comparison**:

| Tables | Old Design | New Design |
|--------|------------|------------|
| 10 | 10 pubs, 10 slots | 1 pub, 1 slot |
| 100 | 100 pubs, 100 slots | 1 pub, 1 slot |
| 100 (grouped) | N/A | 5 pubs, 5 slots |

### 5.2 Background Worker

**Multi-table aware**: One slot returns changes for multiple tables. Worker routes to correct targets.

```c
void ducklake_sync_worker_main(Datum arg) {
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    while (!shutdown_requested) {
        bool any_work_done = false;

        StartTransactionCommand();

        /* Process each enabled sync group */
        foreach(group, get_enabled_sync_groups()) {
            int processed = process_sync_group(group);
            if (processed > 0)
                any_work_done = true;
        }

        CommitTransactionCommand();

        if (!any_work_done) {
            WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT,
                      poll_interval_ms, WAIT_EVENT_EXTENSION);
            ResetLatch(MyLatch);
        }
    }
}

/*
 * Process one sync group (one publication, one slot, multiple tables)
 */
int process_sync_group(SyncGroup *group) {
    HTAB *batches;  /* Hash: source_relid -> SyncBatch */
    int total_processed = 0;

    /* Get changes from slot (may contain multiple tables) */
    changes = pg_logical_slot_get_binary_changes(
        group->slot_name, NULL, batch_size,
        'publication_names', group->publication
    );

    /* Route each change to appropriate batch by table */
    foreach(change, changes) {
        char msgtype = get_message_type(change);

        if (msgtype == 'R') {  /* RELATION message */
            LogicalRepRelation *rel = logicalrep_read_rel(&buf);
            cache_relation(group, rel);  /* Remember relid -> table mapping */
        }
        else if (msgtype == 'I' || msgtype == 'U' || msgtype == 'D') {
            LogicalRepRelId relid = parse_change(&buf, &tuple_data);

            /* Look up which target table this source maps to */
            TableMapping *mapping = get_table_mapping(group, relid);
            if (mapping == NULL || !mapping->enabled)
                continue;  /* Table removed or disabled, skip */

            /* Get or create batch for this table */
            SyncBatch *batch = get_or_create_batch(batches, mapping);
            add_to_batch(batch, msgtype, &tuple_data);
            total_processed++;

            /* Flush individual table if its batch is full */
            if (batch->count >= batch_size_per_table) {
                flush_batch(batch);
                batch->count = 0;
            }
        }
        else if (msgtype == 'C') {  /* COMMIT */
            LogicalRepCommitData commit;
            logicalrep_read_commit(&buf, &commit);
            group->pending_lsn = commit.end_lsn;
        }
    }

    /* Flush all remaining batches */
    flush_all_batches(batches);
    update_checkpoint(group, group->pending_lsn);

    return total_processed;
}
```

void process_subscription(Subscription *sub) {
    StringInfoData buf;

    /* Get binary changes via SPI */
    SPI_connect();

    int ret = SPI_execute_with_args(
        "SELECT lsn, data FROM pg_logical_slot_get_binary_changes($1, NULL, $2, "
        "'proto_version', '4', 'publication_names', $3)",
        3, argtypes, values, nulls, true, 0);

    /* Process each change */
    for (int i = 0; i < SPI_processed; i++) {
        bytea *data = DatumGetByteaP(SPI_getbinval(...));
        initStringInfo(&buf);
        appendBinaryStringInfo(&buf, VARDATA(data), VARSIZE(data) - VARHDRSZ);

        char msgtype = pq_getmsgbyte(&buf);
        process_message(sub, msgtype, &buf);
    }

    /* Flush and checkpoint */
    flush_batch(sub);

    SPI_finish();
}
```

### 5.3 Reusing PostgreSQL's Parsers

The key insight: we directly call PostgreSQL's internal functions:

```c
#include "replication/logicalproto.h"

void process_message(Subscription *sub, char type, StringInfo buf) {
    switch (type) {
        case LOGICAL_REP_MSG_BEGIN:
            {
                LogicalRepBeginData begin_data;
                logicalrep_read_begin(buf, &begin_data);  /* Built-in! */
                sub->current_xid = begin_data.xid;
            }
            break;

        case LOGICAL_REP_MSG_RELATION:
            {
                LogicalRepRelation *rel;
                rel = logicalrep_read_rel(buf);           /* Built-in! */
                cache_relation(sub, rel);
            }
            break;

        case LOGICAL_REP_MSG_INSERT:
            {
                LogicalRepTupleData newtup;
                LogicalRepRelId relid;
                relid = logicalrep_read_insert(buf, &newtup);  /* Built-in! */
                batch_add_insert(sub, relid, &newtup);
            }
            break;

        case LOGICAL_REP_MSG_UPDATE:
            {
                LogicalRepTupleData oldtup, newtup;
                bool has_oldtup;
                LogicalRepRelId relid;
                relid = logicalrep_read_update(buf, &has_oldtup,
                                               &oldtup, &newtup);  /* Built-in! */
                if (has_oldtup)
                    batch_add_delete(sub, relid, &oldtup);
                batch_add_insert(sub, relid, &newtup);
            }
            break;

        case LOGICAL_REP_MSG_DELETE:
            {
                LogicalRepTupleData oldtup;
                LogicalRepRelId relid;
                relid = logicalrep_read_delete(buf, &oldtup);  /* Built-in! */
                batch_add_delete(sub, relid, &oldtup);
            }
            break;

        case LOGICAL_REP_MSG_COMMIT:
            {
                LogicalRepCommitData commit_data;
                logicalrep_read_commit(buf, &commit_data);  /* Built-in! */
                sub->pending_lsn = commit_data.end_lsn;
            }
            break;
    }
}
```

---

## 6. API

### 6.1 Sync Group Management

```sql
-- Create a new sync group (publication + slot)
-- Use this to isolate tables or manage resources
ducklake_sync.create_group(
    name TEXT,                       -- group name
    publication TEXT DEFAULT NULL,   -- defaults to 'ducklake_sync_pub_{name}'
    slot_name TEXT DEFAULT NULL      -- defaults to 'ducklake_sync_slot_{name}'
) RETURNS TEXT

-- Drop a sync group (must have no tables)
ducklake_sync.drop_group(
    name TEXT,
    drop_slot BOOLEAN DEFAULT true
) RETURNS void

-- Enable/disable a sync group
ducklake_sync.enable_group(name TEXT) RETURNS void
ducklake_sync.disable_group(name TEXT) RETURNS void
```

### 6.2 Table Sync Management

```sql
-- Add a table to sync (uses 'default' group)
ducklake_sync.add_table(
    source_table TEXT,               -- 'schema.table'
    target_table TEXT DEFAULT NULL,  -- defaults to 'ducklake.{table}'
    sync_group TEXT DEFAULT 'default',
    copy_data BOOLEAN DEFAULT true   -- copy existing data
) RETURNS void

-- Remove a table from sync
ducklake_sync.remove_table(
    source_table TEXT,
    drop_target BOOLEAN DEFAULT false
) RETURNS void

-- Move a table to different sync group
ducklake_sync.move_table(
    source_table TEXT,
    new_group TEXT
) RETURNS void

-- Force full resync of a table
ducklake_sync.resync_table(source_table TEXT) RETURNS void
```

### 6.3 Monitoring

```sql
-- View all sync groups
ducklake_sync.groups() RETURNS TABLE(
    name TEXT,
    publication TEXT,
    slot_name TEXT,
    enabled BOOLEAN,
    table_count INTEGER,
    lag_bytes BIGINT,
    last_sync TIMESTAMPTZ
)

-- View all synced tables
ducklake_sync.tables() RETURNS TABLE(
    source_table TEXT,
    target_table TEXT,
    sync_group TEXT,
    enabled BOOLEAN,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)

-- Detailed status
ducklake_sync.status() RETURNS TABLE(
    sync_group TEXT,
    source_table TEXT,
    target_table TEXT,
    enabled BOOLEAN,
    lag_bytes BIGINT,
    rows_synced BIGINT,
    last_sync TIMESTAMPTZ
)
```

### 6.4 Configuration (GUC)

```sql
ducklake_sync.poll_interval = 1000       -- ms between polls
ducklake_sync.batch_size_per_table = 1000 -- changes per table per round
ducklake_sync.enabled = on               -- enable/disable worker
```

---

## 7. Table Sync Flow

### 7.1 Adding First Table (Default Group)

```sql
SELECT ducklake_sync.add_table('public.orders');
```

Internally:

```sql
-- 1. Create default publication if not exists
CREATE PUBLICATION ducklake_sync_pub FOR TABLE public.orders;

-- 2. Create replication slot if not exists
SELECT pg_create_logical_replication_slot('ducklake_sync_slot', 'pgoutput');

-- 3. Create target DuckLake table
CREATE TABLE ducklake.orders (...) USING ducklake;

-- 4. Copy existing data
INSERT INTO ducklake.orders SELECT * FROM public.orders;

-- 5. Record table mapping
INSERT INTO ducklake_sync.table_mappings (...) VALUES (...);
```

### 7.2 Adding More Tables to Same Group

```sql
SELECT ducklake_sync.add_table('public.customers');
SELECT ducklake_sync.add_table('public.products');
```

Internally:

```sql
-- Just add table to existing publication (no new slot!)
ALTER PUBLICATION ducklake_sync_pub ADD TABLE public.customers;
ALTER PUBLICATION ducklake_sync_pub ADD TABLE public.products;

-- Create target tables and copy data...
-- Record mappings...
```

### 7.3 Creating Separate Groups (Advanced)

```sql
-- Create a separate group for high-volume tables
SELECT ducklake_sync.create_group('high_volume');

-- Add tables to this group
SELECT ducklake_sync.add_table('public.events', sync_group := 'high_volume');
SELECT ducklake_sync.add_table('public.logs', sync_group := 'high_volume');
```

This creates a separate publication and slot for isolation.

### 7.4 Resource Usage Examples

| Scenario | Groups | Publications | Slots |
|----------|--------|--------------|-------|
| 50 tables, all default | 1 | 1 | 1 |
| 50 tables, 2 groups | 2 | 2 | 2 |
| 100 tables, by schema | 5 | 5 | 5 |

---

## 8. Change Application

### 8.1 Batch Accumulator

```c
typedef struct {
    Oid         target_relid;       /* DuckLake table OID */
    List       *inserts;            /* List of tuples to insert */
    List       *delete_keys;        /* List of PK values to delete */
    XLogRecPtr  last_lsn;           /* LSN of last change in batch */
} SyncBatch;
```

### 8.2 Flush to DuckLake

```sql
-- Within transaction:
BEGIN;

-- Delete first (handles UPDATE = DELETE + INSERT)
DELETE FROM ducklake.orders
WHERE id IN (SELECT unnest($1::int[]));

-- Then insert
INSERT INTO ducklake.orders (id, customer_id, amount, created_at)
SELECT * FROM unnest($2::int[], $3::int[], $4::numeric[], $5::timestamptz[]);

-- Update checkpoint
UPDATE ducklake_sync.subscriptions
SET confirmed_lsn = $6, last_sync_at = now(), rows_synced = rows_synced + $7
WHERE id = $8;

COMMIT;
```

---

## 9. Performance Characteristics

### 9.1 Overhead

| Aspect | Impact |
|--------|--------|
| Source table | Zero (no triggers, WAL already written) |
| WAL retention | Grows until consumed |
| CPU (parsing) | Low (efficient binary format) |
| Memory | batch_size * avg_row_size |

### 9.2 Throughput

Expected with pgoutput + built-in parsers:
- Simple rows: **50,000-100,000 changes/second**
- Complex rows: **10,000-30,000 changes/second**

This is significantly better than test_decoding text parsing.

### 9.3 Latency

| Setting | Typical Latency |
|---------|-----------------|
| poll_interval=1000ms | 1-3 seconds |
| poll_interval=100ms | 100-500ms |

---

## 10. Multi-Table Sync & Fairness

### 10.1 Two Levels of Grouping

```
┌─────────────────────────────────────────────────────────────────┐
│  Sync Group: "default"                                          │
│  (1 publication, 1 slot)                                        │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ orders   │  │ customers│  │ products │  │ payments │        │
│  │ 1M chg   │  │ 100 chg  │  │ 500 chg  │  │ 10K chg  │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Sync Group: "high_volume"                                      │
│  (1 publication, 1 slot)                                        │
│                                                                  │
│  ┌──────────┐  ┌──────────┐                                     │
│  │ events   │  │ logs     │                                     │
│  │ 10M chg  │  │ 5M chg   │                                     │
│  └──────────┘  └──────────┘                                     │
└─────────────────────────────────────────────────────────────────┘
```

### 10.2 Fairness Within a Sync Group

Changes from one slot are interleaved (WAL order). We batch by table and flush when:
1. Table batch reaches `batch_size_per_table` (default 1000)
2. Transaction commits
3. End of poll batch

```
WAL order: [orders INSERT] [customers INSERT] [orders INSERT] [orders UPDATE]...

Processing:
  orders batch:    [INSERT, INSERT, UPDATE, ...]  → flush when full
  customers batch: [INSERT, ...]                   → flush at commit
```

**Result**: All tables in a group make progress together (WAL-order fairness).

### 10.3 Fairness Across Sync Groups

Round-robin between groups:

```
Round 1:
  Group "default":     process up to batch_size changes
  Group "high_volume": process up to batch_size changes

Round 2:
  Group "default":     process up to batch_size changes
  Group "high_volume": process up to batch_size changes
```

### 10.4 Configuration

```sql
-- Changes per table before flush (intra-group fairness)
ducklake_sync.batch_size_per_table = 1000

-- Total changes per group per round (inter-group fairness)
ducklake_sync.batch_size_per_group = 10000

-- Poll interval when all caught up
ducklake_sync.poll_interval = 1000  -- ms
```

### 10.5 When to Use Multiple Groups

| Scenario | Recommendation |
|----------|----------------|
| < 50 tables, similar volume | Single "default" group |
| High-volume tables | Separate group for isolation |
| Different latency requirements | Separate groups |
| Resource limits (slots) | Minimize groups |

### 10.6 Adding New Table to Existing Group (Initial Sync)

This is the most complex workflow. We must handle:
- Existing tables continue streaming (no pause)
- New table needs full data copy
- Changes to new table during copy must not be lost

#### Per-Table State Machine

```
                   add_table()
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      PENDING                                 │
│  Table registered, not yet added to publication             │
└──────────────────────────┬──────────────────────────────────┘
                           │ start snapshot
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      SNAPSHOT                                │
│  Copying existing data with consistent snapshot             │
│  Changes buffered to staging table                          │
│  snapshot_lsn recorded                                       │
└──────────────────────────┬──────────────────────────────────┘
                           │ copy complete
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      CATCHUP                                 │
│  Applying buffered changes from staging table               │
│  + applying new changes where lsn > snapshot_lsn            │
└──────────────────────────┬──────────────────────────────────┘
                           │ caught up to current
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      STREAMING                               │
│  Normal operation - apply changes as they arrive            │
└─────────────────────────────────────────────────────────────┘
```

#### Detailed Workflow

**Step 1: Register table and record snapshot LSN**

```sql
SELECT ducklake_sync.add_table('public.products');
```

```c
void add_table(const char *source_table, const char *group_name) {
    /* Start REPEATABLE READ transaction for consistent snapshot */
    StartTransactionCommand();
    SetTransactionSnapshot(SNAPSHOT_REPEATABLE_READ);

    /* Record current slot LSN - this is our snapshot point */
    XLogRecPtr snapshot_lsn = get_current_slot_lsn(group->slot_name);

    /* Add table to publication */
    exec_sql("ALTER PUBLICATION %s ADD TABLE %s",
             group->publication, source_table);

    /* Create target DuckLake table */
    create_ducklake_table(target_table, source_table);

    /* Create staging table for buffering changes during snapshot */
    exec_sql("CREATE TABLE ducklake_sync.staging_%s ("
             "  lsn pg_lsn, op char, data jsonb"
             ")", table_id);

    /* Record table mapping with state */
    exec_sql("INSERT INTO ducklake_sync.table_mappings "
             "(group_id, source_table, target_table, state, snapshot_lsn) "
             "VALUES ($1, $2, $3, 'SNAPSHOT', $4)",
             group_id, source_table, target_table, snapshot_lsn);

    /* Copy existing data using our snapshot */
    copy_table_data(source_table, target_table);  /* chunked */

    CommitTransactionCommand();

    /* Mark ready for catchup */
    exec_sql("UPDATE ducklake_sync.table_mappings "
             "SET state = 'CATCHUP' WHERE source_table = $1", source_table);
}
```

**Step 2: Worker handles mixed states**

```c
void process_sync_group(SyncGroup *group) {
    /* Get changes from slot */
    changes = pg_logical_slot_get_binary_changes(...);

    foreach(change, changes) {
        char msgtype = get_message_type(change);
        LogicalRepRelId relid = get_relid(change);

        /* Look up table mapping and state */
        TableMapping *mapping = get_table_mapping(group, relid);
        if (!mapping) continue;

        switch (mapping->state) {
            case STATE_STREAMING:
                /* Normal: apply change directly */
                apply_change(mapping, change);
                break;

            case STATE_SNAPSHOT:
                /* Buffer change to staging table - don't apply yet */
                buffer_to_staging(mapping, change);
                break;

            case STATE_CATCHUP:
                /* Apply if lsn > snapshot_lsn (change after snapshot) */
                if (change->lsn > mapping->snapshot_lsn)
                    apply_change(mapping, change);
                /* else: already included in snapshot copy, skip */
                break;

            case STATE_PENDING:
                /* Not in publication yet, shouldn't receive changes */
                break;
        }
    }
}
```

**Step 3: Complete catchup and transition to streaming**

```c
void check_catchup_complete(TableMapping *mapping) {
    if (mapping->state != STATE_CATCHUP)
        return;

    /* Apply any remaining buffered changes */
    apply_staging_table(mapping);

    /* Drop staging table */
    exec_sql("DROP TABLE ducklake_sync.staging_%s", mapping->id);

    /* Transition to streaming */
    exec_sql("UPDATE ducklake_sync.table_mappings "
             "SET state = 'STREAMING' WHERE id = $1", mapping->id);
}
```

#### Timeline Visualization

```
Time ──────────────────────────────────────────────────────────────►

Slot LSN:     1000      1050      1100      1150      1200

              │         │         │         │         │
orders:       │ ●─────────●─────────●─────────●─────────●  (STREAMING)
              │ apply   apply     apply     apply     apply
              │
customers:    │ ●─────────●─────────●─────────●─────────●  (STREAMING)
              │ apply   apply     apply     apply     apply
              │
products:     │         │         │         │         │
              │ add_table()       │         │         │
              │ snapshot_lsn=1000 │         │         │
              │         │         │         │         │
              │ ┌───────┴─────────┴───┐     │         │
              │ │    SNAPSHOT         │     │         │
              │ │ copy 1M rows        │     │         │
              │ │ buffer changes      │     │         │
              │ └─────────────────────┘     │         │
              │                       │     │         │
              │                  CATCHUP    │         │
              │                  apply staged         │
              │                  skip lsn<=1000       │
              │                  apply lsn>1000       │
              │                             │         │
              │                        STREAMING ─────●
              │                             apply     apply
```

#### Key Invariants

| Invariant | How Maintained |
|-----------|----------------|
| No data loss | Buffer changes during SNAPSHOT, apply in CATCHUP |
| No duplicates | Skip changes where lsn <= snapshot_lsn |
| Other tables not blocked | Process all changes, route by table state |
| Consistent snapshot | REPEATABLE READ transaction for copy |

#### Staging Table Schema

```sql
CREATE TABLE ducklake_sync.staging_{table_id} (
    seq         SERIAL,           -- ordering
    lsn         PG_LSN NOT NULL,  -- for debugging
    op          CHAR(1),          -- 'I', 'U', 'D'
    old_data    JSONB,            -- for UPDATE/DELETE (key columns)
    new_data    JSONB             -- for INSERT/UPDATE (all columns)
);
```

#### Resource Considerations

| Resource | During SNAPSHOT | After STREAMING |
|----------|-----------------|-----------------|
| Staging table | Created, receives changes | Dropped |
| Memory | Normal batching | Normal batching |
| Disk | Staging table size | None |
| WAL | No extra retention | Normal |

Staging table size estimate: `changes_per_second × snapshot_duration × avg_row_size`

Example: 1000 changes/sec × 60 sec × 500 bytes = 30 MB (acceptable)

### 10.7 Monitoring

```sql
-- Group-level status
SELECT * FROM ducklake_sync.groups();
  name        | table_count | lag_bytes | last_sync
--------------+-------------+-----------+------------
 default      | 25          | 50KB      | 1s ago
 high_volume  | 2           | 5MB       | 3s ago

-- Table-level status
SELECT * FROM ducklake_sync.tables();
  source_table    | sync_group   | rows_synced | last_sync
------------------+--------------+-------------+-----------
 public.orders    | default      | 1000000     | 1s ago
 public.customers | default      | 50000       | 1s ago
 public.events    | high_volume  | 10000000    | 3s ago
```

---

## 11. Limitations (V1)

1. **Primary key required** on source tables
2. **No DDL sync** - schema changes need manual intervention
3. **No row filtering** - all rows synced
4. **No transformation** - columns copied as-is
5. **Single worker** - one background worker handles all subscriptions
6. **Eventual consistency** - configurable latency (1-5 seconds typical)

---

## 12. Comparison

### vs. External ETL

| Aspect | pg_ducklake_sync | External ETL |
|--------|------------------|--------------|
| Deployment | Extension only | Separate service |
| Protocol | Reuses PG internals | Implements from scratch |
| Latency | Lower (in-process) | Higher (network) |
| Scalability | Single node | Distributed |

### vs. test_decoding approach (v2 design)

| Aspect | pgoutput (v3) | test_decoding (v2) |
|--------|---------------|-------------------|
| Production ready | Yes | No (example only) |
| Parsing | Built-in functions | Custom parser |
| Efficiency | Binary format | Text format |
| Throughput | 50-100K/s | 10-50K/s |
| Code to write | ~600 lines | ~800 lines |

---

## 13. Implementation Plan

### Phase 1: Foundation (3 days)
- Extension skeleton
- Metadata schema
- Background worker registration
- GUC parameters

### Phase 2: Core Logic (4 days)
- SPI calls to pg_logical_slot_get_binary_changes
- Message dispatch using logicalrep_read_* functions
- Relation cache management
- Batch accumulator

### Phase 3: Change Application (3 days)
- SQL generation for DuckLake operations
- Bulk INSERT/DELETE
- LSN checkpoint

### Phase 4: API & Polish (3 days)
- SQL function API
- Initial copy logic
- Status views
- Error handling
- Tests

**Total: ~2 weeks**

---

## 14. File Structure

```
pg_ducklake_sync/
├── DESIGN.md
├── README.md
├── Makefile
├── pg_ducklake_sync.control
├── sql/
│   └── pg_ducklake_sync--1.0.sql
├── src/
│   ├── pg_ducklake_sync.c      # Entry point, GUCs
│   ├── worker.c                # Background worker main loop
│   ├── decoder.c               # Message dispatch (calls logicalrep_read_*)
│   ├── batch.c                 # Batch accumulator
│   ├── apply.c                 # Apply changes to DuckLake
│   └── api.c                   # SQL function implementations
└── test/
    └── sql/
```

---

## 15. Example Usage

### 15.1 Basic Usage (Single Default Group)

```sql
-- Setup
CREATE EXTENSION pg_ducklake;
CREATE EXTENSION pg_ducklake_sync;

-- Create OLTP tables
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, amount NUMERIC);
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT, email TEXT);
CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price NUMERIC);

-- Add all tables to sync (uses default group - 1 publication, 1 slot)
SELECT ducklake_sync.add_table('public.orders');
SELECT ducklake_sync.add_table('public.customers');
SELECT ducklake_sync.add_table('public.products');

-- OLTP writes
INSERT INTO orders (customer_id, amount) VALUES (1, 99.99);
INSERT INTO customers (name, email) VALUES ('Alice', 'alice@example.com');

-- OLAP reads (after sync)
SELECT c.name, SUM(o.amount)
FROM ducklake.orders o
JOIN ducklake.customers c ON o.customer_id = c.id
GROUP BY c.name;

-- Monitor
SELECT * FROM ducklake_sync.groups();   -- 1 group
SELECT * FROM ducklake_sync.tables();   -- 3 tables
```

### 15.2 Advanced Usage (Multiple Groups)

```sql
-- Create separate group for high-volume tables
SELECT ducklake_sync.create_group('analytics');

-- Add high-volume tables to separate group
SELECT ducklake_sync.add_table('public.events', sync_group := 'analytics');
SELECT ducklake_sync.add_table('public.page_views', sync_group := 'analytics');

-- Regular tables use default group
SELECT ducklake_sync.add_table('public.users');
SELECT ducklake_sync.add_table('public.settings');

-- Check resource usage
SELECT * FROM ducklake_sync.groups();
  name      | publication              | slot_name              | table_count
------------+--------------------------+------------------------+------------
 default    | ducklake_sync_pub        | ducklake_sync_slot     | 2
 analytics  | ducklake_sync_pub_analytics | ducklake_sync_slot_analytics | 2

-- Total: 2 publications, 2 slots (not 4!)
```

### 15.3 Moving Tables Between Groups

```sql
-- events table is too noisy, move to separate group
SELECT ducklake_sync.create_group('noisy');
SELECT ducklake_sync.move_table('public.events', 'noisy');
```

---

## 16. Key Code References

PostgreSQL functions we reuse (from `src/backend/replication/logical/proto.c`):

| Function | Purpose |
|----------|---------|
| `logicalrep_read_begin()` | Parse BEGIN message |
| `logicalrep_read_commit()` | Parse COMMIT message |
| `logicalrep_read_rel()` | Parse RELATION message (schema) |
| `logicalrep_read_insert()` | Parse INSERT message |
| `logicalrep_read_update()` | Parse UPDATE message |
| `logicalrep_read_delete()` | Parse DELETE message |
| `logicalrep_read_truncate()` | Parse TRUNCATE message |

Data structures (from `src/include/replication/logicalproto.h`):

| Structure | Purpose |
|-----------|---------|
| `LogicalRepRelation` | Table schema from RELATION message |
| `LogicalRepTupleData` | Row data from INSERT/UPDATE/DELETE |
| `LogicalRepBeginData` | Transaction info from BEGIN |
| `LogicalRepCommitData` | Commit info including LSN |

---

## Appendix A: Design Review

### Complexity Assessment

| Component | Lines | Complexity |
|-----------|-------|------------|
| Worker loop | ~150 | Low |
| Message dispatch | ~100 | Low (switch + function calls) |
| Batch accumulator | ~100 | Low |
| Apply logic | ~150 | Low |
| API functions | ~100 | Low |
| **Total** | **~600** | **Low** |

**Simpler than v2** because we reuse PostgreSQL's parsing code instead of writing our own.

### Production Readiness

| Criterion | Status |
|-----------|--------|
| Output plugin | pgoutput (production) |
| Protocol parsing | PostgreSQL built-in |
| Error handling | Robust (retry + checkpoint) |
| Monitoring | Status function + logs |

### Risk Analysis

| Risk | Mitigation |
|------|------------|
| Binary format changes | Protocol is versioned, request specific version |
| logicalrep_read_* API changes | Internal API but stable, test on upgrade |
| WAL bloat | Monitor slot lag, max_slot_wal_keep_size |

---

## Appendix B: Alternatives Rejected

### test_decoding (v2 design)
- **Rejected**: Documentation explicitly says "example only"
- Was chosen for simplicity but not production-appropriate

### Custom output plugin
- **Rejected**: More work, pgoutput already does what we need

### Streaming protocol (not polling)
- **Deferred to V2**: Polling is simpler, adequate for most workloads
- Streaming would reduce latency but adds complexity
