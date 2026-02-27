# pg_duckpipe Performance Benchmark

This directory contains a benchmark suite for `pg_duckpipe` utilizing `sysbench` for workload generation.

## Files

-   `BENCHMARK_DESIGN.md`: Design document detailing objectives and methodology.
-   `bench.sh`: End-to-end one-shot runner: starts the DB then runs `run_sysbench.py`.
-   `run_sysbench.py`: Main Python script to run the benchmark using `sysbench`.
-   `start_db.sh`: Helper to start a temporary PostgreSQL instance with correct configuration.
-   `stop_db.sh`: Helper to stop the temporary instance.

## Prerequisites

-   `sysbench` installed (e.g., `brew install sysbench` or `apt install sysbench`).
-   PostgreSQL 14+ with `pg_duckdb` and `pg_duckpipe` extensions.

## Usage

### Quick start (recommended)

```bash
# One-shot: fresh DB + default oltp_insert benchmark (1 thread, 30s, 100k rows)
./benchmark/bench.sh

# Custom parameters
./benchmark/bench.sh --threads 4 --duration 60 --table-size 100000
```

### Manual steps

1.  **Start the Database**:
    ```bash
    ./start_db.sh
    ```
    This creates a new data directory `bench_data` and starts Postgres on port 5556.
    The benchmark config sets `duckpipe.flush_batch_threshold=10000` (flush batch size),
    `duckpipe.data_inlining_row_limit=1000`, and `duckpipe.poll_interval=10000`.

2.  **Run Benchmark**:
    ```bash
    # Run with default settings (append-only: oltp_insert)
    python3 run_sysbench.py --db-url "host=localhost port=5556 user=postgres dbname=postgres"
    
    # Run mixed OLTP workload (insert/update/delete)
    python3 run_sysbench.py \
        --workload oltp_read_write \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Customize parameters
    python3 run_sysbench.py \
        --workload oltp_read_write \
        --tables 4 \
        --table-size 50000 \
        --threads 8 \
        --duration 60 \
        --catchup-timeout 600 \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Force full row-by-row consistency check (more expensive)
    python3 run_sysbench.py \
        --consistency-mode full \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"

    # Run checks even when catch-up timeout is hit
    python3 run_sysbench.py \
        --verify-on-timeout \
        --db-url "host=localhost port=5556 user=postgres dbname=postgres"
    ```

3.  **Stop the Database**:
    ```bash
    ./stop_db.sh
    ```

## Methodology

1.  **Preparation**: Uses `sysbench <workload> prepare` to create and populate source tables (`sbtestN`).
2.  **Snapshot Benchmark**:
    -   Creates corresponding target tables in DuckDB (`ducklake.sbtestN`).
    -   Enables `pg_duckpipe` sync.
    -   Measures time to sync initial data.
3.  **Streaming Benchmark**:
    -   By default runs `sysbench oltp_insert run` for append-only traffic.
    -   Optionally runs `sysbench oltp_read_write run` for mixed INSERT/UPDATE/DELETE traffic.
    -   Consistency checks default to `safe` for append-only and `full` for mixed workload.
    -   Monitors replication lag (avg/peak MB during OLTP phase) and catch-up progress.

## Sysbench DML Behavior

This benchmark supports two workloads:

1.  `oltp_insert` (default, append-only)
    -   INSERT per transaction: `1`
    -   UPDATE per transaction: `0`
    -   DELETE per transaction: `0`
    -   Expected DuckPipe apply events per transaction: `1`

2.  `oltp_read_write` (mixed OLTP)
    -   sysbench defaults: `index_updates=1`, `non_index_updates=1`, `delete_inserts=1`
    -   INSERT per transaction: `1`
    -   UPDATE per transaction: `2`
    -   DELETE per transaction: `1`
    -   Expected DuckPipe apply events per transaction: `6`
      (UPDATE is decoded/applied as DELETE+INSERT in DuckPipe)

The wrapper uses these per-transaction event counts to estimate expected
`rows_synced` delta during catch-up.

## Consistency Modes

-   `auto` (default): `safe` for `oltp_insert`, `full` for `oltp_read_write`.
-   `safe`: lightweight check — verifies target row count matches expected (no value-level scan).
-   `full`: checks value-level parity and missing/extra keys (heavier SQL).
-   `off`: skip consistency checks.

By default, consistency checks are skipped when catch-up does not complete
before timeout. Use `--verify-on-timeout` to force checks in that case.

## Results

### Latest run — 2026-02-27

**Environment**: MacBook Pro M1, PostgreSQL 18.1
**Config** (`start_db.sh` defaults): `flush_batch_threshold=10000`, `data_inlining_row_limit=1000`, `poll_interval=10000`
**Invocation**: `./benchmark/bench.sh` (defaults: 1 thread, 30 s, 1 table × 100k rows, `oltp_insert`)

```
===========================================================
 Final Results
===========================================================
 Snapshot Throughput  : 41929 rows/sec
 OLTP Throughput      : 8827.55 TPS
 Avg Replication Lag  : 2.9 MB  [during OLTP]
 Peak Replication Lag : 4.2 MB  [during OLTP]
 Catch-up Time        : 2.2 sec
 Catch-up Throughput  : 0 rows/sec
 Consistency          : PASS
 Count Mismatches     : 0
===========================================================
```

**Notes:**

-   **Snapshot Throughput** includes SNAPSHOT copy + CATCHUP transition overhead, not just raw copy speed.
-   **Catch-up Throughput = 0** means DuckPipe drained the WAL backlog before the first 2 s monitoring poll; the actual flush rate for 10k-row batches is ~115k rows/sec (visible in `bench_pg.log`).
