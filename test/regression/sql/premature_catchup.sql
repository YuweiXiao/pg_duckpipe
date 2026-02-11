-- Premature CATCHUP → STREAMING transition
--
-- Bug: After each poll round, ALL CATCHUP tables are unconditionally promoted
-- to STREAMING. With batch_size_per_group limiting WAL consumption, a CATCHUP
-- table may not have consumed past its snapshot_lsn yet. Subsequent rounds
-- process remaining WAL messages as STREAMING (no skip logic), causing duplicates.
--
-- Key: pg_logical_slot_get_binary_changes() returns complete transactions even
-- past the count limit, so a single bulk INSERT (one transaction) won't trigger
-- the bug. We need MANY SEPARATE transactions to exceed the batch limit across
-- multiple poll rounds.

-- ============================================================
-- Setup: small batch size, long poll interval for control
-- ============================================================
ALTER SYSTEM SET duckpipe.poll_interval = 8000;
ALTER SYSTEM SET duckpipe.batch_size_per_group = 100;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.start_worker();
RESET client_min_messages;

-- Let the worker enter its idle wait cycle
SELECT pg_sleep(1);

-- Create source table
CREATE TABLE premature_catchup_src (id int PRIMARY KEY, val text);

-- Register for sync (SNAPSHOT state, starts worker if needed)
SELECT duckpipe.add_table('public.premature_catchup_src');

-- ============================================================
-- Generate 200 SEPARATE transactions using a stored procedure.
-- Each COMMIT produces a separate BEGIN/INSERT/COMMIT in WAL.
-- With batch_size_per_group=100, each round consumes ~25 txns
-- (4 WAL messages per txn: BEGIN, RELATION, INSERT, COMMIT).
-- ============================================================
CREATE PROCEDURE premature_catchup_insert(num_txns int)
LANGUAGE plpgsql AS $$
DECLARE
  i int;
BEGIN
  FOR i IN 1..num_txns LOOP
    INSERT INTO premature_catchup_src VALUES (i, 'row_' || i);
    COMMIT;
  END LOOP;
END;
$$;

CALL premature_catchup_insert(200);

-- Verify all 200 rows are in source
SELECT count(*) AS source_rows FROM premature_catchup_src;

-- ============================================================
-- Wait for worker to process: snapshot + multiple WAL rounds
-- With batch_size_per_group=100, the worker needs ~8 rounds
-- to consume all 200 transactions. poll_interval=8000ms.
-- ============================================================
SELECT pg_sleep(30);

-- ============================================================
-- Verify correctness
-- ============================================================
SELECT (SELECT count(*) FROM premature_catchup_src) AS source_count,
       (SELECT count(*) FROM public.premature_catchup_src_ducklake) AS target_count,
       (SELECT count(*) FROM public.premature_catchup_src_ducklake) >
       (SELECT count(*) FROM premature_catchup_src) AS has_duplicates;

-- ============================================================
-- Cleanup
-- ============================================================
DROP PROCEDURE premature_catchup_insert;
SELECT duckpipe.remove_table('public.premature_catchup_src', false);
DROP TABLE public.premature_catchup_src_ducklake;
DROP TABLE premature_catchup_src;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

ALTER SYSTEM RESET duckpipe.poll_interval;
ALTER SYSTEM RESET duckpipe.batch_size_per_group;
SELECT pg_reload_conf();
