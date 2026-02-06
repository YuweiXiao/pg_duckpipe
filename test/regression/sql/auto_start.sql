-- Test auto-start: add_table() should start background worker automatically
CREATE EXTENSION pg_duckpipe CASCADE;

-- Verify no worker is running yet
SELECT count(*) AS workers_before FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe';

-- Create source and target tables
CREATE TABLE auto_test (id int primary key, val text);
CREATE TABLE ducklake.auto_test (id int, val text) USING ducklake;

-- add_table should auto-start the worker (no start_worker() call)
SELECT duckpipe.add_table('public.auto_test', 'ducklake.auto_test', 'default', false);

-- Give the worker time to register in pg_stat_activity
SELECT pg_sleep(1);

-- Verify worker is now running
SELECT count(*) AS workers_after FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe';

-- Verify start_worker() reports already running
SELECT duckpipe.start_worker();

-- Adding a second table should NOT start another worker
CREATE TABLE auto_test2 (id int primary key, val text);
CREATE TABLE ducklake.auto_test2 (id int, val text) USING ducklake;
SELECT duckpipe.add_table('public.auto_test2', 'ducklake.auto_test2', 'default', false);

-- Still exactly one worker
SELECT count(*) AS workers_still_one FROM pg_stat_activity WHERE backend_type = 'pg_duckpipe';

-- Cleanup
SELECT duckpipe.remove_table('public.auto_test', false);
SELECT duckpipe.remove_table('public.auto_test2', false);
DROP TABLE ducklake.auto_test;
DROP TABLE ducklake.auto_test2;
DROP TABLE auto_test;
DROP TABLE auto_test2;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;
