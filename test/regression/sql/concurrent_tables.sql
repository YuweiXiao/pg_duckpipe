-- Test concurrent parallel flush across multiple tables
--
-- 5 tables each receive substantial data in single transactions,
-- guaranteeing all accumulate in queues and flush concurrently
-- in the same cycle via parallel_flush() with std::thread::scope.

ALTER SYSTEM SET duckpipe.poll_interval = 100;
SELECT pg_reload_conf();

SELECT duckpipe.start_worker();

-- Create 5 source tables with different schemas
CREATE TABLE conc_a (id int primary key, val text);
CREATE TABLE conc_b (id int primary key, num int);
CREATE TABLE conc_c (id int primary key, amount numeric(10,2));
CREATE TABLE conc_d (id int primary key, flag boolean, label text);
CREATE TABLE conc_e (id int primary key, ts timestamp, data text);

-- Add all to default group (copy_data=false -> STREAMING immediately)
SELECT duckpipe.add_table('public.conc_a', NULL, 'default', false);
SELECT duckpipe.add_table('public.conc_b', NULL, 'default', false);
SELECT duckpipe.add_table('public.conc_c', NULL, 'default', false);
SELECT duckpipe.add_table('public.conc_d', NULL, 'default', false);
SELECT duckpipe.add_table('public.conc_e', NULL, 'default', false);

-- Phase 1: Concurrent INSERT (single transaction -> same WAL batch -> same flush cycle)
BEGIN;
INSERT INTO conc_a SELECT g, 'a_' || g FROM generate_series(1, 100) g;
INSERT INTO conc_b SELECT g, g * 10 FROM generate_series(1, 100) g;
INSERT INTO conc_c SELECT g, g * 1.5 FROM generate_series(1, 100) g;
INSERT INTO conc_d SELECT g, g % 2 = 0, 'label_' || g FROM generate_series(1, 100) g;
INSERT INTO conc_e SELECT g, '2025-01-01'::timestamp + (g || ' hours')::interval, 'data_' || g FROM generate_series(1, 100) g;
COMMIT;

SELECT pg_sleep(5);

-- Verify all 5 targets have 100 rows each
SELECT count(*) AS conc_a_rows FROM public.conc_a_ducklake;
SELECT count(*) AS conc_b_rows FROM public.conc_b_ducklake;
SELECT count(*) AS conc_c_rows FROM public.conc_c_ducklake;
SELECT count(*) AS conc_d_rows FROM public.conc_d_ducklake;
SELECT count(*) AS conc_e_rows FROM public.conc_e_ducklake;

-- Spot-check data integrity
SELECT id, val FROM public.conc_a_ducklake WHERE id IN (1, 50, 100) ORDER BY id;
SELECT id, num FROM public.conc_b_ducklake WHERE id IN (1, 50, 100) ORDER BY id;

-- Phase 2: Concurrent UPDATE (single transaction)
BEGIN;
UPDATE conc_a SET val = 'updated_' || id WHERE id <= 50;
UPDATE conc_b SET num = num + 1 WHERE id <= 50;
UPDATE conc_c SET amount = amount * 2 WHERE id <= 50;
UPDATE conc_d SET flag = NOT flag WHERE id <= 50;
UPDATE conc_e SET data = 'modified_' || id WHERE id <= 50;
COMMIT;

SELECT pg_sleep(5);

-- Verify updates applied across all tables
SELECT count(*) AS updated_a FROM public.conc_a_ducklake WHERE val LIKE 'updated_%';
SELECT count(*) AS total_b FROM public.conc_b_ducklake;

-- Phase 3: Concurrent mixed operations (INSERT + UPDATE + DELETE in one txn)
BEGIN;
INSERT INTO conc_a SELECT g, 'new_' || g FROM generate_series(101, 150) g;
UPDATE conc_b SET num = -1 WHERE id = 1;
DELETE FROM conc_c WHERE id > 80;
INSERT INTO conc_d SELECT g, true, 'extra_' || g FROM generate_series(101, 120) g;
DELETE FROM conc_e WHERE id <= 10;
COMMIT;

SELECT pg_sleep(5);

-- Verify mixed results
SELECT count(*) AS conc_a_total FROM public.conc_a_ducklake;
SELECT num AS conc_b_id1_num FROM public.conc_b_ducklake WHERE id = 1;
SELECT count(*) AS conc_c_total FROM public.conc_c_ducklake;
SELECT count(*) AS conc_d_total FROM public.conc_d_ducklake;
SELECT count(*) AS conc_e_total FROM public.conc_e_ducklake;

-- Cleanup
SELECT duckpipe.remove_table('public.conc_a', false);
SELECT duckpipe.remove_table('public.conc_b', false);
SELECT duckpipe.remove_table('public.conc_c', false);
SELECT duckpipe.remove_table('public.conc_d', false);
SELECT duckpipe.remove_table('public.conc_e', false);

DROP TABLE public.conc_a_ducklake;
DROP TABLE public.conc_b_ducklake;
DROP TABLE public.conc_c_ducklake;
DROP TABLE public.conc_d_ducklake;
DROP TABLE public.conc_e_ducklake;

DROP TABLE conc_a;
DROP TABLE conc_b;
DROP TABLE conc_c;
DROP TABLE conc_d;
DROP TABLE conc_e;

ALTER SYSTEM RESET duckpipe.poll_interval;
SELECT pg_reload_conf();

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
