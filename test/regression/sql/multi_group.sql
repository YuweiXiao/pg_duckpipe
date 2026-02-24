-- Test multiple independent sync groups
--
-- Two groups with separate publications and slots, each with their own tables.
-- Verifies groups are truly independent and don't interfere with each other.

SELECT duckpipe.start_worker();

-- Create a second group
SELECT duckpipe.create_group('group_b');

-- Create source tables
CREATE TABLE grp_a_t1 (id int primary key, val text);
CREATE TABLE grp_b_t1 (id int primary key, num int);

-- Add tables to different groups
SELECT duckpipe.add_table('public.grp_a_t1', NULL, 'default', false);
SELECT duckpipe.add_table('public.grp_b_t1', NULL, 'group_b', false);

-- Insert into both groups' tables
INSERT INTO grp_a_t1 VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO grp_b_t1 VALUES (10, 100), (20, 200);

SELECT pg_sleep(3);

-- Verify both groups synced independently
SELECT * FROM public.grp_a_t1_ducklake ORDER BY id;
SELECT * FROM public.grp_b_t1_ducklake ORDER BY id;

-- UPDATE in group A, DELETE in group B
UPDATE grp_a_t1 SET val = 'gamma' WHERE id = 1;
DELETE FROM grp_b_t1 WHERE id = 10;

SELECT pg_sleep(3);

SELECT * FROM public.grp_a_t1_ducklake ORDER BY id;
SELECT * FROM public.grp_b_t1_ducklake ORDER BY id;

-- Verify each table is in the correct group
SELECT m.source_table, g.name AS group_name
FROM duckpipe.table_mappings m
JOIN duckpipe.sync_groups g ON m.group_id = g.id
WHERE m.source_table IN ('grp_a_t1', 'grp_b_t1')
ORDER BY m.source_table;

-- Disable group_b — group A should keep syncing
SELECT duckpipe.disable_group('group_b');

-- Wait for the worker to complete any in-flight cycle before inserting
SELECT pg_sleep(2);

INSERT INTO grp_a_t1 VALUES (3, 'delta');
INSERT INTO grp_b_t1 VALUES (30, 300);

SELECT pg_sleep(3);

-- Group A change should appear, group B should NOT (disabled)
SELECT * FROM public.grp_a_t1_ducklake ORDER BY id;
SELECT count(*) AS grp_b_count FROM public.grp_b_t1_ducklake;

-- Re-enable group B
SELECT duckpipe.enable_group('group_b');

SELECT pg_sleep(3);

-- Group B should now have caught up (WAL was retained by slot)
SELECT * FROM public.grp_b_t1_ducklake ORDER BY id;

-- Cleanup: stop worker first so streaming connections are released
SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;

SELECT duckpipe.remove_table('public.grp_a_t1', false);
SELECT duckpipe.remove_table('public.grp_b_t1', false);
SELECT duckpipe.drop_group('group_b');

DROP TABLE public.grp_a_t1_ducklake;
DROP TABLE public.grp_b_t1_ducklake;
DROP TABLE grp_a_t1;
DROP TABLE grp_b_t1;
