-- Test OID-based WAL routing after table rename
--
-- After ALTER TABLE RENAME, the publication still includes the table (by OID),
-- and pgoutput RELATION messages carry the new name with the same OID.
-- The OID-based routing should continue syncing and update metadata with
-- the new table name.

SELECT duckpipe.start_worker();

CREATE TABLE rename_src (id int primary key, val text);
SELECT duckpipe.add_table('public.rename_src', NULL, 'default', false);

-- Insert initial data
INSERT INTO rename_src VALUES (1, 'before_rename');

SELECT pg_sleep(2);

-- Verify initial sync
SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Verify metadata has original name
SELECT source_table FROM duckpipe.table_mappings WHERE source_table = 'rename_src';

-- Rename the source table
ALTER TABLE rename_src RENAME TO rename_src_v2;

-- Insert more data using the new name
INSERT INTO rename_src_v2 VALUES (2, 'after_rename');

SELECT pg_sleep(3);

-- Verify new data synced to the same target (target name doesn't change)
SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Verify metadata was updated with new name
SELECT source_table FROM duckpipe.table_mappings
WHERE source_table IN ('rename_src', 'rename_src_v2');

-- UPDATE and DELETE should also work after rename
UPDATE rename_src_v2 SET val = 'updated' WHERE id = 1;
DELETE FROM rename_src_v2 WHERE id = 2;

SELECT pg_sleep(2);

SELECT * FROM public.rename_src_ducklake ORDER BY id;

-- Cleanup (use new name)
SELECT duckpipe.remove_table('public.rename_src_v2', false);
DROP TABLE public.rename_src_ducklake;
DROP TABLE rename_src_v2;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
