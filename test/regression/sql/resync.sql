-- Test resync_table functionality
CREATE EXTENSION pg_duckpipe CASCADE;
SELECT duckpipe.start_worker();

CREATE TABLE resync_src (id int primary key, val text);
INSERT INTO resync_src VALUES (1, 'one'), (2, 'two');

CREATE TABLE ducklake.resync_src (id int, val text) USING ducklake;

SELECT duckpipe.add_table('public.resync_src', 'ducklake.resync_src', 'default', true);

SELECT pg_sleep(2);

-- Verify initial snapshot
SELECT * FROM ducklake.resync_src ORDER BY id;

-- Modify source data directly
INSERT INTO resync_src VALUES (3, 'three');

SELECT pg_sleep(2);

-- Now trigger resync
SELECT duckpipe.resync_table('public.resync_src');

SELECT pg_sleep(3);

-- Verify target has current source data
SELECT * FROM ducklake.resync_src ORDER BY id;

SELECT duckpipe.remove_table('public.resync_src', false);
DROP TABLE ducklake.resync_src;
DROP TABLE resync_src;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;
