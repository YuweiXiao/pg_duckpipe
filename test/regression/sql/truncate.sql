-- Test TRUNCATE propagation
CREATE EXTENSION pg_duckpipe CASCADE;
SELECT duckpipe.start_worker();

CREATE TABLE trunc_test (id int primary key, val text);
CREATE TABLE ducklake.trunc_test (id int, val text) USING ducklake;

SELECT duckpipe.add_table('public.trunc_test', 'ducklake.trunc_test', 'default', false);

INSERT INTO trunc_test VALUES (1, 'one'), (2, 'two'), (3, 'three');

SELECT pg_sleep(2);

SELECT * FROM ducklake.trunc_test ORDER BY id;

-- Truncate source table
TRUNCATE trunc_test;

SELECT pg_sleep(2);

-- Verify target is also truncated
SELECT count(*) FROM ducklake.trunc_test;

SELECT duckpipe.remove_table('public.trunc_test', false);
DROP TABLE ducklake.trunc_test;
DROP TABLE trunc_test;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;
