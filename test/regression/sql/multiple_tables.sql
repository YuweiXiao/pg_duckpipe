-- Test multiple tables in the same sync group
CREATE EXTENSION pg_duckpipe CASCADE;
SELECT duckpipe.start_worker();

-- Create source tables
CREATE TABLE multi_a (id int primary key, val text);
CREATE TABLE multi_b (id int primary key, num int);
CREATE TABLE multi_c (id int primary key, flag boolean);

-- Create target tables
CREATE TABLE ducklake.multi_a (id int, val text) USING ducklake;
CREATE TABLE ducklake.multi_b (id int, num int) USING ducklake;
CREATE TABLE ducklake.multi_c (id int, flag boolean) USING ducklake;

-- Add all to default group
SELECT duckpipe.add_table('public.multi_a', 'ducklake.multi_a', 'default', false);
SELECT duckpipe.add_table('public.multi_b', 'ducklake.multi_b', 'default', false);
SELECT duckpipe.add_table('public.multi_c', 'ducklake.multi_c', 'default', false);

-- Insert into each table
INSERT INTO multi_a VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO multi_b VALUES (10, 100), (20, 200);
INSERT INTO multi_c VALUES (1, true), (2, false);

SELECT pg_sleep(2);

SELECT * FROM ducklake.multi_a ORDER BY id;
SELECT * FROM ducklake.multi_b ORDER BY id;
SELECT * FROM ducklake.multi_c ORDER BY id;

-- Update and delete across tables
UPDATE multi_a SET val = 'gamma' WHERE id = 1;
DELETE FROM multi_b WHERE id = 10;

SELECT pg_sleep(2);

SELECT * FROM ducklake.multi_a ORDER BY id;
SELECT * FROM ducklake.multi_b ORDER BY id;

-- Remove tables individually
SELECT duckpipe.remove_table('public.multi_a', false);
SELECT duckpipe.remove_table('public.multi_b', false);
SELECT duckpipe.remove_table('public.multi_c', false);

DROP TABLE ducklake.multi_a;
DROP TABLE ducklake.multi_b;
DROP TABLE ducklake.multi_c;
DROP TABLE multi_a;
DROP TABLE multi_b;
DROP TABLE multi_c;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
DROP EXTENSION pg_duckpipe CASCADE;
