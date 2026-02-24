-- Test composite (multi-column) primary keys
--
-- Verifies INSERT, UPDATE, DELETE with composite PKs.
-- Exercises the DuckDB compaction logic (ROW_NUMBER PARTITION BY pk1, pk2)
-- and DELETE WHERE pk1 = ... AND pk2 = ... generation.

SELECT duckpipe.start_worker();

-- Two-column PK
CREATE TABLE cpk_orders (
    region text,
    order_id int,
    product text,
    qty int,
    PRIMARY KEY (region, order_id)
);

SELECT duckpipe.add_table('public.cpk_orders', NULL, 'default', false);

-- Insert rows with composite keys
INSERT INTO cpk_orders VALUES
    ('east', 1, 'widget', 10),
    ('east', 2, 'gadget', 5),
    ('west', 1, 'widget', 20),
    ('west', 2, 'gizmo', 3);

SELECT pg_sleep(3);

SELECT * FROM public.cpk_orders_ducklake ORDER BY region, order_id;

-- UPDATE by composite PK
UPDATE cpk_orders SET qty = 15 WHERE region = 'east' AND order_id = 1;
UPDATE cpk_orders SET product = 'super_gadget' WHERE region = 'east' AND order_id = 2;

SELECT pg_sleep(3);

SELECT * FROM public.cpk_orders_ducklake ORDER BY region, order_id;

-- DELETE by composite PK
DELETE FROM cpk_orders WHERE region = 'west' AND order_id = 2;

SELECT pg_sleep(2);

SELECT * FROM public.cpk_orders_ducklake ORDER BY region, order_id;

-- Three-column PK
CREATE TABLE cpk_triple (
    a int,
    b int,
    c int,
    val text,
    PRIMARY KEY (a, b, c)
);

SELECT duckpipe.add_table('public.cpk_triple', NULL, 'default', false);

INSERT INTO cpk_triple VALUES
    (1, 1, 1, 'aaa'),
    (1, 1, 2, 'aab'),
    (1, 2, 1, 'aba'),
    (2, 1, 1, 'baa');

SELECT pg_sleep(3);

SELECT * FROM public.cpk_triple_ducklake ORDER BY a, b, c;

-- Update one row, delete another
UPDATE cpk_triple SET val = 'updated' WHERE a = 1 AND b = 1 AND c = 1;
DELETE FROM cpk_triple WHERE a = 1 AND b = 2 AND c = 1;

SELECT pg_sleep(2);

SELECT * FROM public.cpk_triple_ducklake ORDER BY a, b, c;

-- Rapid updates to same composite key (compaction test)
UPDATE cpk_orders SET qty = 100 WHERE region = 'east' AND order_id = 1;
UPDATE cpk_orders SET qty = 200 WHERE region = 'east' AND order_id = 1;
UPDATE cpk_orders SET qty = 300 WHERE region = 'east' AND order_id = 1;

SELECT pg_sleep(3);

-- Should have final value only
SELECT region, order_id, qty FROM public.cpk_orders_ducklake
WHERE region = 'east' AND order_id = 1;

-- Cleanup
SELECT duckpipe.remove_table('public.cpk_orders', false);
SELECT duckpipe.remove_table('public.cpk_triple', false);
DROP TABLE public.cpk_orders_ducklake;
DROP TABLE public.cpk_triple_ducklake;
DROP TABLE cpk_orders;
DROP TABLE cpk_triple;

SET client_min_messages = warning;
SELECT duckpipe.stop_worker();
RESET client_min_messages;
