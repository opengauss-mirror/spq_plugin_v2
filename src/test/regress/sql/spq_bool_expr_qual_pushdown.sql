CREATE SCHEMA bool_expr_qual_pushdown;
SET search_path TO bool_expr_qual_pushdown;
SET spq.next_shard_id TO 31080000;
SET spq.shard_count TO 2;
SET spq.shard_replication_factor TO 1;

CREATE TABLE test_bug2(
    id INT PRIMARY KEY,
    a TEXT,
    b TEXT
);

SELECT create_distributed_table('test_bug2', 'id');

INSERT INTO test_bug2 VALUES
    (1, 'x', '111'),
    (2, 'x', '222'),
    (3, 'y', '999');

SELECT * FROM test_bug2 WHERE a = 'x' AND b = '999' ORDER BY id;

SELECT * FROM test_bug2 WHERE a = 'y' OR b = '222' ORDER BY id;

EXPLAIN (COSTS OFF)
SELECT * FROM test_bug2 WHERE a = 'x' AND b = '999';

DROP SCHEMA bool_expr_qual_pushdown CASCADE;
RESET all;
