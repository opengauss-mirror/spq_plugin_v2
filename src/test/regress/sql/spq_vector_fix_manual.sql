\set ON_ERROR_STOP on

-- Run this file on the coordinator (the CN, normally port 7988) after
-- loading the spq extension. Every block prints the rows and EXPLAIN output
-- that should be checked when validating vector-distribution pruning.
-- The two missing-center sections intentionally emit SQLSTATE 55000 errors;
-- ON_ERROR_STOP is disabled only around those statements so cleanup continues.
SET spq.propagate_set_commands = 'local';

DROP TABLE IF EXISTS vector_fix_hnsw CASCADE;
CREATE TABLE vector_fix_hnsw (id int, val vector(2));
INSERT INTO vector_fix_hnsw VALUES
    (1, '[0,0]'),
    (100, '[10,10]');
CREATE INDEX vector_fix_hnsw_idx
    ON vector_fix_hnsw USING hnsw (val vector_l2_ops);
SELECT create_distributed_table(
    'vector_fix_hnsw'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);

-- Both shards must have a center, and min/max are equal because they store
-- centers rather than scalar range boundaries.
SELECT shardid, shardminvalue, shardmaxvalue
FROM pg_dist_shard
WHERE logicalrelid = 'vector_fix_hnsw'::regclass
ORDER BY shardid;

INSERT INTO vector_fix_hnsw VALUES
    (3, '[-1,100]'),
    (4, '[20,-100]'),
    (5, '[5,5]');

-- Equality pruning must use nearest-center routing, not vector_cmp.
SELECT id FROM vector_fix_hnsw
WHERE val = '[-1,100]'::vector;
SELECT id FROM vector_fix_hnsw
WHERE val = '[20,-100]'::vector;
SELECT id FROM vector_fix_hnsw
WHERE val = '[5,5]'::vector;

UPDATE vector_fix_hnsw
SET id = 30
WHERE val = '[-1,100]'::vector
RETURNING id;

DELETE FROM vector_fix_hnsw
WHERE val = '[20,-100]'::vector
RETURNING id;

-- With probes=1, only a primary ascending distance key may prune to one
-- shard. Expected ids below are 1, 30, 100, and 1. DESC and secondary
-- distance keys must retain both shards.
SET hnsw_distributed_probes = 1;
SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector DESC
LIMIT 1;
SELECT id FROM vector_fix_hnsw
ORDER BY id DESC, val <-> '[0,0]'::vector
LIMIT 1;
SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector, id DESC
LIMIT 1;

EXPLAIN SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
EXPLAIN SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector DESC
LIMIT 1;
EXPLAIN SELECT id FROM vector_fix_hnsw
ORDER BY id DESC, val <-> '[0,0]'::vector
LIMIT 1;

-- Parameterized queries must remain correct. Task Count: 1 means the bound
-- parameter was resolved during planning; Task Count: 2 is a correct fallback
-- when the parameter remains unresolved.
PREPARE vector_fix_hnsw_query(vector) AS
SELECT id FROM vector_fix_hnsw
ORDER BY val <-> $1
LIMIT 1;
EXECUTE vector_fix_hnsw_query('[0,0]');
EXECUTE vector_fix_hnsw_query('[10,10]');
EXPLAIN EXECUTE vector_fix_hnsw_query('[0,0]');
EXPLAIN EXECUTE vector_fix_hnsw_query('[10,10]');
DEALLOCATE vector_fix_hnsw_query;

-- A missing center is metadata corruption and must be an error, never a
-- silently incomplete result. Roll back the corruption after each check.
\set ON_ERROR_STOP off
\echo 'EXPECTED ERROR: HNSW ORDER BY must reject a missing distribution center'
BEGIN;
UPDATE pg_dist_shard
SET shardminvalue = NULL
WHERE shardid = (
    SELECT max(shardid)
    FROM pg_dist_shard
    WHERE logicalrelid = 'vector_fix_hnsw'::regclass
);
SELECT id FROM vector_fix_hnsw
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
ROLLBACK;

\echo 'EXPECTED ERROR: HNSW INSERT must reject a missing distribution center'
BEGIN;
UPDATE pg_dist_shard
SET shardminvalue = NULL
WHERE shardid = (
    SELECT max(shardid)
    FROM pg_dist_shard
    WHERE logicalrelid = 'vector_fix_hnsw'::regclass
);
INSERT INTO vector_fix_hnsw VALUES (200, '[1,1]');
ROLLBACK;
\set ON_ERROR_STOP on

-- Duplicate centers must not disable equality routing.
DROP TABLE IF EXISTS vector_fix_hnsw_duplicate CASCADE;
CREATE TABLE vector_fix_hnsw_duplicate (id int, val vector(2));
INSERT INTO vector_fix_hnsw_duplicate VALUES
    (1, '[0,0]'),
    (2, '[0,0]');
CREATE INDEX vector_fix_hnsw_duplicate_idx
    ON vector_fix_hnsw_duplicate USING hnsw (val vector_l2_ops);
SELECT create_distributed_table(
    'vector_fix_hnsw_duplicate'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);
SELECT count(*) AS shard_count,
       count(DISTINCT shardminvalue) AS distinct_center_count
FROM pg_dist_shard
WHERE logicalrelid = 'vector_fix_hnsw_duplicate'::regclass;
INSERT INTO vector_fix_hnsw_duplicate VALUES (3, '[5,5]');
SELECT id FROM vector_fix_hnsw_duplicate
WHERE val = '[5,5]'::vector;

DROP TABLE vector_fix_hnsw_duplicate;
DROP TABLE vector_fix_hnsw;
SET hnsw_distributed_probes = DEFAULT;

-- Repeat the same checks for IVF.
DROP TABLE IF EXISTS vector_fix_ivf CASCADE;
CREATE TABLE vector_fix_ivf (id int, val vector(2));
INSERT INTO vector_fix_ivf VALUES
    (1, '[0,0]'),
    (100, '[10,10]');
CREATE INDEX vector_fix_ivf_idx
    ON vector_fix_ivf USING ivfflat (val vector_l2_ops) WITH (lists = 2);
SELECT create_distributed_table(
    'vector_fix_ivf'::regclass,
    'val'::text,
    'ivf'::__$spq$__.distribution_type,
    2);

SELECT shardid, shardminvalue, shardmaxvalue
FROM pg_dist_shard
WHERE logicalrelid = 'vector_fix_ivf'::regclass
ORDER BY shardid;

INSERT INTO vector_fix_ivf VALUES
    (3, '[-1,100]'),
    (4, '[20,-100]'),
    (5, '[5,5]');
SELECT id FROM vector_fix_ivf
WHERE val = '[-1,100]'::vector;
SELECT id FROM vector_fix_ivf
WHERE val = '[20,-100]'::vector;

UPDATE vector_fix_ivf
SET id = 30
WHERE val = '[-1,100]'::vector
RETURNING id;
DELETE FROM vector_fix_ivf
WHERE val = '[20,-100]'::vector
RETURNING id;

SET ivfflat_distributed_probes = 1;
-- Expected ids below are 1, 30, 100, and 1.
SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector DESC
LIMIT 1;
SELECT id FROM vector_fix_ivf
ORDER BY id DESC, val <-> '[0,0]'::vector
LIMIT 1;
SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector, id DESC
LIMIT 1;

EXPLAIN SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
EXPLAIN SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector DESC
LIMIT 1;
EXPLAIN SELECT id FROM vector_fix_ivf
ORDER BY id DESC, val <-> '[0,0]'::vector
LIMIT 1;

PREPARE vector_fix_ivf_query(vector) AS
SELECT id FROM vector_fix_ivf
ORDER BY val <-> $1
LIMIT 1;
EXECUTE vector_fix_ivf_query('[0,0]');
EXECUTE vector_fix_ivf_query('[10,10]');
EXPLAIN EXECUTE vector_fix_ivf_query('[0,0]');
EXPLAIN EXECUTE vector_fix_ivf_query('[10,10]');
DEALLOCATE vector_fix_ivf_query;

\set ON_ERROR_STOP off
\echo 'EXPECTED ERROR: IVF ORDER BY must reject a missing distribution center'
BEGIN;
UPDATE pg_dist_shard
SET shardminvalue = NULL
WHERE shardid = (
    SELECT max(shardid)
    FROM pg_dist_shard
    WHERE logicalrelid = 'vector_fix_ivf'::regclass
);
SELECT id FROM vector_fix_ivf
ORDER BY val <-> '[0,0]'::vector
LIMIT 1;
ROLLBACK;
\set ON_ERROR_STOP on

DROP TABLE vector_fix_ivf;
SET ivfflat_distributed_probes = DEFAULT;
SET spq.propagate_set_commands = DEFAULT;
