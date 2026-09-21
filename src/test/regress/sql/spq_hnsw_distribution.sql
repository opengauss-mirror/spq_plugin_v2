SET spq.propagate_set_commands = 'local';

CREATE OR REPLACE FUNCTION hnsw_distribution_task_count(query_text text)
    RETURNS text
    LANGUAGE plpgsql
AS $$
DECLARE
    plan_line text;
BEGIN
    FOR plan_line IN EXECUTE 'EXPLAIN ' || query_text LOOP
        IF plan_line LIKE '%Task Count:%' THEN
            RETURN btrim(plan_line);
        END IF;
    END LOOP;

    RETURN NULL;
END;
$$;

-- successful HNSW distributed table creation
CREATE TABLE hnsw_dist_t (id int, val vector(2));
INSERT INTO hnsw_dist_t (id, val) VALUES
    (1, '[0,0]'),
    (2, '[0,1]'),
    (3, '[10,10]'),
    (4, '[10,11]');
CREATE INDEX hnsw_dist_t_val_idx ON hnsw_dist_t USING hnsw (val vector_l2_ops);

SELECT create_distributed_table(
    'hnsw_dist_t'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);

SELECT partmethod
FROM pg_dist_partition
WHERE logicalrelid = 'hnsw_dist_t'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'hnsw_dist_t'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'hnsw_dist_t'::regclass
  AND shardminvalue IS NOT NULL
  AND shardmaxvalue IS NOT NULL
  AND shardminvalue = shardmaxvalue;

INSERT INTO hnsw_dist_t (id, val) VALUES
    (5, '[0.2,0.1]'),
    (6, '[10.2,10.1]');

SELECT count(*) FROM hnsw_dist_t;
SELECT id FROM hnsw_dist_t ORDER BY id;

SET hnsw_distributed_probes = 1;
SELECT hnsw_distribution_task_count(
    'SELECT id FROM hnsw_dist_t ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

SELECT hnsw_distribution_task_count(
    'SELECT id FROM hnsw_dist_t ORDER BY val <-> ''[0,0]''::vector');

SET hnsw_distributed_probes = 2;
SELECT hnsw_distribution_task_count(
    'SELECT id FROM hnsw_dist_t ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

DROP TABLE hnsw_dist_t;

-- shard_count is required for HNSW distribution
CREATE TABLE hnsw_no_shard_count (id int, val vector(2));
INSERT INTO hnsw_no_shard_count (id, val) VALUES
    (1, '[0,0]'),
    (2, '[1,1]');
CREATE INDEX hnsw_no_shard_count_val_idx ON hnsw_no_shard_count USING hnsw (val vector_l2_ops);
SELECT create_distributed_table(
    'hnsw_no_shard_count'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type);
DROP TABLE hnsw_no_shard_count;

-- distribution column must be vector
CREATE TABLE hnsw_non_vector_dist (id int, val vector(2));
INSERT INTO hnsw_non_vector_dist (id, val) VALUES
    (1, '[0,0]'),
    (2, '[1,1]');
CREATE INDEX hnsw_non_vector_dist_val_idx ON hnsw_non_vector_dist USING hnsw (val vector_l2_ops);
SELECT create_distributed_table(
    'hnsw_non_vector_dist'::regclass,
    'id'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);
DROP TABLE hnsw_non_vector_dist;

-- HNSW distribution requires an L2 HNSW index on the distribution column
CREATE TABLE hnsw_no_l2_index (id int, val vector(2));
INSERT INTO hnsw_no_l2_index (id, val) VALUES
    (1, '[0,0]'),
    (2, '[1,1]');
CREATE INDEX hnsw_no_l2_index_val_idx ON hnsw_no_l2_index USING hnsw (val vector_ip_ops);
SELECT create_distributed_table(
    'hnsw_no_l2_index'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);
DROP TABLE hnsw_no_l2_index;

-- empty tables cannot generate distribution centers
CREATE TABLE hnsw_empty_dist (id int, val vector(2));
CREATE INDEX hnsw_empty_dist_val_idx ON hnsw_empty_dist USING hnsw (val vector_l2_ops);
SELECT create_distributed_table(
    'hnsw_empty_dist'::regclass,
    'val'::text,
    'hnsw'::__$spq$__.distribution_type,
    2);
DROP TABLE hnsw_empty_dist;

DROP FUNCTION hnsw_distribution_task_count;
SET hnsw_distributed_probes = DEFAULT;
SET spq.propagate_set_commands = DEFAULT;
