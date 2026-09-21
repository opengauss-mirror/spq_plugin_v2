SET spq.propagate_set_commands = 'local';

CREATE OR REPLACE FUNCTION ivf_distribution_task_count(query_text text)
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

-- successful IVF distributed table creation
CREATE TABLE ivf_dist_t (id int, val vector(2));
INSERT INTO ivf_dist_t (id, val) VALUES
    (1, '[0,0]'),
    (2, '[0,1]'),
    (3, '[10,10]'),
    (4, '[10,11]');
CREATE INDEX ivf_dist_t_val_idx ON ivf_dist_t USING ivfflat (val vector_l2_ops) WITH (lists = 2);

SELECT create_distributed_table(
    'ivf_dist_t'::regclass,
    'val'::text,
    'ivf'::__$spq$__.distribution_type,
    3);

SELECT partmethod
FROM pg_dist_partition
WHERE logicalrelid = 'ivf_dist_t'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'ivf_dist_t'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'ivf_dist_t'::regclass
  AND shardminvalue IS NOT NULL
  AND shardmaxvalue IS NOT NULL
  AND shardminvalue = shardmaxvalue;

INSERT INTO ivf_dist_t (id, val) VALUES
    (5, '[0.2,0.1]'),
    (6, '[10.2,10.1]');

SELECT count(*) FROM ivf_dist_t;
SELECT id FROM ivf_dist_t ORDER BY id;

SET ivfflat_distributed_probes = 1;
SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_dist_t ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_dist_t ORDER BY val <-> ''[0,0]''::vector');

SET ivfflat_distributed_probes = 2;
SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_dist_t ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

DROP TABLE ivf_dist_t;

-- shard_count can be smaller than IVF center count
CREATE TABLE ivf_more_centers (id int, val vector(2));
INSERT INTO ivf_more_centers (id, val) VALUES
    (1, '[0,0]'),
    (2, '[0,1]'),
    (3, '[0,2]'),
    (4, '[10,10]'),
    (5, '[10,11]'),
    (6, '[10,12]');
CREATE INDEX ivf_more_centers_val_idx ON ivf_more_centers USING ivfflat (val vector_l2_ops) WITH (lists = 3);
SELECT create_distributed_table(
    'ivf_more_centers'::regclass,
    'val'::text,
    'ivf'::__$spq$__.distribution_type,
    2);

SELECT partmethod
FROM pg_dist_partition
WHERE logicalrelid = 'ivf_more_centers'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'ivf_more_centers'::regclass;

SELECT count(*)
FROM pg_dist_shard
WHERE logicalrelid = 'ivf_more_centers'::regclass
  AND shardminvalue IS NOT NULL
  AND shardmaxvalue IS NOT NULL
  AND shardminvalue = shardmaxvalue;

INSERT INTO ivf_more_centers (id, val) VALUES
    (7, '[0.2,0.1]'),
    (8, '[10.2,10.1]');

SELECT count(*) FROM ivf_more_centers;
SELECT id FROM ivf_more_centers ORDER BY id;

SET ivfflat_distributed_probes = 1;
SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_more_centers ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_more_centers ORDER BY val <-> ''[0,0]''::vector');

SET ivfflat_distributed_probes = 2;
SELECT ivf_distribution_task_count(
    'SELECT id FROM ivf_more_centers ORDER BY val <-> ''[0,0]''::vector LIMIT 1');

DROP TABLE ivf_more_centers;

-- distribution column must be vector
CREATE TABLE ivf_non_vector_dist (id int, val vector(2));
INSERT INTO ivf_non_vector_dist (id, val) VALUES
    (1, '[0,0]'),
    (2, '[1,1]');
CREATE INDEX ivf_non_vector_dist_val_idx ON ivf_non_vector_dist USING ivfflat (val vector_l2_ops) WITH (lists = 1);
SELECT create_distributed_table(
    'ivf_non_vector_dist'::regclass,
    'id'::text,
    'ivf'::__$spq$__.distribution_type);
DROP TABLE ivf_non_vector_dist;

-- IVF distribution requires an L2 IVFFLAT index on the distribution column
CREATE TABLE ivf_no_l2_index (id int, val vector(2));
INSERT INTO ivf_no_l2_index (id, val) VALUES
    (1, '[0,0]'),
    (2, '[1,1]');
CREATE INDEX ivf_no_l2_index_val_idx ON ivf_no_l2_index USING ivfflat (val vector_ip_ops) WITH (lists = 1);
SELECT create_distributed_table(
    'ivf_no_l2_index'::regclass,
    'val'::text,
    'ivf'::__$spq$__.distribution_type);
DROP TABLE ivf_no_l2_index;

-- empty tables cannot generate distribution centers
CREATE TABLE ivf_empty_dist (id int, val vector(2));
CREATE INDEX ivf_empty_dist_val_idx ON ivf_empty_dist USING ivfflat (val vector_l2_ops) WITH (lists = 1);
SELECT create_distributed_table(
    'ivf_empty_dist'::regclass,
    'val'::text,
    'ivf'::__$spq$__.distribution_type);
DROP TABLE ivf_empty_dist;

DROP FUNCTION ivf_distribution_task_count;
SET ivfflat_distributed_probes = DEFAULT;
SET spq.propagate_set_commands = DEFAULT;
