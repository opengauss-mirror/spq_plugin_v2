SET spq.propagate_set_commands = 'local';
SET client_min_messages = warning;

CREATE TEMP TABLE vector_pruning_reference (id int, val vector(2));

-- Compare each predicate against an ordinary table, including values outside
-- the centers' lexicographic range and a value equidistant from both centers.
CREATE FUNCTION vector_pruning_check(table_name text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    predicate text;
    actual_ids int[];
    expected_ids int[];
    plan_line text;
    task_count int;
    returned_id int;
BEGIN
    TRUNCATE vector_pruning_reference;
    EXECUTE format('INSERT INTO vector_pruning_reference SELECT * FROM %I', table_name);

    FOREACH predicate IN ARRAY ARRAY[
        'val = ''[-1,100]''::vector',
        'val = ''[20,-100]''::vector',
        'val = ''[5,5]''::vector',
        'val = ''[0,0]''::vector',
        'val = ''[99,99]''::vector',
        'val < ''[0,0]''::vector',
        'val <= ''[-1,100]''::vector',
        'val > ''[10,10]''::vector',
        'val >= ''[20,-100]''::vector',
        'val = ''[-1,100]''::vector AND val < ''[0,0]''::vector',
        'val = ''[-1,100]''::vector AND val > ''[0,0]''::vector',
        'val = ''[-1,100]''::vector AND val = ''[20,-100]''::vector',
        'val IN (''[-1,100]''::vector, ''[20,-100]''::vector)',
        'val = ''[-1,100]''::vector OR val = ''[20,-100]''::vector',
        'val = ''[5,5]''::vector OR val > ''[10,10]''::vector',
        'val = ''[-1,100]''::vector OR id = 2',
        'false'
    ] LOOP
        EXECUTE format('SELECT array_agg(id ORDER BY id) FROM %I WHERE %s',
                       table_name, predicate) INTO actual_ids;
        EXECUTE format('SELECT array_agg(id ORDER BY id) FROM vector_pruning_reference WHERE %s',
                       predicate) INTO expected_ids;
        IF actual_ids IS DISTINCT FROM expected_ids THEN
            RAISE EXCEPTION 'result mismatch for %: %, got %, expected %',
                table_name, predicate, actual_ids, expected_ids;
        END IF;
    END LOOP;

    FOREACH predicate IN ARRAY ARRAY[
        'val = ''[-1,100]''::vector',
        'val = ''[5,5]''::vector',
        'val < ''[0,0]''::vector',
        'val > ''[10,10]''::vector'
    ] LOOP
        task_count := NULL;
        FOR plan_line IN EXECUTE format('EXPLAIN SELECT id FROM %I WHERE %s',
                                        table_name, predicate) LOOP
            IF plan_line LIKE '%Task Count:%' THEN
                task_count := btrim(split_part(plan_line, 'Task Count:', 2))::int;
            END IF;
        END LOOP;
        IF task_count IS DISTINCT FROM
            (CASE WHEN predicate LIKE 'val =%' THEN 1 ELSE 2 END) THEN
            RAISE EXCEPTION 'unexpected task count for %: %, got %',
                table_name, predicate, task_count;
        END IF;
    END LOOP;

    EXECUTE format('UPDATE %I SET id = 30 WHERE val = ''[-1,100]''::vector RETURNING id',
                   table_name) INTO returned_id;
    IF returned_id IS DISTINCT FROM 30 THEN
        RAISE EXCEPTION 'equality UPDATE missed row in %', table_name;
    END IF;
    UPDATE vector_pruning_reference SET id = 30 WHERE val = '[-1,100]'::vector;

    EXECUTE format('DELETE FROM %I WHERE val = ''[20,-100]''::vector RETURNING id',
                   table_name) INTO returned_id;
    IF returned_id IS DISTINCT FROM 4 THEN
        RAISE EXCEPTION 'equality DELETE missed row in %', table_name;
    END IF;
    DELETE FROM vector_pruning_reference WHERE val = '[20,-100]'::vector;

    EXECUTE format('SELECT array_agg(id ORDER BY id) FROM %I', table_name) INTO actual_ids;
    SELECT array_agg(id ORDER BY id) INTO expected_ids FROM vector_pruning_reference;
    IF actual_ids IS DISTINCT FROM expected_ids THEN
        RAISE EXCEPTION 'post-DML mismatch in %: got %, expected %',
            table_name, actual_ids, expected_ids;
    END IF;

    RETURN 'ok';
END;
$$;

-- Two distinct centers, generated from exactly two distinct seed vectors.
CREATE TABLE vector_pruning_hnsw (id int, val vector(2));
INSERT INTO vector_pruning_hnsw VALUES (1, '[0,0]'), (2, '[10,10]');
CREATE INDEX ON vector_pruning_hnsw USING hnsw (val vector_l2_ops);
SELECT create_distributed_table('vector_pruning_hnsw', 'val', 'hnsw', 2);
SELECT count(DISTINCT shardminvalue) = 2 AS distinct_centers
FROM pg_dist_shard WHERE logicalrelid = 'vector_pruning_hnsw'::regclass;
INSERT INTO vector_pruning_hnsw VALUES
    (3, '[-1,100]'), (4, '[20,-100]'), (5, '[5,5]');
SELECT vector_pruning_check('vector_pruning_hnsw');
DROP TABLE vector_pruning_hnsw;

CREATE TABLE vector_pruning_ivf (id int, val vector(2));
INSERT INTO vector_pruning_ivf VALUES (1, '[0,0]'), (2, '[10,10]');
CREATE INDEX ON vector_pruning_ivf USING ivfflat (val vector_l2_ops) WITH (lists = 2);
SELECT create_distributed_table('vector_pruning_ivf', 'val', 'ivf', 2);
SELECT count(DISTINCT shardminvalue) = 2 AS distinct_centers
FROM pg_dist_shard WHERE logicalrelid = 'vector_pruning_ivf'::regclass;
INSERT INTO vector_pruning_ivf VALUES
    (3, '[-1,100]'), (4, '[20,-100]'), (5, '[5,5]');
SELECT vector_pruning_check('vector_pruning_ivf');
DROP TABLE vector_pruning_ivf;

-- HNSW retains duplicate centers for identical samples. This naturally
-- exercises overlapping intervals without changing routing metadata.
CREATE TABLE vector_pruning_overlap (id int, val vector(2));
INSERT INTO vector_pruning_overlap VALUES (1, '[0,0]'), (2, '[0,0]');
CREATE INDEX ON vector_pruning_overlap USING hnsw (val vector_l2_ops);
SELECT create_distributed_table('vector_pruning_overlap', 'val', 'hnsw', 2);
SELECT count(*) = 2 AND count(DISTINCT shardminvalue) = 1 AS duplicate_centers
FROM pg_dist_shard WHERE logicalrelid = 'vector_pruning_overlap'::regclass;
INSERT INTO vector_pruning_overlap VALUES
    (3, '[-1,100]'), (4, '[20,-100]'), (5, '[5,5]');
SELECT vector_pruning_check('vector_pruning_overlap');
DROP TABLE vector_pruning_overlap;

-- ORDER BY pruning must recognize only a leading ascending distance key.
CREATE FUNCTION vector_order_pruning_check(table_name text, probe_count int) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    test_case record;
    result_row record;
    query_text text;
    actual_ids int[];
    plan_line text;
    task_count int;
BEGIN
    FOR test_case IN
        SELECT * FROM (VALUES
            ('SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector LIMIT 1',
             ARRAY[1], probe_count),
            ('SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector ASC LIMIT 1',
             ARRAY[1], probe_count),
            ('SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector DESC LIMIT 1',
             ARRAY[100], 2),
            ('SELECT id FROM %I ORDER BY id DESC, val <-> ''[0,0]''::vector LIMIT 1',
             ARRAY[100], 2),
            ('SELECT id FROM %I ORDER BY id + 0 DESC, val <-> ''[0,0]''::vector LIMIT 1',
             ARRAY[100], 2),
            ('SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector ASC, id DESC LIMIT 1',
             ARRAY[1], probe_count),
            ('SELECT id FROM %I ORDER BY ''[0,0]''::vector <-> val LIMIT 1',
             ARRAY[1], probe_count),
            ('SELECT id, val <-> ''[0,0]''::vector AS distance FROM %I ORDER BY distance LIMIT 1',
             ARRAY[1], probe_count),
            ('SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector',
             ARRAY[1,100], 2),
            ('SELECT id FROM %I WHERE val = ''[10,10]''::vector ORDER BY val <-> ''[0,0]''::vector DESC LIMIT 1',
             ARRAY[100], 1)
        ) AS cases(query_template, expected_ids, expected_tasks)
    LOOP
        query_text := format(test_case.query_template, table_name);
        actual_ids := ARRAY[]::int[];
        FOR result_row IN EXECUTE query_text LOOP
            actual_ids := array_append(actual_ids, result_row.id);
        END LOOP;
        IF actual_ids IS DISTINCT FROM test_case.expected_ids THEN
            RAISE EXCEPTION 'ORDER BY result mismatch: %, got %, expected %',
                query_text, actual_ids, test_case.expected_ids;
        END IF;

        task_count := NULL;
        FOR plan_line IN EXECUTE 'EXPLAIN ' || query_text LOOP
            IF plan_line LIKE '%Task Count:%' THEN
                task_count := btrim(split_part(plan_line, 'Task Count:', 2))::int;
            END IF;
        END LOOP;
        IF task_count IS DISTINCT FROM test_case.expected_tasks THEN
            RAISE EXCEPTION 'ORDER BY task count mismatch: %, got %, expected %',
                query_text, task_count, test_case.expected_tasks;
        END IF;
    END LOOP;

    RETURN 'ok';
END;
$$;

-- Corrupt centers only inside rolled-back transactions. Check that both
-- routing and nearest-center pruning report the offending shard and SQLSTATE.
CREATE FUNCTION vector_missing_center_check(table_name text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    query_template text;
    error_message text;
    error_detail text;
    identifies_missing_shard bool;
BEGIN
    FOREACH query_template IN ARRAY ARRAY[
        'SELECT id FROM %I ORDER BY val <-> ''[0,0]''::vector LIMIT 1',
        'SELECT id FROM %I WHERE val = ''[0,0]''::vector',
        'INSERT INTO %I VALUES (200, ''[0,0]''::vector)',
        'UPDATE %I SET id = 200 WHERE val = ''[0,0]''::vector',
        'DELETE FROM %I WHERE val = ''[0,0]''::vector'
    ] LOOP
        BEGIN
            EXECUTE format(query_template, table_name);
            RAISE EXCEPTION 'missing center was silently accepted: %', query_template;
        EXCEPTION WHEN SQLSTATE '55000' THEN
            GET STACKED DIAGNOSTICS
                error_message = MESSAGE_TEXT,
                error_detail = PG_EXCEPTION_DETAIL;
            SELECT EXISTS (
                SELECT 1 FROM pg_dist_shard
                WHERE logicalrelid = table_name::regclass
                  AND shardminvalue IS NULL
                  AND error_message = format(
                      'vector distributed shard %s has no distribution center', shardid)
            ) INTO identifies_missing_shard;
            IF NOT identifies_missing_shard OR error_detail IS DISTINCT FROM
                'Shard metadata in pg_dist_shard is inconsistent.' THEN
                RAISE EXCEPTION 'unexpected missing-center error: %, detail: %',
                    error_message, error_detail;
            END IF;
        END;
    END LOOP;

    RETURN 'ok';
END;
$$;

-- Seed rows themselves form the two centers; the largest id and farthest
-- vector must be in the shard excluded by an incorrect nearest-center prune.
CREATE TABLE vector_order_hnsw (id int, val vector(2));
INSERT INTO vector_order_hnsw VALUES (1, '[0,0]'), (100, '[10,10]');
CREATE INDEX ON vector_order_hnsw USING hnsw (val vector_l2_ops);
SELECT create_distributed_table('vector_order_hnsw', 'val', 'hnsw', 2);
SET hnsw_distributed_probes = 1;
SELECT vector_order_pruning_check('vector_order_hnsw', 1);
SET hnsw_distributed_probes = 2;
SELECT vector_order_pruning_check('vector_order_hnsw', 2);
-- Reject a missing center even when another shard still has a valid one.
SET hnsw_distributed_probes = 1;
BEGIN;
UPDATE pg_dist_shard SET shardminvalue = NULL
WHERE logicalrelid = 'vector_order_hnsw'::regclass
  AND shardminvalue = '[10,10]';
SELECT vector_missing_center_check('vector_order_hnsw');
ROLLBACK;
-- All centers missing must also error, including when probes covers all shards.
SET hnsw_distributed_probes = 2;
BEGIN;
UPDATE pg_dist_shard SET shardminvalue = NULL
WHERE logicalrelid = 'vector_order_hnsw'::regclass;
SELECT vector_missing_center_check('vector_order_hnsw');
ROLLBACK;
-- Metadata and rows must remain usable after rollback.
SELECT vector_order_pruning_check('vector_order_hnsw', 2);
DROP TABLE vector_order_hnsw;
SET hnsw_distributed_probes = DEFAULT;

CREATE TABLE vector_order_ivf (id int, val vector(2));
INSERT INTO vector_order_ivf VALUES (1, '[0,0]'), (100, '[10,10]');
CREATE INDEX ON vector_order_ivf USING ivfflat (val vector_l2_ops) WITH (lists = 2);
SELECT create_distributed_table('vector_order_ivf', 'val', 'ivf', 2);
SET ivfflat_distributed_probes = 1;
SELECT vector_order_pruning_check('vector_order_ivf', 1);
SET ivfflat_distributed_probes = 2;
SELECT vector_order_pruning_check('vector_order_ivf', 2);
-- Reject a missing center even when another shard still has a valid one.
SET ivfflat_distributed_probes = 1;
BEGIN;
UPDATE pg_dist_shard SET shardminvalue = NULL
WHERE logicalrelid = 'vector_order_ivf'::regclass
  AND shardminvalue = '[10,10]';
SELECT vector_missing_center_check('vector_order_ivf');
ROLLBACK;
-- All centers missing must also error, including when probes covers all shards.
SET ivfflat_distributed_probes = 2;
BEGIN;
UPDATE pg_dist_shard SET shardminvalue = NULL
WHERE logicalrelid = 'vector_order_ivf'::regclass;
SELECT vector_missing_center_check('vector_order_ivf');
ROLLBACK;
-- Metadata and rows must remain usable after rollback.
SELECT vector_order_pruning_check('vector_order_ivf', 2);
DROP TABLE vector_order_ivf;
SET ivfflat_distributed_probes = DEFAULT;

DROP FUNCTION vector_order_pruning_check(text, int);
DROP FUNCTION vector_missing_center_check(text);

DROP FUNCTION vector_pruning_check(text);
DROP TABLE vector_pruning_reference;
RESET client_min_messages;
SET spq.propagate_set_commands = DEFAULT;
