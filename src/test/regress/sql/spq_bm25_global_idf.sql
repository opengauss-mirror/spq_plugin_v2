SET enable_bm25_global_idf = on;
SET enable_seqscan = off;

CREATE TABLE bm25_global_cache_test (
    id int,
    title text,
    content text
);
SELECT create_distributed_table('bm25_global_cache_test', 'id', shard_count := 4);
INSERT INTO bm25_global_cache_test VALUES
    (1, 'heart', 'heart surgery'),
    (2, 'heart', 'medical care care'),
    (3, 'brain', 'heart heart care'),
    (4, 'brain', 'brain surgery surgery');
INSERT INTO bm25_global_cache_test
SELECT id, 'filler', 'unrelated text'
FROM generate_series(5, 1004) AS id;
CREATE INDEX bm25_global_cache_title_idx
    ON bm25_global_cache_test USING bm25(title);
CREATE INDEX bm25_global_cache_content_idx
    ON bm25_global_cache_test USING bm25(content);
ANALYZE bm25_global_cache_test;

-- The coordinator is metadata-only and deliberately cannot accept a TCP connection.
UPDATE pg_dist_node
SET nodeport = 1, shouldhaveshards = false
WHERE groupid = 0;
SELECT groupid = 0 AND noderole = 'primary' AND NOT shouldhaveshards
       AS coordinator_is_metadata_only
FROM pg_dist_node
WHERE groupid = 0;
SELECT bool_and(placement.groupid <> 0) AS all_shards_are_on_workers
FROM pg_dist_shard AS shard
JOIN pg_dist_placement AS placement USING (shardid)
WHERE shard.logicalrelid = 'bm25_global_cache_test'::regclass;
SELECT bm25_collect_global_stat(
           'heart surgery', 'bm25_global_cache_test', 'content'
       ) LIKE '%(workers=2)' AS collected_from_workers_only;

-- First execution collects from DNs; repeated and overlapping queries reuse term cache.
SELECT id
FROM bm25_global_cache_test
ORDER BY content <&> 'heart surgery' DESC
LIMIT 2;
SELECT id
FROM bm25_global_cache_test
ORDER BY content <&> 'heart surgery' DESC
LIMIT 2;
SELECT id
FROM bm25_global_cache_test
ORDER BY content <&> 'heart care' DESC
LIMIT 2;

-- Distinct BM25 query information in one statement falls back to Local IDF.
(SELECT id
 FROM bm25_global_cache_test
 ORDER BY title <&> 'heart' DESC
 LIMIT 1)
UNION ALL
(SELECT id
 FROM bm25_global_cache_test
 ORDER BY content <&> 'surgery' DESC
 LIMIT 1);

UPDATE pg_dist_node
SET nodeport = :master_port
WHERE groupid = 0;
DROP TABLE bm25_global_cache_test;
RESET enable_seqscan;
RESET enable_bm25_global_idf;
