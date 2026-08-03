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

DROP TABLE bm25_global_cache_test;
RESET enable_seqscan;
RESET enable_bm25_global_idf;
