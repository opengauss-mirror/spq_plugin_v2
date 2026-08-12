\c - - - :master_port

SELECT spq_add_node('localhost', :worker_1_port) > 0;
SELECT spq_add_node('localhost', :worker_2_port) > 0;

CREATE TABLE follower_shard_sizes_test (
    distribution_key integer,
    payload text
);
SELECT create_distributed_table('follower_shard_sizes_test', 'distribution_key');
INSERT INTO follower_shard_sizes_test VALUES (1, 'follower-read');

\c - - - :follower_master_port

SELECT pg_is_in_recovery();
SELECT count(*) > 0 AS has_test_shard_sizes
FROM pg_catalog.spq_shard_sizes() shard_sizes
JOIN pg_dist_shard shards
  ON shards.shardid = shard_sizes.shard_id
WHERE shards.logicalrelid = 'follower_shard_sizes_test'::regclass
  AND shard_sizes.size > 0;

\c - - - :master_port

DROP TABLE follower_shard_sizes_test;
SELECT spq_remove_node('localhost', :worker_1_port);
SELECT spq_remove_node('localhost', :worker_2_port);
