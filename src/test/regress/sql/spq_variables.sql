SHOW spq.version;

SELECT name,setting FROM pg_settings WHERE name like 'spq.%';

SHOW spq.bm25_global_stat_cache_ttl;

ALTER SYSTEM SET spq.bm25_global_stat_cache_ttl = '120s';
SELECT pg_reload_conf();
\c regression
SHOW spq.bm25_global_stat_cache_ttl;

ALTER SYSTEM SET spq.bm25_global_stat_cache_ttl = '3600s';
SELECT pg_reload_conf();
\c regression
SHOW spq.bm25_global_stat_cache_ttl;

--add more variables testcases for spq's variables
