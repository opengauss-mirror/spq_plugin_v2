CREATE FUNCTION pg_catalog.bm25_collect_global_stat(query_text text,
                                                    table_name text DEFAULT NULL)
    RETURNS text
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', $$bm25_collect_global_stat$$;
COMMENT ON FUNCTION pg_catalog.bm25_collect_global_stat(text, text)
    IS 'collect distributed BM25 global stats of a table from all DNs and SET LOCAL bm25_global_stat';
