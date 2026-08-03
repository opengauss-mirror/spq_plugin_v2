/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 */
#ifndef BM25_GLOBAL_STAT_CACHE_H
#define BM25_GLOBAL_STAT_CACHE_H

#include "postgres.h"

typedef enum Bm25GlobalStatCacheResult {
	BM25_GLOBAL_STAT_CACHE_HIT,
	BM25_GLOBAL_STAT_CACHE_REFRESH,
	BM25_GLOBAL_STAT_CACHE_FALLBACK,
	BM25_GLOBAL_STAT_CACHE_BYPASS
} Bm25GlobalStatCacheResult;

extern void InitializeBm25GlobalStatCache(void);
extern Bm25GlobalStatCacheResult Bm25GlobalStatCacheLookup(Oid databaseOid,
	Oid relationOid,
	const char* queryText,
	char** statOut);
extern void Bm25GlobalStatCacheStore(Oid databaseOid, Oid relationOid,
	const char* queryText, const char* stat);
extern void Bm25GlobalStatCacheAbortRefresh(Oid databaseOid, Oid relationOid,
	const char* queryText);

#endif
