/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 */
#ifndef BM25_GLOBAL_STAT_CACHE_H
#define BM25_GLOBAL_STAT_CACHE_H

#include "postgres.h"

#define BM25_GLOBAL_STAT_TERM_LEN 100

extern int Bm25GlobalStatCacheTtlSeconds;
extern void InitializeBm25GlobalStatCache(void);
extern bool Bm25GlobalStatCacheLookup(Oid databaseOid, Oid relationOid,
	AttrNumber columnAttnum,
	const char* const* terms, int termCount,
	char** statOut);
extern void Bm25GlobalStatCacheStore(Oid databaseOid, Oid relationOid,
	AttrNumber columnAttnum,
	const char* stat);

#endif
