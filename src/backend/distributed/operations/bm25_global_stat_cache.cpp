/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 */
#include "postgres.h"

#include "access/hash.h"
#include "executor/executor.h"
#include "knl/knl_instance.h"
#include "lib/stringinfo.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "distributed/bm25_global_stat_cache.h"

/*
 * BM25 statistics are approximate by nature. A one-minute TTL keeps repeated searches
 * cheap while bounding staleness after regular writes. The cache is deliberately small:
 * every entry is query-term-specific and a miss can safely use local IDF.
 */
static const int BM25_GLOBAL_STAT_CACHE_MAX_ENTRIES = 1024;
static const int64 BM25_GLOBAL_STAT_CACHE_TTL_US = 60 * USECS_PER_SEC;
static const int BM25_GLOBAL_STAT_VALUE_LEN = 65536;

typedef struct Bm25GlobalStatCacheKey {
    Oid databaseOid;
    Oid relationOid;
    uint64 queryLength;
    uint32 queryHash;
    uint32 reverseQueryHash;
} Bm25GlobalStatCacheKey;

typedef struct Bm25GlobalStatCacheEntry {
    Bm25GlobalStatCacheKey key;
    TimestampTz updatedAt;
    bool refreshing;
    char stat[BM25_GLOBAL_STAT_VALUE_LEN];
} Bm25GlobalStatCacheEntry;

static HTAB* g_bm25GlobalStatCache = NULL;
static pthread_rwlock_t g_bm25GlobalStatCacheLock;
static pthread_once_t g_bm25GlobalStatCacheOnce = PTHREAD_ONCE_INIT;

static void Bm25GlobalStatCacheInitializeOnce(void)
{
    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Bm25GlobalStatCacheKey);
    ctl.entrysize = sizeof(Bm25GlobalStatCacheEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    ctl.dsize = ctl.max_dsize = hash_select_dirsize(BM25_GLOBAL_STAT_CACHE_MAX_ENTRIES);
    g_bm25GlobalStatCache =
        hash_create("SPQ BM25 global statistics", BM25_GLOBAL_STAT_CACHE_MAX_ENTRIES,
                    &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_DIRSIZE);
    PthreadRwLockInit(&g_bm25GlobalStatCacheLock, NULL);
}

void InitializeBm25GlobalStatCache(void)
{
    int rc = pthread_once(&g_bm25GlobalStatCacheOnce, Bm25GlobalStatCacheInitializeOnce);
    if (rc != 0) {
        ereport(ERROR,
                (errmsg("could not initialize BM25 global statistics cache: %d", rc)));
    }
}

static Bm25GlobalStatCacheKey Bm25GlobalStatCacheMakeKey(Oid databaseOid, Oid relationOid,
                                                         const char* queryText)
{
    Bm25GlobalStatCacheKey key;
    errno_t rc = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "\0", "\0");
    key.databaseOid = databaseOid;
    key.relationOid = relationOid;
    key.queryLength = strlen(queryText);
    key.queryHash =
        hash_any(reinterpret_cast<const unsigned char*>(queryText), key.queryLength);
    StringInfoData reversed;
    initStringInfo(&reversed);
    enlargeStringInfo(&reversed, key.queryLength);
    for (int64 index = (int64)key.queryLength - 1; index >= 0; --index) {
        appendStringInfoCharMacro(&reversed, queryText[index]);
    }
    key.reverseQueryHash =
        hash_any(reinterpret_cast<const unsigned char*>(reversed.data), reversed.len);
    pfree(reversed.data);
    return key;
}

static Bm25GlobalStatCacheEntry* Bm25GlobalStatCacheOldestEntry(void)
{
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, g_bm25GlobalStatCache);
    Bm25GlobalStatCacheEntry* oldest = NULL;
    Bm25GlobalStatCacheEntry* entry = NULL;
    while ((entry = static_cast<Bm25GlobalStatCacheEntry*>(hash_seq_search(&status))) !=
           NULL) {
        if (!entry->refreshing &&
            (oldest == NULL || entry->updatedAt < oldest->updatedAt)) {
            oldest = entry;
        }
    }
    return oldest;
}

Bm25GlobalStatCacheResult Bm25GlobalStatCacheLookup(Oid databaseOid, Oid relationOid,
                                                    const char* queryText, char** statOut)
{
    InitializeBm25GlobalStatCache();
    *statOut = NULL;
    Bm25GlobalStatCacheKey key =
        Bm25GlobalStatCacheMakeKey(databaseOid, relationOid, queryText);
    TimestampTz now = GetCurrentTimestamp();

    AutoRWLock readLock(&g_bm25GlobalStatCacheLock);
    readLock.RdLock();
    Bm25GlobalStatCacheEntry* entry = static_cast<Bm25GlobalStatCacheEntry*>(
        hash_search(g_bm25GlobalStatCache, &key, HASH_FIND, NULL));
    if (entry != NULL && entry->stat[0] != '\0' &&
        now - entry->updatedAt <= BM25_GLOBAL_STAT_CACHE_TTL_US) {
        *statOut = pstrdup(entry->stat);
        readLock.UnLock();
        return BM25_GLOBAL_STAT_CACHE_HIT;
    }
    if (entry != NULL && entry->refreshing) {
        readLock.UnLock();
        return BM25_GLOBAL_STAT_CACHE_FALLBACK;
    }
    readLock.UnLock();

    AutoRWLock writeLock(&g_bm25GlobalStatCacheLock);
    writeLock.WrLock();
    bool found = false;
    entry = static_cast<Bm25GlobalStatCacheEntry*>(
        hash_search(g_bm25GlobalStatCache, &key, HASH_ENTER_NULL, &found));
    if (entry == NULL) {
        Bm25GlobalStatCacheEntry* oldest = Bm25GlobalStatCacheOldestEntry();
        if (oldest != NULL) {
            Bm25GlobalStatCacheKey oldestKey = oldest->key;
            (void)hash_search(g_bm25GlobalStatCache, &oldestKey, HASH_REMOVE, NULL);
            entry = static_cast<Bm25GlobalStatCacheEntry*>(
                hash_search(g_bm25GlobalStatCache, &key, HASH_ENTER_NULL, &found));
        }
        if (entry == NULL) {
            writeLock.UnLock();
            return BM25_GLOBAL_STAT_CACHE_BYPASS;
        }
    }
    if (found && entry->stat[0] != '\0' &&
        now - entry->updatedAt <= BM25_GLOBAL_STAT_CACHE_TTL_US) {
        *statOut = pstrdup(entry->stat);
        writeLock.UnLock();
        return BM25_GLOBAL_STAT_CACHE_HIT;
    }
    if (found && entry->refreshing) {
        writeLock.UnLock();
        return BM25_GLOBAL_STAT_CACHE_FALLBACK;
    }
    if (!found) {
        errno_t rc = memset_s(((char*)entry) + sizeof(entry->key),
                              sizeof(*entry) - sizeof(entry->key), 0,
                              sizeof(*entry) - sizeof(entry->key));
        securec_check(rc, "\0", "\0");
    }
    entry->refreshing = true;
    writeLock.UnLock();
    return BM25_GLOBAL_STAT_CACHE_REFRESH;
}

void Bm25GlobalStatCacheStore(Oid databaseOid, Oid relationOid, const char* queryText,
                              const char* stat)
{
    InitializeBm25GlobalStatCache();
    Bm25GlobalStatCacheKey key =
        Bm25GlobalStatCacheMakeKey(databaseOid, relationOid, queryText);
    AutoRWLock writeLock(&g_bm25GlobalStatCacheLock);
    writeLock.WrLock();
    bool found = false;
    Bm25GlobalStatCacheEntry* entry = static_cast<Bm25GlobalStatCacheEntry*>(
        hash_search(g_bm25GlobalStatCache, &key, HASH_ENTER_NULL, &found));
    if (entry != NULL) {
        if (!found) {
            errno_t rc = memset_s(((char*)entry) + sizeof(entry->key),
                                  sizeof(*entry) - sizeof(entry->key), 0,
                                  sizeof(*entry) - sizeof(entry->key));
            securec_check(rc, "\0", "\0");
        }
        size_t statLength = strlen(stat);
        if (statLength < sizeof(entry->stat)) {
            errno_t rc = strncpy_s(entry->stat, sizeof(entry->stat), stat, statLength);
            securec_check(rc, "\0", "\0");
            entry->updatedAt = GetCurrentTimestamp();
        } else {
            entry->stat[0] = '\0';
            entry->updatedAt = 0;
        }
        entry->refreshing = false;
    }
    writeLock.UnLock();
}

void Bm25GlobalStatCacheAbortRefresh(Oid databaseOid, Oid relationOid,
                                     const char* queryText)
{
    InitializeBm25GlobalStatCache();
    Bm25GlobalStatCacheKey key =
        Bm25GlobalStatCacheMakeKey(databaseOid, relationOid, queryText);
    AutoRWLock writeLock(&g_bm25GlobalStatCacheLock);
    writeLock.WrLock();
    Bm25GlobalStatCacheEntry* entry = static_cast<Bm25GlobalStatCacheEntry*>(
        hash_search(g_bm25GlobalStatCache, &key, HASH_FIND, NULL));
    if (entry != NULL) {
        entry->refreshing = false;
    }
    writeLock.UnLock();
}
