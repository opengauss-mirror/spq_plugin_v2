/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 */
#include "postgres.h"

#include "access/hash.h"
#include "knl/knl_instance.h"
#include "lib/stringinfo.h"
#include "storage/barrier.h"
#include "utils/atomic.h"
#include "utils/timestamp.h"

#include "distributed/bm25_global_stat_cache.h"

static const uint32 BM25_TERM_CACHE_BUCKET_COUNT = 16384;
static const uint32 BM25_STAT_CACHE_BUCKET_COUNT = 256;
static const uint32 BM25_CACHE_ASSOCIATIVITY = 4;

int Bm25GlobalStatCacheTtlSeconds = 3600;

typedef struct Bm25GlobalStatKey {
    Oid databaseOid;
    Oid relationOid;
    AttrNumber columnAttnum;
} Bm25GlobalStatKey;

typedef struct Bm25GlobalStatEntry {
    pg_atomic_uint64 version;
    Bm25GlobalStatKey key;
    uint64 documentCount;
    uint64 tokenCount;
    TimestampTz updatedAt;
} Bm25GlobalStatEntry;

typedef struct Bm25GlobalTermEntry {
    pg_atomic_uint64 version;
    Bm25GlobalStatKey key;
    uint32 termHash;
    uint64 documentFrequency;
    TimestampTz updatedAt;
    char term[BM25_GLOBAL_STAT_TERM_LEN];
} Bm25GlobalTermEntry;

static Bm25GlobalStatEntry* g_bm25GlobalStatCache = NULL;
static Bm25GlobalTermEntry* g_bm25GlobalTermCache = NULL;
static pthread_once_t g_bm25GlobalStatCacheOnce = PTHREAD_ONCE_INIT;

static void Bm25GlobalStatCacheInitializeOnce(void)
{
    MemoryContext cacheContext = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    MemoryContext oldContext = MemoryContextSwitchTo(cacheContext);
    g_bm25GlobalStatCache = static_cast<Bm25GlobalStatEntry*>(
        palloc0(BM25_STAT_CACHE_BUCKET_COUNT * BM25_CACHE_ASSOCIATIVITY *
                sizeof(Bm25GlobalStatEntry)));
    g_bm25GlobalTermCache = static_cast<Bm25GlobalTermEntry*>(
        palloc0(BM25_TERM_CACHE_BUCKET_COUNT * BM25_CACHE_ASSOCIATIVITY *
                sizeof(Bm25GlobalTermEntry)));
    MemoryContextSwitchTo(oldContext);
}

void InitializeBm25GlobalStatCache(void)
{
    int rc = pthread_once(&g_bm25GlobalStatCacheOnce, Bm25GlobalStatCacheInitializeOnce);
    if (rc != 0) {
        ereport(ERROR,
                (errmsg("could not initialize BM25 global statistics cache: %d", rc)));
    }
}

static uint32 Bm25GlobalStatKeyHash(const Bm25GlobalStatKey* key)
{
    return hash_any(reinterpret_cast<const unsigned char*>(key), sizeof(*key));
}

static bool Bm25GlobalStatKeyEquals(const Bm25GlobalStatKey* left,
                                    const Bm25GlobalStatKey* right)
{
    return left->databaseOid == right->databaseOid &&
           left->relationOid == right->relationOid &&
           left->columnAttnum == right->columnAttnum;
}

template <typename Entry>
static bool Bm25CacheReadStable(const Entry* source, Entry* snapshot)
{
    uint64 before = pg_atomic_barrier_read_u64(
        const_cast<pg_atomic_uint64*>(&source->version));
    if (before == 0 || (before & 1) != 0) {
        return false;
    }
    errno_t rc = memcpy_s(snapshot, sizeof(*snapshot), source, sizeof(*source));
    securec_check(rc, "\0", "\0");
    pg_read_barrier();
    uint64 after = pg_atomic_barrier_read_u64(
        const_cast<pg_atomic_uint64*>(&source->version));
    return before == after && (after & 1) == 0;
}

template <typename Entry>
static bool Bm25CacheTryWrite(Entry* target, const Entry* value)
{
    uint64 version = pg_atomic_barrier_read_u64(&target->version);
    if ((version & 1) != 0 ||
        !pg_atomic_compare_exchange_u64(&target->version, &version, version + 1)) {
        return false;
    }
    errno_t rc = memcpy_s(reinterpret_cast<char*>(target) + sizeof(target->version),
                          sizeof(*target) - sizeof(target->version),
                          reinterpret_cast<const char*>(value) + sizeof(value->version),
                          sizeof(*value) - sizeof(value->version));
    securec_check(rc, "\0", "\0");
    pg_write_barrier();
    pg_atomic_write_u64(&target->version, version + 2);
    return true;
}

static bool Bm25GlobalStatCacheRead(const Bm25GlobalStatKey* key,
                                    TimestampTz now, uint64* documentCount,
                                    uint64* tokenCount)
{
    uint32 bucket = Bm25GlobalStatKeyHash(key) & (BM25_STAT_CACHE_BUCKET_COUNT - 1);
    int64 ttl = static_cast<int64>(Bm25GlobalStatCacheTtlSeconds) * USECS_PER_SEC;
    for (uint32 way = 0; way < BM25_CACHE_ASSOCIATIVITY; ++way) {
        Bm25GlobalStatEntry snapshot;
        Bm25GlobalStatEntry* entry =
            &g_bm25GlobalStatCache[bucket * BM25_CACHE_ASSOCIATIVITY + way];
        if (Bm25CacheReadStable(entry, &snapshot) &&
            Bm25GlobalStatKeyEquals(&snapshot.key, key) &&
            snapshot.documentCount > 0 && snapshot.tokenCount >= snapshot.documentCount &&
            now - snapshot.updatedAt <= ttl) {
            *documentCount = snapshot.documentCount;
            *tokenCount = snapshot.tokenCount;
            return true;
        }
    }
    return false;
}

static bool Bm25GlobalTermCacheRead(const Bm25GlobalStatKey* key, const char* term,
                                    TimestampTz now, uint64* documentFrequency)
{
    uint32 termHash = hash_any(reinterpret_cast<const unsigned char*>(term), strlen(term));
    uint32 bucket = (Bm25GlobalStatKeyHash(key) ^ termHash) &
                    (BM25_TERM_CACHE_BUCKET_COUNT - 1);
    int64 ttl = static_cast<int64>(Bm25GlobalStatCacheTtlSeconds) * USECS_PER_SEC;
    for (uint32 way = 0; way < BM25_CACHE_ASSOCIATIVITY; ++way) {
        Bm25GlobalTermEntry snapshot;
        Bm25GlobalTermEntry* entry =
            &g_bm25GlobalTermCache[bucket * BM25_CACHE_ASSOCIATIVITY + way];
        if (Bm25CacheReadStable(entry, &snapshot) &&
            snapshot.termHash == termHash &&
            Bm25GlobalStatKeyEquals(&snapshot.key, key) &&
            strncmp(snapshot.term, term, sizeof(snapshot.term)) == 0 &&
            snapshot.documentFrequency > 0 &&
            now - snapshot.updatedAt <= ttl) {
            *documentFrequency = snapshot.documentFrequency;
            return true;
        }
    }
    return false;
}

bool Bm25GlobalStatCacheLookup(Oid databaseOid, Oid relationOid,
                               AttrNumber columnAttnum,
                               const char* const* terms, int termCount,
                               char** statOut)
{
    InitializeBm25GlobalStatCache();
    *statOut = NULL;
    if (terms == NULL || termCount <= 0) {
        return false;
    }

    Bm25GlobalStatKey key = {databaseOid, relationOid, columnAttnum};
    TimestampTz now = GetCurrentTimestamp();
    uint64 documentCount = 0;
    uint64 tokenCount = 0;
    if (!Bm25GlobalStatCacheRead(&key, now, &documentCount, &tokenCount)) {
        return false;
    }

    StringInfoData stat;
    initStringInfo(&stat);
    appendStringInfo(&stat, "N=%lu;T=%lu;", (unsigned long)documentCount,
                     (unsigned long)tokenCount);
    for (int index = 0; index < termCount; ++index) {
        uint64 documentFrequency = 0;
        if (!Bm25GlobalTermCacheRead(&key, terms[index], now, &documentFrequency)) {
            pfree(stat.data);
            return false;
        }
        if (documentFrequency > documentCount || documentFrequency > UINT_MAX) {
            pfree(stat.data);
            return false;
        }
        appendStringInfo(&stat, "%s%s:%lu", index == 0 ? "" : ",", terms[index],
                         (unsigned long)documentFrequency);
    }
    *statOut = stat.data;
    return true;
}

static Bm25GlobalStatEntry* Bm25GlobalStatCacheChooseSlot(
    const Bm25GlobalStatKey* key)
{
    uint32 bucket = Bm25GlobalStatKeyHash(key) & (BM25_STAT_CACHE_BUCKET_COUNT - 1);
    Bm25GlobalStatEntry* oldest = NULL;
    TimestampTz oldestUpdatedAt = PG_INT64_MAX;
    for (uint32 way = 0; way < BM25_CACHE_ASSOCIATIVITY; ++way) {
        Bm25GlobalStatEntry snapshot;
        Bm25GlobalStatEntry* entry =
            &g_bm25GlobalStatCache[bucket * BM25_CACHE_ASSOCIATIVITY + way];
        if (!Bm25CacheReadStable(entry, &snapshot)) {
            if (pg_atomic_barrier_read_u64(&entry->version) == 0) {
                return entry;
            }
            continue;
        }
        if (Bm25GlobalStatKeyEquals(&snapshot.key, key)) {
            return entry;
        }
        if (snapshot.updatedAt < oldestUpdatedAt) {
            oldest = entry;
            oldestUpdatedAt = snapshot.updatedAt;
        }
    }
    return oldest;
}

static Bm25GlobalTermEntry* Bm25GlobalTermCacheChooseSlot(
    const Bm25GlobalStatKey* key, const char* term, uint32 termHash)
{
    uint32 bucket = (Bm25GlobalStatKeyHash(key) ^ termHash) &
                    (BM25_TERM_CACHE_BUCKET_COUNT - 1);
    Bm25GlobalTermEntry* oldest = NULL;
    TimestampTz oldestUpdatedAt = PG_INT64_MAX;
    for (uint32 way = 0; way < BM25_CACHE_ASSOCIATIVITY; ++way) {
        Bm25GlobalTermEntry snapshot;
        Bm25GlobalTermEntry* entry =
            &g_bm25GlobalTermCache[bucket * BM25_CACHE_ASSOCIATIVITY + way];
        if (!Bm25CacheReadStable(entry, &snapshot)) {
            if (pg_atomic_barrier_read_u64(&entry->version) == 0) {
                return entry;
            }
            continue;
        }
        if (snapshot.termHash == termHash &&
            Bm25GlobalStatKeyEquals(&snapshot.key, key) &&
            strncmp(snapshot.term, term, sizeof(snapshot.term)) == 0) {
            return entry;
        }
        if (snapshot.updatedAt < oldestUpdatedAt) {
            oldest = entry;
            oldestUpdatedAt = snapshot.updatedAt;
        }
    }
    return oldest;
}

static bool Bm25GlobalStatParseUnsigned(const char* value, uint64* parsed)
{
    if (value == NULL || value[0] == '\0') {
        return false;
    }
    for (const char* cursor = value; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
    }
    errno = 0;
    char* end = NULL;
    unsigned long long number = strtoull(value, &end, 10);
    if (errno == ERANGE || end == value || *end != '\0') {
        return false;
    }
    *parsed = static_cast<uint64>(number);
    return true;
}

void Bm25GlobalStatCacheStore(Oid databaseOid, Oid relationOid,
                              AttrNumber columnAttnum, const char* stat)
{
    InitializeBm25GlobalStatCache();
    if (stat == NULL || stat[0] == '\0') {
        return;
    }

    char* buffer = pstrdup(stat);
    char* firstSemi = strchr(buffer, ';');
    char* secondSemi = firstSemi == NULL ? NULL : strchr(firstSemi + 1, ';');
    if (firstSemi == NULL || secondSemi == NULL) {
        pfree(buffer);
        return;
    }
    *firstSemi = '\0';
    *secondSemi = '\0';
    uint64 documentCount = 0;
    uint64 tokenCount = 0;
    if (strncmp(buffer, "N=", 2) != 0 ||
        strncmp(firstSemi + 1, "T=", 2) != 0 ||
        !Bm25GlobalStatParseUnsigned(buffer + 2, &documentCount) ||
        !Bm25GlobalStatParseUnsigned(firstSemi + 3, &tokenCount) ||
        documentCount == 0 || tokenCount < documentCount) {
        pfree(buffer);
        return;
    }

    Bm25GlobalStatKey key = {databaseOid, relationOid, columnAttnum};
    TimestampTz now = GetCurrentTimestamp();
    Bm25GlobalStatEntry statValue = {};
    statValue.key = key;
    statValue.documentCount = documentCount;
    statValue.tokenCount = tokenCount;
    statValue.updatedAt = now;
    Bm25GlobalStatEntry* statSlot = Bm25GlobalStatCacheChooseSlot(&key);
    if (statSlot != NULL) {
        (void)Bm25CacheTryWrite(statSlot, &statValue);
    }

    char* pairContext = NULL;
    for (char* pair = strtok_r(secondSemi + 1, ",", &pairContext);
         pair != NULL; pair = strtok_r(NULL, ",", &pairContext)) {
        char* colon = strchr(pair, ':');
        if (colon == NULL || colon == pair) {
            continue;
        }
        *colon = '\0';
        uint64 documentFrequency = 0;
        if (strlen(pair) >= BM25_GLOBAL_STAT_TERM_LEN ||
            !Bm25GlobalStatParseUnsigned(colon + 1, &documentFrequency) ||
            documentFrequency == 0 || documentFrequency > documentCount) {
            continue;
        }
        uint32 termHash =
            hash_any(reinterpret_cast<const unsigned char*>(pair), strlen(pair));
        Bm25GlobalTermEntry termValue = {};
        termValue.key = key;
        termValue.termHash = termHash;
        termValue.documentFrequency = documentFrequency;
        termValue.updatedAt = now;
        errno_t rc = strncpy_s(termValue.term, sizeof(termValue.term), pair,
                               sizeof(termValue.term) - 1);
        securec_check(rc, "\0", "\0");
        Bm25GlobalTermEntry* termSlot =
            Bm25GlobalTermCacheChooseSlot(&key, pair, termHash);
        if (termSlot != NULL) {
            (void)Bm25CacheTryWrite(termSlot, &termValue);
        }
    }
    pfree(buffer);
}
