/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*-------------------------------------------------------------------------
 *
 * hash_helpers.c
 *   Helpers for dynahash.c style hash tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#ifdef DISABLE_OG_COMMENTS
#include "common/hashfn.h"
#endif
#include "utils/hsearch.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/hash_helpers.h"

#define LOCAL_FCINFO(name, nargs)                                        \
    /* use union with FunctionCallInfoBaseData to guarantee alignment */ \
    FunctionCallInfoData name##fcinfo;                                   \
    FunctionCallInfo name = &name##fcinfo;

/*
 * Empty a hash, without destroying the hash table itself.
 */
void hash_delete_all(HTAB* htab)
{
    HASH_SEQ_STATUS status;
    void* entry = NULL;

    hash_seq_init(&status, htab);
    while ((entry = hash_seq_search(&status)) != 0) {
        bool found = false;

        hash_search(htab, entry, HASH_REMOVE, &found);
        Assert(found);
    }
}

/*
 * CreateSimpleHashWithNameAndSize creates a hashmap that hashes its key using
 * tag_hash function and stores the entries in the current memory context.
 */
HTAB* CreateSimpleHashWithNameAndSizeInternal(Size keySize, Size entrySize, char* name,
                                              long nelem)
{
    HASHCTL info;
    memset_struct_0(info);
    info.keysize = keySize;
    info.entrysize = entrySize;
    info.hcxt = CurrentMemoryContext;

    int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

    HTAB* publicationInfoHash = hash_create(name, nelem, &info, hashFlags);
    return publicationInfoHash;
}

/*
 * foreach_htab_cleanup cleans up the hash iteration state after the iteration
 * is done. This is only needed when break statements are present in the
 * foreach block.
 */
void foreach_htab_cleanup(void* var, HASH_SEQ_STATUS* status)
{
    if ((var) != NULL) {
        hash_seq_term(status);
    }
}
