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
 * spq_version.cc
 *
 * This file contains functions for displaying the Citus version string
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"

#include "spq_version.h"
#include "utils/uuid.h"

PG_FUNCTION_INFO_V1(spq_version);
PG_FUNCTION_INFO_V1(spq_server_id);

extern "C" Datum spq_version(PG_FUNCTION_ARGS);
extern "C" Datum spq_server_id(PG_FUNCTION_ARGS);

/* GIT_VERSION is passed in as a compiler flag during builds that have git installed */
#ifdef GIT_VERSION
#define GIT_REF " gitref: " GIT_VERSION
#else
#define GIT_REF
#endif

Datum spq_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(SPQ_VERSION_STR GIT_REF));
}

/*
 * citus_server_id returns a random UUID value as server identifier. This is
 * modeled after PostgreSQL's pg_random_uuid().
 */
Datum spq_server_id(PG_FUNCTION_ARGS)
{
    uint8* buf = (uint8*)palloc(UUID_LEN);

    for (int bufIdx = 0; bufIdx < UUID_LEN; bufIdx++) {
        buf[bufIdx] = (uint8)(random() & 0xFF);
    }

    /*
     * Set magic numbers for a "version 4" (pseudorandom) UUID, see
     * http://tools.ietf.org/html/rfc4122#section-4.4
     */
    buf[6] = (buf[6] & 0x0f) | 0x40; /* "version" field */
    buf[8] = (buf[8] & 0x3f) | 0x80; /* "variant" field */

    PG_RETURN_UUID_P((pg_uuid_t*)buf);
}
