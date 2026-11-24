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
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#ifdef DISABLE_OG_COMMENTS
#include "common/cryptohash.h"
#include "common/sha2.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"

#include "pg_version_constants.h"

#include "distributed/errormessage.h"
#include "distributed/log_utils.h"

/*
 * IsLoggableLevel returns true if either of client or server log guc is configured to
 * log the given log level.
 * In postgres, log can be configured differently for clients and servers.
 */
bool IsLoggableLevel(int logLevel)
{
    return log_min_messages <= logLevel || client_min_messages <= logLevel;
}
