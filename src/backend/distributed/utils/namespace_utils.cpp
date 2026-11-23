/*
 * spq_plugin_v2 - distributed query processing for openGauss.
 * Copyright (C) 2026 Huawei Technologies Co., Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

/*-------------------------------------------------------------------------
 *
 * namespace_utils.c
 *
 * Utility functions related to namespace changes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"

#include "distributed/namespace_utils.h"

/*
 * We use the equivalent of a function SET option to allow the setting to
 * persist for the exact duration of the transaction, guc.c takes care of
 * undoing the setting on error.
 *
 * We set search_path to "pg_catalog" instead of "" to expose useful utilities.
 */
int PushEmptySearchPath()
{
    int saveNestLevel = NewGUCNestLevel();
    (void)set_config_option("search_path", "pg_catalog", PGC_USERSET, PGC_S_SESSION,
                            GUC_ACTION_SAVE, true, 0, false);
    return saveNestLevel;
}

/*
 * Restore the GUC variable search_path we set in PushEmptySearchPath
 */
void PopEmptySearchPath(int saveNestLevel)
{
    AtEOXact_GUC(true, saveNestLevel);
}
