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
 * function.c
 *
 * Utility functions for dealing with functions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "commands/defrem.h"
#include "utils/lsyscache.h"

#include "distributed/utils/function.h"

/*
 * GetFunctionInfo first resolves the operator for the given data type, access
 * method, and support procedure. The function then uses the resolved operator's
 * identifier to fill in a function manager object, and returns this object.
 */
FmgrInfo* GetFunctionInfo(Oid typeId, Oid accessMethodId, int16 procedureId)
{
    FmgrInfo* functionInfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo));

    /* get default operator class from pg_opclass for datum type */
    Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

    Oid operatorFamilyId = get_opclass_family(operatorClassId);
    Oid operatorClassInputType = get_opclass_input_type(operatorClassId);

    Oid operatorId = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
                                       operatorClassInputType, procedureId);

    if (operatorId == InvalidOid) {
        ereport(ERROR, (errmsg("could not find function for data typeId %u", typeId)));
    }

    /* fill in the FmgrInfo struct using the operatorId */
    fmgr_info(operatorId, functionInfo);

    return functionInfo;
}
