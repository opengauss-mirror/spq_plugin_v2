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
 * param_utils.c
 *	  Utilities to process paramaters.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

#include "distributed/param_utils.h"

/*
 * IsExternParamUsedInQuery returns true if the passed in paramId
 * is used in the query, false otherwise.
 */
bool GetParamsUsedInQuery(Node* expression, Bitmapset** paramBitmap)
{
    if (expression == NULL) {
        return false;
    }

    if (IsA(expression, Param)) {
        Param* param = (Param*)expression;
        int paramId = param->paramid;

        /* only care about user supplied parameters */
        if (param->paramkind != PARAM_EXTERN) {
            return false;
        }

        /* Found a parameter, mark it in the bitmap and continue */
        *paramBitmap = bms_add_member(*paramBitmap, paramId);

        /* Continue searching */
        return false;
    }

    /* keep traversing */
    if (IsA(expression, Query)) {
        return query_tree_walker((Query*)expression,
                                 reinterpret_cast<bool (*)()>(GetParamsUsedInQuery),
                                 paramBitmap, 0);
    } else {
        return expression_tree_walker(
            expression, reinterpret_cast<bool (*)()>(GetParamsUsedInQuery), paramBitmap);
    }
}

/*
 * MarkUnreferencedExternParams marks parameter's type to zero if the
 * parameter is not used in the query.
 */
void MarkUnreferencedExternParams(Node* expression, ParamListInfo boundParams)
{
    int parameterCount = boundParams->numParams;
    Bitmapset* paramBitmap = NULL;

    /* Fetch all parameters used in the query */
    GetParamsUsedInQuery(expression, &paramBitmap);

    /* Check for any missing parameters */
    for (int parameterNum = 1; parameterNum <= parameterCount; parameterNum++) {
        if (!bms_is_member(parameterNum, paramBitmap)) {
            boundParams->params[parameterNum - 1].ptype = 0;
        }
    }
}
