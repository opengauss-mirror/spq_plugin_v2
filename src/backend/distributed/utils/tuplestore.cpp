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
 * tuplestore.c
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "distributed/tuplestore.h"

static ReturnSetInfo* CheckTuplestoreReturnSetInfo(FunctionCallInfo fcinfo);
static void CheckTuplestoreReturnType(FunctionCallInfo fcinfo, TupleDesc* tupdesc);
/*
 * CheckTuplestoreReturn checks if a tuplestore can be returned in the callsite
 * of the UDF.
 */
static ReturnSetInfo* CheckTuplestoreReturnSetInfo(FunctionCallInfo fcinfo)
{
    ReturnSetInfo* returnSetInfo = (ReturnSetInfo*)fcinfo->resultinfo;

    /* check to see if caller supports us returning a tuplestore */
    if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot "
                               "accept a set")));
    }
    if (!(returnSetInfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not "
                               "allowed in this context")));
    }
    return returnSetInfo;
}

static void CheckTuplestoreReturnType(FunctionCallInfo fcinfo, TupleDesc* tupdesc)
{
    switch (get_call_result_type(fcinfo, NULL, tupdesc)) {
        case TYPEFUNC_COMPOSITE: {
            /* success */
            break;
        }

        case TYPEFUNC_RECORD: {
            /* failed to determine actual type of RECORD */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("function returning record called in context "
                                   "that cannot accept type record")));
            break;
        }

        default: {
            /* result type isn't composite */
            elog(ERROR, "return type must be a row type");
            break;
        }
    }
}
/*
 * SetupTuplestore sets up a tuplestore for returning data.
 */
Tuplestorestate* SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc* tupleDescriptor)
{
    ReturnSetInfo* resultSet = CheckTuplestoreReturnSetInfo(fcinfo);
    /* In openGauss, resultset's tuple descriptor should be allocated from per-query
    context, the caller will free it. */
    MemoryContext perQueryContext = resultSet->econtext->ecxt_per_query_memory;
    MemoryContext oldContext = MemoryContextSwitchTo(perQueryContext);

    CheckTuplestoreReturnType(fcinfo, tupleDescriptor);

    /* dynamic tuple descriptor, copy it. */
    if (((*tupleDescriptor) == resultSet->expectedDesc) &&
        (*tupleDescriptor)->tdrefcount == -1) {
        *tupleDescriptor = CopyTupleDesc(resultSet->expectedDesc);
    }

    Tuplestorestate* tupstore =
        tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    resultSet->returnMode = SFRM_Materialize;
    resultSet->setResult = tupstore;
    resultSet->setDesc = *tupleDescriptor;
    MemoryContextSwitchTo(oldContext);

    return tupstore;
}
