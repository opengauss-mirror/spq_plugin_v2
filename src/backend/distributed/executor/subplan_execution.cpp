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
 * subplan_execution.c
 *
 * Functions for execution subplans prior to distributed table execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "utils/datetime.h"

#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/subplan_execution.h"
#include "distributed/session_ctx.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"

#define SECOND_TO_MILLI_SECOND 1000
#define MICRO_TO_MILLI_SECOND 0.001

/*
 * ExecuteSubPlans executes a list of subplans from a distributed plan
 * by sequentially executing each plan from the top.
 */
void ExecuteSubPlans(DistributedPlan* distributedPlan)
{
    uint64 planId = distributedPlan->planId;
    List* subPlanList = distributedPlan->subPlanList;

    if (subPlanList == NIL) {
        /* no subplans to execute */
        return;
    }

    HTAB* intermediateResultsHash = MakeIntermediateResultHTAB();
    RecordSubplanExecutionsOnNodes(intermediateResultsHash, distributedPlan);

    /*
     * Make sure that this transaction has a distributed transaction ID.
     *
     * Intermediate results of subplans will be stored in a directory that is
     * derived from the distributed transaction ID.
     */
    UseCoordinatedTransaction();

    DistributedSubPlan* subPlan = NULL;
    foreach_declared_ptr(subPlan, subPlanList)
    {
        PlannedStmt* plannedStmt = subPlan->plan;
        uint32 subPlanId = subPlan->subPlanId;
        ParamListInfo params = NULL;
        char* resultId = GenerateResultId(planId, subPlanId);
        List* remoteWorkerNodeList =
            FindAllWorkerNodesUsingSubplan(intermediateResultsHash, resultId);

        IntermediateResultsHashEntry* entry =
            SearchIntermediateResult(intermediateResultsHash, resultId);

        Session_ctx::ExecCtx().SubPlanLevel++;
        EState* estate = CreateExecutorState();
        DestReceiver* copyDest = CreateRemoteFileDestReceiver(
            resultId, estate, remoteWorkerNodeList, entry->writeLocalFile);

        TimestampTz startTimestamp = GetCurrentTimestamp();

        ExecutePlanIntoDestReceiver(plannedStmt, params, copyDest);

        /*
         * EXPLAIN ANALYZE instrumentations. Calculating these are very light-weight,
         * so always populate them regardless of EXPLAIN ANALYZE or not.
         */
        long durationSeconds = 0.0;
        int durationMicrosecs = 0;
        TimestampDifference(startTimestamp, GetCurrentTimestamp(), &durationSeconds,
                            &durationMicrosecs);

        subPlan->durationMillisecs = durationSeconds * SECOND_TO_MILLI_SECOND;
        subPlan->durationMillisecs += durationMicrosecs * MICRO_TO_MILLI_SECOND;

        subPlan->bytesSentPerWorker = RemoteFileDestReceiverBytesSent(copyDest);
        subPlan->remoteWorkerCount = list_length(remoteWorkerNodeList);
        subPlan->writeLocalFile = entry->writeLocalFile;

        Session_ctx::ExecCtx().SubPlanLevel--;
        FreeExecutorState(estate);
    }
}
