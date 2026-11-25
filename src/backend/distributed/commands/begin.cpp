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
 * begin.c
 *    Processing of the BEGIN command.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"

#include "nodes/parsenodes.h"

#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/transaction_management.h"
#include "distributed/session_ctx.h"

/*
 * SaveBeginCommandProperties stores the transaction properties passed
 * via BEGIN.
 */
void SaveBeginCommandProperties(TransactionStmt* transactionStmt)
{
    DefElem* item = NULL;

    /*
     * This loop is similar to the one in standard_ProcessUtility.
     *
     * While BEGIN can be quite frequent it will rarely have options set.
     */
    foreach_declared_ptr(item, transactionStmt->options)
    {
        A_Const* constant = (A_Const*)item->arg;

        if (strcmp(item->defname, "transaction_read_only") == 0) {
            if (intVal(&constant->val) == 1) {
                Session_ctx::Trans().BeginXactReadOnly = BeginXactReadOnly_Enabled;
            } else {
                Session_ctx::Trans().BeginXactReadOnly = BeginXactReadOnly_Disabled;
            }
        } else if (strcmp(item->defname, "transaction_deferrable") == 0) {
            if (intVal(&constant->val) == 1) {
                Session_ctx::Trans().BeginXactDeferrable = BeginXactDeferrable_Enabled;
            } else {
                Session_ctx::Trans().BeginXactDeferrable = BeginXactDeferrable_Disabled;
            }
        }
    }
}
