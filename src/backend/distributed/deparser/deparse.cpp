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
 * deparse.c
 *    Entrypoint for deparsing parsetrees.
 *
 *    The goal of deparsing parsetrees is to reconstruct sql statements
 *    from any parsed sql statement by ParseTreeNode. Deparsed statements
 *    can be used to reapply them on remote postgres nodes like the citus
 *    workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

/*
 * DeparseTreeNode aims to be the inverse of postgres' ParseTreeNode. Currently with
 * limited support. Check support before using, and add support for new statements as
 * required.
 */
char* DeparseTreeNode(Node* stmt)
{
    const DistributeObjectOps* ops = GetDistributeObjectOps(stmt);

    if (!ops->deparse) {
        ereport(ERROR, (errmsg("unsupported statement for deparsing")));
    }

    return ops->deparse(stmt);
}

/*
 * DeparseTreeNodes deparses all stmts in the list from the statement datastructure into
 * sql statements.
 */
List* DeparseTreeNodes(List* stmts)
{
    List* sqls = NIL;
    Node* stmt = NULL;
    foreach_declared_ptr(stmt, stmts)
    {
        sqls = lappend(sqls, DeparseTreeNode(stmt));
    }
    return sqls;
}
