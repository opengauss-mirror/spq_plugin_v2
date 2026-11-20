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
 * deparse_owned_stmts.c
 *	  Functions to turn all Statement structures related to owned back
 *    into sql.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

static void AppendDropOwnedStmt(StringInfo buf, DropOwnedStmt* stmt);
static void AppendRoleList(StringInfo buf, List* roleList);

/*
 * DeparseDropOwnedStmt builds and returns a string representing of the
 * DropOwnedStmt for application on a remote server.
 */
char* DeparseDropOwnedStmt(Node* node)
{
    DropOwnedStmt* stmt = castNode(DropOwnedStmt, node);

    StringInfoData buf = {0};
    initStringInfo(&buf);

    AppendDropOwnedStmt(&buf, stmt);

    return buf.data;
}

/*
 * AppendDropOwnedStmt generates the string representation of the
 * DropOwnedStmt and appends it to the buffer.
 */
static void AppendDropOwnedStmt(StringInfo buf, DropOwnedStmt* stmt)
{
    appendStringInfo(buf, "DROP OWNED BY ");

    AppendRoleList(buf, stmt->roles);

    if (stmt->behavior == DROP_RESTRICT) {
        appendStringInfo(buf, " RESTRICT");
    } else if (stmt->behavior == DROP_CASCADE) {
        appendStringInfo(buf, " CASCADE");
    }
}

static void AppendRoleList(StringInfo buf, List* roleList)
{
    ListCell* cell = NULL;
    foreach (cell, roleList) {
        Node* roleNode = (Node*)lfirst(cell);
#ifdef DISABLE_OG_COMMENTS
        Assert(IsA(roleNode, RoleSpec) || IsA(roleNode, AccessPriv));
#else
        Assert(IsA(roleNode, AccessPriv));
#endif
        char const* rolename = NULL;
#ifdef DISABLE_OG_COMMENTS
        if (IsA(roleNode, RoleSpec)) {
            rolename = RoleSpecString((RoleSpec*)roleNode, true);
        }
#else
        if (strVal(roleNode) != NULL) {
            rolename = RoleSpecString(strVal(roleNode), true);
        }
#endif
        appendStringInfoString(buf, rolename);
        if (cell != list_tail(roleList)) {
            appendStringInfo(buf, ", ");
        }
    }
}
