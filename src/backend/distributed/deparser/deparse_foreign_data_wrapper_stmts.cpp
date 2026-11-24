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
 * deparse_foreign_data_wrapper_stmts.c
 *	  All routines to deparse foreign data wrapper statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"

static void AppendGrantOnFDWStmt(StringInfo buf, GrantStmt* stmt);
static void AppendGrantOnFDWNames(StringInfo buf, GrantStmt* stmt);

char* DeparseGrantOnFDWStmt(Node* node)
{
    GrantStmt* stmt = castNode(GrantStmt, node);
    Assert(stmt->objtype == ACL_OBJECT_FDW);

    StringInfoData str = {0};
    initStringInfo(&str);

    AppendGrantOnFDWStmt(&str, stmt);

    return str.data;
}

static void AppendGrantOnFDWStmt(StringInfo buf, GrantStmt* stmt)
{
    Assert(stmt->objtype == ACL_OBJECT_FDW);
    AppendGrantSharedPrefix(buf, stmt);
    AppendGrantOnFDWNames(buf, stmt);
    AppendGrantSharedSuffix(buf, stmt);
}

static void AppendGrantOnFDWNames(StringInfo buf, GrantStmt* stmt)
{
    ListCell* cell = NULL;
    appendStringInfo(buf, " ON FOREIGN DATA WRAPPER ");

    foreach (cell, stmt->objects) {
        char* fdwname = strVal(lfirst(cell));

        appendStringInfoString(buf, quote_identifier(fdwname));
        if (cell != list_tail(stmt->objects)) {
            appendStringInfo(buf, ", ");
        }
    }
}
