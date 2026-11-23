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
 * qualify.c
 *	  The deparser relies on fully qualified names on all statements to
 *	  work transparently on a remote worker node. Here we have helpers to
 *	  fully qualify parsetrees.
 *
 *	  Fully qualified parsetrees contain names for all identifiers that
 *	  are search_path agnostic. Meaning we need to include the schema name
 *	  for each and every identifier in the parsetree.
 *
 *	  This file contains mostly the distpatching functions to specialized
 *	  functions for each class of objects. eg qualify_type_stmt.c contains
 *	  all functions related to fully qualifying parsetrees that interact
 *	  with types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"

/*
 * QualifyTreeNode transforms the statement in place and makes all (supported) statements
 * fully qualified. Fully qualified statements allow for application on a remote postgres
 * server irregardless of their search_path.
 */
void QualifyTreeNode(Node* stmt)
{
    const DistributeObjectOps* ops = GetDistributeObjectOps(stmt);

    if (ops && ops->qualify) {
        ops->qualify(stmt);
    }
}

/*
 * QualifyRenameAttributeStmt transforms a RENAME ATTRIBUTE statement in place and makes
 * all (supported) statements fully qualified.
 */
void QualifyRenameAttributeStmt(Node* node)
{
    RenameStmt* stmt = castNode(RenameStmt, node);
    Assert(stmt->renameType == OBJECT_ATTRIBUTE);

    switch (stmt->relationType) {
        case OBJECT_TYPE: {
            QualifyRenameTypeAttributeStmt(node);
            return;
        }

        default: {
            return;
        }
    }
}
