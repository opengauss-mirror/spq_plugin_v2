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
 * deparse_attribute_stmts.c
 *	  All routines to deparse attribute statements.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"

char* DeparseRenameAttributeStmt(Node* node)
{
    RenameStmt* stmt = castNode(RenameStmt, node);
    Assert(stmt->renameType == OBJECT_ATTRIBUTE);

    switch (stmt->relationType) {
        case OBJECT_TYPE: {
            return DeparseRenameTypeAttributeStmt(node);
        }

        default: {
            ereport(ERROR, (errmsg("unsupported rename attribute statement for"
                                   " deparsing")));
        }
    }
    return NULL;
}
