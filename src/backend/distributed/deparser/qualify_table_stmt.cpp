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
 * qualify_table_stmt.c
 *	  Functions specialized in fully qualifying all table statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying table statements consists of adding the schema name
 *	  to the subject of the table as well as any other branch of the
 *	  parsetree.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "distributed/deparser.h"

void QualifyAlterTableSchemaStmt(Node* node)
{
    AlterObjectSchemaStmt* stmt = castNode(AlterObjectSchemaStmt, node);
    Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

    if (stmt->relation->schemaname == NULL) {
        Oid tableOid = RelnameGetRelid(stmt->relation->relname);
        Oid schemaOid = get_rel_namespace(tableOid);
        stmt->relation->schemaname = get_namespace_name(schemaOid);
    }
}
