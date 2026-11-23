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
 * qualify_aggregate_stmts.c
 *	  Functions specialized in fully qualifying all aggregate statements.
 *    These functions are dispatched from qualify.c
 *
 *	  Fully qualifying aggregate statements consists of adding the schema name
 *	  to the subject of the types as well as any other branch of the parsetree.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

#include "distributed/deparser.h"

void QualifyDefineAggregateStmt(Node* node)
{
    DefineStmt* stmt = castNode(DefineStmt, node);

    if (list_length(stmt->defnames) == 1) {
        char* objname = NULL;
        Oid creationSchema = QualifiedNameGetCreationNamespace(stmt->defnames, &objname);
        stmt->defnames = list_make2(makeString(get_namespace_name(creationSchema)),
                                    linitial(stmt->defnames));
    }
}
