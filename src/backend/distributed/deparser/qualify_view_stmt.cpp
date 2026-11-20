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
 * qualify_view_stmt.c
 *	  Functions specialized in fully qualifying all view statements. These
 *	  functions are dispatched from qualify.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/nodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void QualifyViewRangeVar(RangeVar* view);

/*
 * QualifyDropViewStmt quailifies the view names of the DROP VIEW statement.
 */
void QualifyDropViewStmt(Node* node)
{
    DropStmt* stmt = castNode(DropStmt, node);
    List* qualifiedViewNames = NIL;

    List* possiblyQualifiedViewName = NULL;
    foreach_declared_ptr(possiblyQualifiedViewName, stmt->objects)
    {
        char* viewName = NULL;
        char* schemaName = NULL;
        List* viewNameToAdd = possiblyQualifiedViewName;
        DeconstructQualifiedName(possiblyQualifiedViewName, &schemaName, &viewName);

        if (schemaName == NULL) {
            RangeVar* viewRangeVar = makeRangeVarFromNameList(possiblyQualifiedViewName);
            Oid viewOid =
                RangeVarGetRelid(viewRangeVar, AccessExclusiveLock, stmt->missing_ok);

            /*
             * If DROP VIEW IF EXISTS called and the view doesn't exist, oid can be
             * invalid. Do not try to qualify it.
             */
            if (OidIsValid(viewOid)) {
                Oid schemaOid = get_rel_namespace(viewOid);
                schemaName = get_namespace_name(schemaOid);

                List* qualifiedViewName =
                    list_make2(makeString(schemaName), makeString(viewName));
                viewNameToAdd = qualifiedViewName;
            }
        }

        qualifiedViewNames = lappend(qualifiedViewNames, viewNameToAdd);
    }

    stmt->objects = qualifiedViewNames;
}

/*
 * QualifyAlterViewStmt quailifies the view name of the ALTER VIEW statement.
 */
void QualifyAlterViewStmt(Node* node)
{
    AlterTableStmt* stmt = castNode(AlterTableStmt, node);
    RangeVar* view = stmt->relation;
    QualifyViewRangeVar(view);
}

/*
 * QualifyRenameViewStmt quailifies the view name of the ALTER VIEW ... RENAME statement.
 */
void QualifyRenameViewStmt(Node* node)
{
    RenameStmt* stmt = castNode(RenameStmt, node);
    RangeVar* view = stmt->relation;
    QualifyViewRangeVar(view);
}

/*
 * QualifyAlterViewSchemaStmt quailifies the view name of the ALTER VIEW ... SET SCHEMA
 * statement.
 */
void QualifyAlterViewSchemaStmt(Node* node)
{
    AlterObjectSchemaStmt* stmt = castNode(AlterObjectSchemaStmt, node);
    RangeVar* view = stmt->relation;
    QualifyViewRangeVar(view);
}

/*
 * QualifyViewRangeVar qualifies the given view RangeVar if it is not qualified.
 */
static void QualifyViewRangeVar(RangeVar* view)
{
    if (view->schemaname == NULL) {
        Oid viewOid = RelnameGetRelid(view->relname);
        Oid schemaOid = get_rel_namespace(viewOid);
        view->schemaname = get_namespace_name(schemaOid);
    }
}
