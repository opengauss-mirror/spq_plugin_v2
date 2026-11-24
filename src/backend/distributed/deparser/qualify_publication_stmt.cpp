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
 * qualify_publication_stmt.c
 *	  Functions specialized in fully qualifying all publication statements. These
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

static void QualifyPublicationObjects(List* publicationObjects);
static void QualifyPublicationRangeVar(RangeVar* publication);

/*
 * QualifyCreatePublicationStmt quailifies the publication names of the
 * CREATE PUBLICATION statement.
 */
void QualifyCreatePublicationStmt(Node* node)
{
    CreatePublicationStmt* stmt = castNode(CreatePublicationStmt, node);

    QualifyPublicationObjects(stmt->tables);
}

/*
 * QualifyPublicationObjects ensures all table names in a list of
 * publication objects are fully qualified.
 */
static void QualifyPublicationObjects(List* publicationObjects)
{
    RangeVar* publicationObject = NULL;

    foreach_declared_ptr(publicationObject, publicationObjects)
    {
#ifdef DISABLE_OG_COMMENTS
        if (publicationObject->pubobjtype == PUBLICATIONOBJ_TABLE) {
            /* FOR TABLE ... */
            PublicationTable* publicationTable = publicationObject->pubtable;

            QualifyPublicationRangeVar(publicationTable->relation);
        }
#else
        QualifyPublicationRangeVar(publicationObject);
#endif
    }
}

/*
 * QualifyPublicationObjects ensures all table names in a list of
 * publication objects are fully qualified.
 */
void QualifyAlterPublicationStmt(Node* node)
{
    AlterPublicationStmt* stmt = castNode(AlterPublicationStmt, node);

    QualifyPublicationObjects(stmt->tables);
}

/*
 * QualifyPublicationRangeVar qualifies the given publication RangeVar if it is not
 * qualified.
 */
static void QualifyPublicationRangeVar(RangeVar* publication)
{
    if (publication->schemaname == NULL) {
        Oid publicationOid = RelnameGetRelid(publication->relname);
        Oid schemaOid = get_rel_namespace(publicationOid);
        publication->schemaname = get_namespace_name(schemaOid);
    }
}
