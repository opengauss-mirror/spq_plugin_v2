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
 * objectaddress.c
 *    Parstrees almost always target a object that postgres can address by
 *    an ObjectAddress. Here we have a walker for parsetrees to find the
 *    address of the object targeted.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"

/*
 * GetObjectAddressListFromParseTree returns the list of ObjectAddress of the main target
 * of the parse tree.
 */
List* GetObjectAddressListFromParseTree(Node* parseTree, bool missing_ok,
                                        bool isPostprocess)
{
    const DistributeObjectOps* ops = GetDistributeObjectOps(parseTree);

    if (!ops->address) {
        ereport(ERROR, (errmsg("unsupported statement to get object address for")));
    }

    return ops->address(parseTree, missing_ok, isPostprocess);
}

List* RenameAttributeStmtObjectAddress(Node* node, bool missing_ok, bool isPostprocess)
{
    RenameStmt* stmt = castNode(RenameStmt, node);
    Assert(stmt->renameType == OBJECT_ATTRIBUTE);

    switch (stmt->relationType) {
        case OBJECT_TYPE: {
            return RenameTypeAttributeStmtObjectAddress(node, missing_ok);
        }

        default: {
            ereport(ERROR, (errmsg("unsupported alter rename attribute statement to get "
                                   "object address for")));
        }
    }
    return NIL;
}

/*
 * CreateExtensionStmtObjectAddress finds the ObjectAddress for the extension described
 * by the CreateExtensionStmt. If missing_ok is false, then this function throws an
 * error if the extension does not exist.
 *
 * Never returns NULL, but the objid in the address could be invalid if missing_ok was set
 * to true.
 */
List* CreateExtensionStmtObjectAddress(Node* node, bool missing_ok, bool isPostprocess)
{
    CreateExtensionStmt* stmt = castNode(CreateExtensionStmt, node);
    ObjectAddress* address = static_cast<ObjectAddress*>(palloc0(sizeof(ObjectAddress)));

    const char* extensionName = stmt->extname;

    Oid extensionoid = get_extension_oid(extensionName, missing_ok);

    /* if we couldn't find the extension, error if missing_ok is false */
    if (!missing_ok && extensionoid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("extension \"%s\" does not exist", extensionName)));
    }

    ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

    return list_make1(address);
}
