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
 * qualify_role_stmt.c
 *	  Functions specialized in fully qualifying all role statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying role statements consists of adding the current database
 *    name, session user, current use, and current configuration values.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/nodes.h"
#include "utils/guc.h"

#include "distributed/deparser.h"

static void QualifyVarSetCurrent(VariableSetStmt* setStmt);

/*
 * QualifyAlterRoleSetStmt transforms a
 * ALTER ROLE .. SET ..
 * statement in place and makes the settings fully qualified.
 */
void QualifyAlterRoleSetStmt(Node* node)
{
    AlterRoleSetStmt* stmt = castNode(AlterRoleSetStmt, node);
    VariableSetStmt* setStmt = stmt->setstmt;

    if (setStmt->kind == VAR_SET_CURRENT) {
        QualifyVarSetCurrent(setStmt);
    }
}

/*
 * QualifyVarSetCurrent transforms a
 * FROM CURRENT
 * into a
 * SET config_name TO 'config_value'
 * VariableSetStmt in place and hence makes it fully qualified.
 */
static void QualifyVarSetCurrent(VariableSetStmt* setStmt)
{
    char* configurationName = setStmt->name;
    char* configValue = GetConfigOptionByName(configurationName, NULL);

    setStmt->kind = VAR_SET_VALUE;
    setStmt->args = list_make1(MakeSetStatementArguments(configurationName, configValue));
}
