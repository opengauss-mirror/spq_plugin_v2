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

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"

/*
 * Append the 'WITH GRANT OPTION' clause to the given buffer if the given
 * statement is a 'GRANT' statement and the grant option is specified.
 */
void AppendWithGrantOption(StringInfo buf, GrantStmt* stmt)
{
    if (stmt->is_grant && stmt->grant_option) {
        appendStringInfo(buf, " WITH GRANT OPTION");
    }
}

/*
 * Append the 'GRANT OPTION FOR' clause to the given buffer if the given
 * statement is a 'REVOKE' statement and the grant option is specified.
 */
void AppendGrantOptionFor(StringInfo buf, GrantStmt* stmt)
{
    if (!stmt->is_grant && stmt->grant_option) {
        appendStringInfo(buf, "GRANT OPTION FOR ");
    }
}

/*
 * Append the 'RESTRICT' or 'CASCADE' clause to the given buffer if the given
 * statement is a 'REVOKE' statement and the behavior is specified.
 */
void AppendGrantRestrictAndCascadeForRoleSpec(StringInfo buf, DropBehavior behavior,
                                              bool isGrant)
{
    if (!isGrant) {
        if (behavior == DROP_RESTRICT) {
            appendStringInfo(buf, " RESTRICT");
        } else if (behavior == DROP_CASCADE) {
            appendStringInfo(buf, " CASCADE");
        }
    }
}

/*
 * Append the 'RESTRICT' or 'CASCADE' clause to the given buffer using 'GrantStmt',
 * if the given statement is a 'REVOKE' statement and the behavior is specified.
 */
void AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt* stmt)
{
    AppendGrantRestrictAndCascadeForRoleSpec(buf, stmt->behavior, stmt->is_grant);
}

/*
 * Append the 'GRANTED BY' clause to the given buffer if the given statement is a
 * 'GRANT' statement and the grantor is specified.
 */
void AppendGrantedByInGrantForRoleSpec(StringInfo buf, char* grantor, bool isGrant)
{
    if (isGrant && grantor) {
        appendStringInfo(buf, " GRANTED BY %s", RoleSpecString(grantor, true));
    }
}

/*
 * Append the 'GRANTED BY' clause to the given buffer using 'GrantStmt',
 * if the given statement is a 'GRANT' statement and the grantor is specified.
 */
void AppendGrantedByInGrant(StringInfo buf, GrantStmt* stmt)
{
#ifdef DISABLE_OG_COMMENTS
    AppendGrantedByInGrantForRoleSpec(buf, stmt->grantor, stmt->is_grant);
#endif
}

void AppendGrantSharedPrefix(StringInfo buf, GrantStmt* stmt)
{
    appendStringInfo(buf, "%s ", stmt->is_grant ? "GRANT" : "REVOKE");
    AppendGrantOptionFor(buf, stmt);
    AppendGrantPrivileges(buf, stmt);
}

void AppendGrantSharedSuffix(StringInfo buf, GrantStmt* stmt)
{
    AppendGrantGrantees(buf, stmt);
    AppendWithGrantOption(buf, stmt);
    AppendGrantRestrictAndCascade(buf, stmt);
    AppendGrantedByInGrant(buf, stmt);
    appendStringInfo(buf, ";");
}
