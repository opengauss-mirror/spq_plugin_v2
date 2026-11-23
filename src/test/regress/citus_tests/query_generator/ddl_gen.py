# spq_plugin_v2 - distributed query processing for openGauss.
# Copyright (C) 2026 Huawei Technologies Co., Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from config.config import getConfig, isTableDistributed, isTableReference


def getTableDDLs():
    ddls = ""
    tables = getConfig().targetTables
    for table in tables:
        ddls += _genTableDDL(table)
        ddls += "\n"
    return ddls


def _genTableDDL(table):
    ddl = ""
    ddl += "DROP TABLE IF EXISTS " + table.name + ";"
    ddl += "\n"

    ddl += "CREATE TABLE " + table.name + "("
    for column in table.columns[:-1]:
        ddl += _genColumnDDL(column)
        ddl += ",\n"
    if len(table.columns) > 0:
        ddl += _genColumnDDL(table.columns[-1])
    ddl += ");\n"

    if isTableDistributed(table):
        ddl += (
            "SELECT create_distributed_table("
            + "'"
            + table.name
            + "','"
            + getConfig().commonColName
            + "'"
            + ");"
        )
        ddl += "\n"
    elif isTableReference(table):
        ddl += "SELECT create_reference_table(" + "'" + table.name + "'" + ");"
        ddl += "\n"
    return ddl


def _genColumnDDL(column):
    ddl = ""
    ddl += column.name
    ddl += " "
    ddl += column.type
    return ddl
