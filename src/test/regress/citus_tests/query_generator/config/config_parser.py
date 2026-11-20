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

from node_defs import CitusType, Column, JoinType, RestrictOp, RTEType, Table


def parseJoinType(joinTypeText):
    return JoinType[joinTypeText]


def parseJoinTypeArray(joinTypeTexts):
    joinTypes = []
    for joinTypeText in joinTypeTexts:
        joinType = parseJoinType(joinTypeText)
        joinTypes.append(joinType)
    return joinTypes


def parseRteType(rteTypeText):
    return RTEType[rteTypeText]


def parseRteTypeArray(rteTypeTexts):
    rteTypes = []
    for rteTypeText in rteTypeTexts:
        rteType = parseRteType(rteTypeText)
        rteTypes.append(rteType)
    return rteTypes


def parseRestrictOp(restrictOpText):
    return RestrictOp[restrictOpText]


def parseRestrictOpArray(restrictOpTexts):
    restrictOps = []
    for restrictOpText in restrictOpTexts:
        restrictOp = parseRestrictOp(restrictOpText)
        restrictOps.append(restrictOp)
    return restrictOps


def parseTable(targetTableDict):
    name = targetTableDict["name"]
    citusType = CitusType[targetTableDict["citusType"]]
    maxAllowedUseOnQuery = targetTableDict["maxAllowedUseOnQuery"]
    rowCount = targetTableDict["rowCount"]
    nullRate = targetTableDict["nullRate"]
    duplicateRate = targetTableDict["duplicateRate"]
    columns = []
    for columnDict in targetTableDict["columns"]:
        col = parseColumn(columnDict)
        columns.append(col)
    distinctCopyCount = targetTableDict["distinctCopyCount"]
    return Table(
        name,
        citusType,
        maxAllowedUseOnQuery,
        rowCount,
        nullRate,
        duplicateRate,
        columns,
        distinctCopyCount,
    )


def parseTableArray(targetTableDicts):
    tables = []
    for targetTableDict in targetTableDicts:
        table = parseTable(targetTableDict["Table"])
        tables.append(table)
    return tables


def parseColumn(targetColumnDict):
    name = targetColumnDict["name"]
    type = targetColumnDict["type"]
    return Column(name, type)


def parseRange(rangeDict):
    fromVal = rangeDict["from"]
    toVal = rangeDict["to"]
    return (fromVal, toVal)
