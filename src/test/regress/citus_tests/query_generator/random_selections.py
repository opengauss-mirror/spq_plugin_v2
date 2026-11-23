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

import random
import time

from node_defs import RestrictOp

from config.config import getConfig


def shouldSelectThatBranch():
    """returns 0 or 1 randomly"""
    return random.randint(0, 1)


def currentMilliSecs():
    """returns total milliseconds since epoch"""
    return round(time.time() * 1000)


def randomRteType():
    """returns a randomly selected RteType given at config"""
    rtes = getConfig().targetRteTypes
    return random.choice(rtes)


def randomJoinOp():
    """returns a randomly selected JoinOp given at config"""
    joinTypes = getConfig().targetJoinTypes
    return " " + random.choice(joinTypes).name + " JOIN"


def randomRestrictOp():
    """returns a randomly selected RestrictOp given at config"""
    restrictOps = getConfig().targetRestrictOps
    restrictOp = random.choice(restrictOps)
    opText = ""
    if restrictOp == RestrictOp.LT:
        opText = "<"
    elif restrictOp == RestrictOp.GT:
        opText = ">"
    elif restrictOp == RestrictOp.EQ:
        opText = "="
    else:
        raise BaseException("Unknown restrict op")

    return " " + opText + " "
