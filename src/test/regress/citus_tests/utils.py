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

import os
import subprocess

USER = "postgres"


def psql(pg_path, port, command):
    return subprocess.run(
        [
            os.path.join(pg_path, "psql"),
            "-U",
            USER,
            "-p",
            str(port),
            "-c",
            command,
            "-P",
            "pager=off",
            "--no-psqlrc",
        ],
        check=True,
    )


def psql_capture(pg_path, port, command):
    return subprocess.check_output(
        [
            os.path.join(pg_path, "psql"),
            "-U",
            USER,
            "-p",
            str(port),
            "-c",
            command,
            "-P",
            "pager=off",
            "--no-psqlrc",
            "--tuples-only",
        ],
    )


# Taken from https://stackoverflow.com/questions/431684/how-do-i-change-directory-cd-in-python/13197763#13197763


class cd(object):
    """Context manager for changing the current working directory"""

    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        print(self.savedPath)
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
