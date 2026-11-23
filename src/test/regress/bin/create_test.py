#!/usr/bin/env python3
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
import random
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "ERROR: Expected the name of the new test as an argument, such as:\n"
            "src/test/regress/bin/create_test.py my_awesome_test"
        )
        sys.exit(1)

    test_name = sys.argv[1]

    regress_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(regress_dir, "sql", f"{test_name}.sql")

    if os.path.isfile(filename):
        print(f"ERROR: test file '{filename}' already exists")
        sys.exit(1)

    shard_id = random.randint(1, 999999) * 100

    contents = f"""CREATE SCHEMA {test_name};
SET search_path TO {test_name};
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO {shard_id};

-- add tests here

SET client_min_messages TO WARNING;
DROP SCHEMA {test_name} CASCADE;
"""

    with open(filename, "w") as f:
        f.write(contents)

    print(f"Created {filename}")
    print(f"Don't forget to add '{test_name}' in multi_schedule somewhere")
