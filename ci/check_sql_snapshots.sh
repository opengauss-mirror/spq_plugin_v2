#!/bin/bash
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

set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh

for udf_dir in src/backend/distributed/sql/udfs/* src/backend/columnar/sql/udfs/*; do
    # We want to find the last snapshotted sql file, to make sure it's the same
    # as "latest.sql". This is done by:
    # 1. Getting the filenames in the UDF directory (using find instead of ls, to keep shellcheck happy)
    # 2. Filter out latest.sql
    # 3. Sort using "version sort"
    # 4. Get the last one using tail
    latest_snapshot=$(\
        find "$udf_dir" -iname "*.sql" -exec basename {} \; \
        | { grep --invert-match latest.sql || true; } \
        | sort --version-sort \
        | tail --lines 1);
    diff --unified --color=auto "$udf_dir/latest.sql" "$udf_dir/$latest_snapshot"; \
done
