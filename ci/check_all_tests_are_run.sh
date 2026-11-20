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


cd src/test/regress

# 1. Find all *.sql and *.spec files in the sql, and spec directories
# 2. Strip the extension and the directory
# 3. Ignore names that end with .include, those files are meant to be in an C
#    preprocessor #include statement. They should not be in schedules.
test_names=$(
    find sql spec -iname "*.sql" -o -iname "*.spec" |
    sed -E 's#^\w+/([^/]+)\.[^.]+$#\1#g' |
    grep -v '.include$'
)
for name in $test_names; do
    if ! grep "\\b$name\\b" ./*_schedule > /dev/null; then
        echo "ERROR: Test with name \"$name\" is not used in any of the schedule files"
        exit 1
    fi
done
