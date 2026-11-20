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


# 1. Find all *.sh files in the ci directory
# 2. Strip the directory
# 3. Exclude some scripts that we should not run in CI directly
ci_scripts=$(
    find ci/ -iname "*.sh" |
    sed -E 's#^ci/##g' |
    grep -v -E '^(ci_helpers.sh|fix_style.sh)$'
)
for script in $ci_scripts; do
    if ! grep "\\bci/$script\\b" -r .github > /dev/null; then
        echo "ERROR: CI script with name \"$script\" is not actually used in .github folder"
        exit 1
    fi
    if ! grep "^## \`$script\`\$" ci/README.md > /dev/null; then
        echo "ERROR: CI script with name \"$script\" does not have a section in ci/README.md"
        exit 1
    fi
    if ! grep "source ci/ci_helpers.sh" "ci/$script" > /dev/null; then
        echo "ERROR: CI script with name \"$script\" does not include ci/ci_helpers.sh"
        exit 1
    fi
done
