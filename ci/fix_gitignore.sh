#! /bin/bash
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

# Remove all the ignored files from git tree, and error out
# find all ignored files in git tree, and use quotation marks to prevent word splitting on filenames with spaces in them
# NOTE: Option --cached is needed to avoid a bug in git ls-files command.
ignored_lines_in_git_tree=$(git ls-files --ignored --cached --exclude-standard | sed 's/.*/"&"/')

if [[ -n $ignored_lines_in_git_tree ]]
then
    echo "Ignored files should not be in git tree!"
    echo "${ignored_lines_in_git_tree}"

    echo "Removing these files from git tree, please review and commit"
    echo "$ignored_lines_in_git_tree" | xargs git rm -r --cached
    exit 1
fi
