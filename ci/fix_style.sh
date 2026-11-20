#!/bin/sh
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


# fail if trying to reference a variable that is not set.
set -u / set -o nounset
# exit immediately if a command fails
set -e

cidir="${0%/*}"
cd ${cidir}/..

citus_indent . --quiet
black . --quiet
isort . --quiet
ci/editorconfig.sh
ci/remove_useless_declarations.sh
ci/disallow_c_comments_in_migrations.sh
ci/disallow_hash_comments_in_spec_files.sh
ci/disallow_long_changelog_entries.sh
ci/normalize_expected.sh
ci/fix_gitignore.sh
ci/print_stack_trace.sh
ci/sort_and_group_includes.sh
