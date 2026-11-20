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

# make ** match all directories and subdirectories
shopt -s globstar

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# We do not use c-style comments in migration files as the stripped
# zero-length migration files cause warning during packaging
# See #3115 for more info

# In this file, we aim to keep the indentation intact by capturing whitespaces,
# and reusing them if needed. GNU sed unfortunately does not support lookaround assertions.

# /* -> --
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#/\*#--#g'

# */ -> `` (empty string)
# remove all whitespaces immediately before the match
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#\s*\*/\s*##g'

# * -> --
# keep the indentation
# allow only whitespaces before the match
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#^\(\s*\) \*#\1--#g'

# // -> --
# do not touch http:// or similar by allowing only whitespaces before //
find src/backend/{distributed,columnar}/sql/**/*.sql -print0 | xargs -0 sed -i 's#^\(\s*\)//#\1--#g'
