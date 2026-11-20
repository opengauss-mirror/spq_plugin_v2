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

# We do not use comments starting with # in spec files because it creates warnings from
# preprocessor that expects directives after this character.

# `# ` -> `-- `
find src/test/regress/spec/*.spec -print0 | xargs -0 sed -i 's!# !// !g'
