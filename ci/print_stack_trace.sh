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

# find all core files
core_files=( $(find . -type f -regex .*core.*\d*.*postgres) )
if [ ${#core_files[@]} -gt 0 ]; then
    # print stack traces for the core files
    for core_file in "${core_files[@]}"
    do
        # set print frame-arguments all: show all scalars + structures in the frame
        # set print pretty on:           show structures in indented mode
        # set print addr off:            do not show pointer address
        # thread apply all bt full:      show stack traces for all threads
        gdb --batch \
            -ex "set print frame-arguments all" \
            -ex "set print pretty on" \
            -ex "set print addr off" \
            -ex "thread apply all bt full" \
            postgres "${core_file}"
    done
fi
