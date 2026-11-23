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


# For echo commands "set -x" would show the message effectively twice. Once as
# part of the echo command shown by "set -x" and once because of the output of
# the echo command. We do not want "set -x" to show the echo command. We only
# want to see the actual message in the output of echo itself. This function is
# a trick to do so. Read the StackOverflow post below to understand why this
# works and what this works around.
# Source: https://superuser.com/a/1141026/242593
shopt -s expand_aliases
alias echo='{ save_flags="$-"; set +x;} 2> /dev/null && echo_and_restore'
echo_and_restore() {
        builtin echo "$*"
        #shellcheck disable=SC2154
        case "$save_flags" in
         (*x*)  set -x
        esac
}

# Make sure that on a failing exit we show a useful message
hint_on_fail() {
    exit_code=$?
    # Get filename of the currently running script
    # Source: https://stackoverflow.com/a/192337/2570866
    filename=$(basename "$0")
    if [ $exit_code != 0 ]; then
        echo "HINT: To solve this failure look here: https://github.com/citusdata/citus/blob/master/ci/README.md#$filename"
    fi
    exit $exit_code
}
trap hint_on_fail EXIT

