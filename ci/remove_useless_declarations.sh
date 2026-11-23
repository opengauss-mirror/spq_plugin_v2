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

files=$(find src -iname '*.c' -type f | git check-attr --stdin citus-style | grep -v ': unset$' | sed 's/: citus-style: set$//')
while true; do
    # A visual version of this regex can be seen here (it is MUCH clearer):
    # https://www.debuggex.com/r/XodMNE9auT9e-bTx
    # This visual version only contains the search bit, the replacement bit is
    # quite simple. It looks like when extracted from the command below:
    # \n$+{code_between}\t$+{type}$+{variable} =
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t)?(?=\b(?P=variable)\b))(?<=\n\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t$+{type}$+{variable} =/sg' $files
    # The following are simply the same regex, but repeated for different
    # indentation levels, i.e. finding declarations indented using 2, 3, 4, 5
    # and 6 tabs. More than 6 don't really occur in the wild.
    # (this is needed because variable sized backtracking is not supported in perl)
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    git diff --quiet $files && break;
    # shellcheck disable=SC2086
    git add $files;
done
