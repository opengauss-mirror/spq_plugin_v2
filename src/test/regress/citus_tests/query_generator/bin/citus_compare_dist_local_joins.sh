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


# make bash behave
set -euo pipefail

psql_user=$1
psql_db=$2
psql_port=$3
seed=${4:-""}

runDDLs()
{
    # run ddls
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" -f "${out_folder}"/ddls.sql > /dev/null
}

runUndistributeTables()
{
    undistribute_all_tables_command='SELECT undistribute_table(logicalrelid) FROM pg_dist_partition;'
    # run undistribute all tables
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" -c "${undistribute_all_tables_command}" > /dev/null
}

runQueries()
{
    out_filename=$1

    # run dmls
    # echo queries and comments for query tracing
    psql -U "${psql_user}" -d "${psql_db}" -p "${psql_port}" \
        --echo-all \
        -f "${out_folder}"/queries.sql > "${out_filename}" 2>&1
}

showDiffs()
{
    pushd . && cd "${script_folder}" && python3 diff-checker.py && popd
}

# run query generator and let it create output ddls and queries
script_folder=$(dirname "$0")
out_folder="${script_folder}"/../out
pushd . && cd "${script_folder}"/.. && python3 generate_queries.py --seed="${seed}" && popd

# remove result files if exists
rm -rf "${out_folder}"/dist_queries.out "${out_folder}"/local_queries.out

# run ddls
runDDLs

# runs dmls for distributed tables
runQueries "${out_folder}"/dist_queries.out

# undistribute all dist tables
runUndistributeTables

# runs the same dmls for pg local tables
runQueries "${out_folder}"/local_queries.out

# see diffs in results
showDiffs
