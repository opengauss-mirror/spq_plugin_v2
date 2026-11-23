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

def test_call_param(cluster):
    # create a distributed table and an associated distributed procedure
    # to ensure parameterized CALL succeed, even when the param is the
    # distribution key.
    coord = cluster.coordinator
    coord.sql("CREATE TABLE test(i int)")
    coord.sql(
        """
        CREATE PROCEDURE p(_i INT) LANGUAGE plpgsql AS $$
        BEGIN
        INSERT INTO test(i) VALUES (_i);
        END; $$
        """
    )
    sql = "CALL p(%s)"

    # prepare/exec before distributing
    coord.sql_prepared(sql, (1,))

    coord.sql("SELECT create_distributed_table('test', 'i')")
    coord.sql(
        "SELECT create_distributed_function('p(int)', distribution_arg_name := '_i', colocate_with := 'test')"
    )

    # prepare/exec after distribution
    coord.sql_prepared(sql, (2,))

    sum_i = coord.sql_value("select sum(i) from test;")

    assert sum_i == 3
