#!/usr/bin/env python3
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


import config as cfg


def read_config_names():
    config_names = []
    # We fill the configs from all of the possible classes in config.py so that if we add a new config,
    # we don't need to add it here. And this avoids the problem where we forget to add it here
    for x in cfg.__dict__.values():
        if cfg.should_include_config(x):
            config_names.append(x.__name__)
    return config_names


def print_config_names():
    config_names = read_config_names()
    for config_name in config_names:
        print(config_name)


if __name__ == "__main__":
    print_config_names()
