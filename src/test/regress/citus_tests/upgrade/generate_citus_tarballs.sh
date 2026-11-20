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


set -euxo pipefail

pg_version=$1
citus_old_version=$2

base="$(pwd)"

install_citus_and_tar() {
    # do everything in a subdirectory to avoid clutter in current directory
    mkdir -p "${builddir}" && cd "${builddir}"

    "${citus_dir}/configure" --without-libcurl

    installdir="${builddir}/install"
    make "-j$(nproc)" && mkdir -p "${installdir}" && make DESTDIR="${installdir}" install

    cd "${installdir}" && find . -type f -print >"${builddir}/files.lst"

    tar cvf "${basedir}/install-pg${pg_version}-citus${citus_version}.tar" $(cat "${builddir}"/files.lst)
    mv "${basedir}/install-pg${pg_version}-citus${citus_version}.tar" "${base}/install-pg${pg_version}-citus${citus_version}.tar"

    cd "${builddir}" && rm -rf install files.lst && make clean
}

build_ext() {
    citus_version="$1"
    # If tarball already exsists we're good
    if [ -f "${base}/install-pg${pg_version}-citus${citus_version}.tar" ]; then
        return
    fi

    basedir="${base}/${citus_version}"

    rm -rf "${basedir}"
    mkdir -p "${basedir}"
    cd "${basedir}"
    citus_dir=${basedir}/citus_$citus_version
    git clone --branch "$citus_version" https://github.com/citusdata/citus.git --depth 1 citus_"$citus_version"
    builddir="${basedir}/build"

    install_citus_and_tar
}

build_ext "${citus_old_version}"
