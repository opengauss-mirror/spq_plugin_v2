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
IFS=$'\n\t'

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# read pg major version, error if not provided
PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

# get the project and clear out the git repo (reduce workspace size
rm -rf "${basedir}/.git"

build_ext() {
  pg_major="$1"

  builddir="${basedir}/build-${pg_major}"
  echo "Beginning build for PostgreSQL ${pg_major}..." >&2

  # do everything in a subdirectory to avoid clutter in current directory
  mkdir -p "${builddir}" && cd "${builddir}"

  CFLAGS=-Werror "${basedir}/configure" PG_CONFIG="/usr/lib/postgresql/${pg_major}/bin/pg_config" --enable-coverage --with-security-flags

  installdir="${builddir}/install"
  make -j$(nproc) && mkdir -p "${installdir}" && { make DESTDIR="${installdir}" install-all || make DESTDIR="${installdir}" install ; }

  cd "${installdir}" && find . -type f -print > "${builddir}/files.lst"
  tar cvf "${basedir}/install-${pg_major}.tar" `cat ${builddir}/files.lst`

  cd "${builddir}" && rm -rf install files.lst && make clean
}

build_ext "${PG_MAJOR}"
