/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*-------------------------------------------------------------------------
 *
 * priority.c
 *	  Utilities for managing CPU priority.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"

#include "distributed/priority.h"

/*
 * SetOwnPriority changes the CPU priority of the current backend to the given
 * priority. If the OS disallows us to set the priority to the given value, we
 * only warn about it.
 */
void SetOwnPriority(int priority)
{
    if (priority == CPU_PRIORITY_INHERIT) {
        return;
    }

    if (setpriority(PRIO_PROCESS, getpid(), priority) == -1) {
        ereport(WARNING, (errmsg("could not set cpu priority to %d: %m", priority),
                          errhint("Try changing the 'nice' resource limit by changing "
                                  "/etc/security/limits.conf for the postgres user "
                                  "and/or by setting LimitNICE in your the systemd "
                                  "service file (depending on how you start "
                                  "postgres).")));
    }
}

/*
 * GetOwnPriority returns the current CPU priority value of the backend.
 */
int GetOwnPriority(void)
{
    errno = 0;
    int result = getpriority(PRIO_PROCESS, getpid());

    /*
     * We explicitly check errno too because getpriority can return -1 on
     * success too, if the actual priority value is -1
     */
    if (result == -1 && errno != 0) {
        ereport(WARNING, (errmsg("could not get current cpu priority value, "
                                 "assuming 0: %m")));
        return 0;
    }
    return result;
}
