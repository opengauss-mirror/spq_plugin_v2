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

/*
 * cancel_utils.c
 *
 * Utilities related to query cancellation
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "miscadmin.h"

#include "distributed/cancel_utils.h"

/*
 * IsHoldOffCancellationReceived returns true if a cancel signal
 * was sent and HOLD_INTERRUPTS was called prior to this. The motivation
 * here is that since our queries can take a long time, in some places
 * we do not want to wait even if HOLD_INTERRUPTS was called.
 */
bool IsHoldOffCancellationReceived()
{
    return t_thrd.int_cxt.InterruptHoldoffCount > 0 &&
           (t_thrd.int_cxt.QueryCancelPending || t_thrd.int_cxt.ProcDiePending);
}
