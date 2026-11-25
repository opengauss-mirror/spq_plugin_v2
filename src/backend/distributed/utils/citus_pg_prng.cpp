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
 * Pseudo-Random Number Generator
 *
 * Copyright (c) 2021-2025, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <cmath>
#include "postgres.h"
#include "access/datavec/pg_prng.h"
/** opengauss extern it but not definie it */
PGDLLIMPORT pg_prng_state pg_global_prng_state;

/*
 * 64-bit rotate left
 */
static inline uint64 rotl(uint64 x, int bits)
{
    return (x << bits) | (x >> (64 - bits));
}

/*
 * The basic xoroshiro128** algorithm.
 * Generates and returns a 64-bit uniformly distributed number,
 * updating the state vector for next time.
 *
 * Note: the state vector must not be all-zeroes, as that is a fixed point.
 */
static uint64 xoroshiro128ss(pg_prng_state* state)
{
    uint64 s0 = state->s0, sx = state->s1 ^ s0, val = rotl(s0 * 5, 7) * 9;

    /* update state */
    state->s0 = rotl(s0, 24) ^ sx ^ (sx << 16);
    state->s1 = rotl(sx, 37);

    return val;
}

/*
 * Select a random double uniformly from the range [0.0, 1.0).
 *
 * Note: if you want a result in the range (0.0, 1.0], the standard way
 * to get that is "1.0 - pg_prng_double(state)".
 */
double pg_prng_double(pg_prng_state* state)
{
    uint64 v = xoroshiro128ss(state);

    /*
     * As above, assume there's 52 mantissa bits in a double.  This result
     * could round to 1.0 if double's precision is less than that; but we
     * assume IEEE float arithmetic elsewhere in Postgres, so this seems OK.
     */
    return ldexp((double)(v >> (64 - 52)), -52);
}