/*-------------------------------------------------------------------------
 *
 * og_version_compat.h
 *    Compatibility layer for different openGauss versions.
 *
 * This file provides compatibility macros and functions to support both
 * openGauss master branch (7.x) and 6.0.0 branch (6.x).
 *
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 *-------------------------------------------------------------------------
 */

#ifndef OG_VERSION_COMPAT_H
#define OG_VERSION_COMPAT_H

#include "spq_config.h"

#define OG_VERSION_7 70000
#define OG_VERSION_6 60000
#define OG_VERSION_NUM OPENGAUSS_VERSION_NUM

/*
 * Macro to check if we're on master branch (7.x) or 6.0.0 branch (6.x)
 */
#define OG_VERSION_IS_MASTER (OG_VERSION_NUM >= OG_VERSION_7)
#define OG_VERSION_IS_6_0 (OG_VERSION_NUM < OG_VERSION_7)

/*
 * coerce_to_target_type compatibility macro
 * Master (7.x): has fmtstr and nlsfmtstr parameters
 * 6.0.0 (6.x): does not have these parameters
 */
#if OPENGAUSS_VERSION_NUM >= 70000
#define coerce_to_target_type_compat(pstate, expr, exprtype, targettype, targettypmod, \
                                     ccontext, cformat, location)                      \
    coerce_to_target_type(pstate, expr, exprtype, targettype, targettypmod, ccontext,  \
                          cformat, NULL, NULL, location)
#else
#define coerce_to_target_type_compat(pstate, expr, exprtype, targettype, targettypmod, \
                                     ccontext, cformat, location)                      \
    coerce_to_target_type(pstate, expr, exprtype, targettype, targettypmod, ccontext,  \
                          cformat, location)
#endif

/*
 * stringTypeDatum compatibility macro
 * Master (7.x): has fmtstr and nlsfmtstr parameters
 * 6.0.0 (6.x): does not have these parameters
 */
#if OPENGAUSS_VERSION_NUM >= 70000
#define stringTypeDatum_compat(tp, string, atttypmod) \
    stringTypeDatum(tp, string, NULL, NULL, atttypmod)
#else
#define stringTypeDatum_compat(tp, string, atttypmod) \
    stringTypeDatum(tp, string, atttypmod)
#endif

#endif /* OG_VERSION_COMPAT_H */
