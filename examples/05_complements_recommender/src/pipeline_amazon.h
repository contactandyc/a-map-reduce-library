// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#pragma once

#include "a-map-reduce-library/amr.h"

#ifdef __cplusplus
extern "C" {
#endif

// Configuration payload so the pipeline isn't hardcoded to specific paths
typedef struct {
    const char *items_file;
    const char *events_file;
} amazon_config_t;

/* Logical Outputs:
 * - "out_dict"     (StringPair: ASIN -> Title)
 * - "out_sessions" (StringPair: UserID -> ASIN)
 */
bool pipeline_amazon_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
