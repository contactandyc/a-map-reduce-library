// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#pragma once

#include "a-map-reduce-library/amr.h"

#ifdef __cplusplus
extern "C" {
#endif

// We expose the final struct so the application can deserialize it
typedef struct {
    double w;
    char *a;
    char *title_a;
    char *b;
    char *title_b;
} full_enriched_t;

/* Logical Inputs:  "in_pairs" (StringPairWeight), "in_dict" (StringPair)
 * Logical Outputs: "out_enriched" (FullEnriched) */
bool pipeline_enrich_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
