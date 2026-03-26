// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#pragma once

#include "a-map-reduce-library/amr.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * PIPELINE: SUBSTITUTES (SECOND-ORDER TF-IDF & L2 NORMALIZATION)
 * ========================================================================
 * Logical Inputs:
 * - "in_freqs"  (IDPairWeight: ID_A, ID_B, Raw Co-Frequency)
 * - "in_counts" (UInt32Pair: Item ID, Global Total Frequency)
 *
 * Logical Outputs:
 * - "out_scores" (IDPairWeight: ID_A, ID_B, Substitutes Score)
 * ======================================================================== */
bool pipeline_substitutes_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
