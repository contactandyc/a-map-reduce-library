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
 * PIPELINE: COMPLEMENTS (FIRST-ORDER COSINE SIMILARITY)
 * ========================================================================
 * Logical Inputs:
 * - "in_user_to_partial_items" (IDList)
 * - "in_item_to_users"         (IDList)
 * - "in_item_counts"           (UInt32Pair)
 *
 * Logical Outputs:
 * - "out_scores"               (IDPairWeight: ID_A, ID_B, Cosine Score)
 * ======================================================================== */
bool pipeline_complements_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
