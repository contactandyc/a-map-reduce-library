// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "a-map-reduce-library/amr.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * PIPELINE: INVERTED INDEX CO-FREQUENCY (UNIVERSAL GRAPH ENGINE)
 * ======================================================================== */

typedef enum {
    INV_FREQ_OUT_A_WDESC = 0, // Zero-shuffle: Output local overlaps (for O(1) formatting)
    INV_FREQ_OUT_B_A = 1,     // Billion-Scale: Shuffle by B (for distributed merge joins)
    INV_FREQ_INDEX_ONLY = 2   // Complements: Stop early, just expose the bipartite indices
} inv_freq_out_mode_t;

typedef struct {
    int min_overlap;
    inv_freq_out_mode_t out_mode;
} inv_freq_config_t;

/* Logical Inputs:  "in_sessions" (StringPair: User, Item)
 * Logical Outputs:
 * - "out_freqs"                 (IDPairWeight: ID_A, ID_B, Weight) -> The Integer Math
 * - "out_item_dict"             (IdStringPair: ID_A, ASIN)         -> The Mapping
 * - "out_item_counts"           (UInt32Pair: ID_A, Total)          -> Global Frequencies
 * - "out_item_to_users"         (IDList)                           -> Inverted Index
 * - "out_user_to_partial_items" (IDList)                           -> Fragmented Forward Index
 */
bool pipeline_inv_freq_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
