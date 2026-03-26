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
 * PIPELINE: RECIPROCAL NEAREST NEIGHBORS (RNN)
 * ========================================================================
 * Logical Inputs:
 * - "in_scores" (IDPairWeight: ID_A, ID_B, Substitutes Score)
 * - "in_counts" (UInt32Pair: Item ID, Global Total Frequency)
 *
 * Logical Outputs:
 * - "out_rnn"   (IDPairWeights: A, B, sim_w, aw, bw)
 * ======================================================================== */

typedef struct {
    uint32_t min_users;
    double min_sim;
} rnn_config_t;

bool pipeline_rnn_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
