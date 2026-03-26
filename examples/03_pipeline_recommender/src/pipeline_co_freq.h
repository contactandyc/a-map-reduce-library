// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#pragma once

#include "a-map-reduce-library/amr.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Logical Inputs:  "in_sessions" (StringPair)
 * Logical Outputs: "out_pairs"   (StringPairWeight) */
bool pipeline_co_freq_setup(amr_pipeline_t *p);

#ifdef __cplusplus
}
#endif
