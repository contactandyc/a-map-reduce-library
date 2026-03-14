// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef INTERNAL_AMR_WORKER_H
#define INTERNAL_AMR_WORKER_H

#include "amr_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Worker Preparation & State Management */
void fill_inputs(amr_worker_t *w);
void clone_inputs_and_outputs(amr_worker_t *w);

/* Output scanning for debugging/dumping */
void scan_output(amr_worker_t *w, char **files, size_t num_files);
void scan_output_sample(amr_worker_t *w);

/* File Information Resolvers */
io_file_info_t *file_info_shuffle(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_all_to_all(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_first(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_name(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_partition(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);

io_file_info_t *file_info_prev_run_first(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_prev_run_partition(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_prev_run_all_to_all(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);
io_file_info_t *file_info_prev_run_shuffle(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);

#ifdef __cplusplus
}
#endif

#endif /* AMR_WORKER_H */
