#include <cstddef>
#include <cstdint>

#ifndef LIMIT_H
#define LIMIT_H

/** Limit helper functions */
void count_rows_per_partition(uint8_t *input_rows, size_t input_rows_length,
                              uint8_t **output_rows, size_t *output_rows_length);

void compute_num_rows_per_partitions(uint32_t limit,
                                     uint8_t input_rows, size_t input_rows_length,
                                     uint8_t **output_rows, size_t *output_rows_length);

void limit();

#endif
