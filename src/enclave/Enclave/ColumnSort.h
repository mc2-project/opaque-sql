#include <stdint.h>

void shift_up(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, uint32_t *output_rows_length);

void shift_down(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, uint32_t *output_rows_length);

void transpose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, uint32_t *output_rows_length);

void untranspose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, uint32_t *output_rows_length);

void column_sort_pad(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint8_t **output_row,
                         uint32_t *output_rows_size);

void column_sort_filter(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint8_t **output_row,
                         uint32_t *output_row_size);