#include <cstdint>
#include <cstddef>

void shift_up(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_size);

void shift_down(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_size);

void transpose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_size);

void untranspose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_row_size);

void column_sort_pad(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint32_t rows_per_partition,
                         uint8_t **output_row,
                         size_t *output_row_size);

void column_sort_filter(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint8_t **output_row,
                         size_t *output_row_size);