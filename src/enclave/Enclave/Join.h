#include <cstddef>
#include <cstdint>

#ifndef JOIN_H
#define JOIN_H

void scan_collect_last_primary(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length);

void non_oblivious_sort_merge_join(
    uint8_t *join_expr, size_t join_expr_length,
    uint8_t *input_rows, size_t input_rows_length,
    uint8_t *join_row, size_t join_row_length,
    uint8_t **output_rows, size_t *output_rows_length);

#endif
