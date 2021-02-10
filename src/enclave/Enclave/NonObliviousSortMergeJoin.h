#include <cstddef>
#include <cstdint>

void non_oblivious_sort_merge_join(
    uint8_t *join_expr, size_t join_expr_length,
    uint8_t *input_rows, size_t input_rows_length,
    uint8_t **output_rows, size_t *output_rows_length);
