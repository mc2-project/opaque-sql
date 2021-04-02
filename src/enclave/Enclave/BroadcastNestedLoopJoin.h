#include <cstddef>
#include <cstdint>

void broadcast_nested_loop_join(
    uint8_t *join_expr, size_t join_expr_length,
    uint8_t *outer_rows, size_t outer_rows_length,
    uint8_t *inner_rows, size_t inner_rows_length,
    uint8_t **output_rows, size_t *output_rows_length);
