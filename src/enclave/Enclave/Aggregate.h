#include <cstddef>
#include <cstdint>

#ifndef AGGREGATE_H
#define AGGREGATE_H

void non_oblivious_aggregate(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length,
  bool is_partial);

#endif // AGGREGATE_H
