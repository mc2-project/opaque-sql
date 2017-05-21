#include "Aggregate.h"

#include "ExpressionEvaluation.h"
#include "common.h"

void non_oblivious_aggregate_step1(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **first_row, size_t *first_row_length,
  uint8_t **last_group, size_t *last_group_length) {

  FlatbuffersAggOpEvaluator agg_op_eval(agg_op, agg_op_length);
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter first_row_writer;
  FlatbuffersRowWriter last_group_writer;

  const tuix::Row *a = nullptr, *prev, *cur = nullptr;
  while (r.has_next()) {
    prev = cur;
    cur = r.next();

    if (a == nullptr) {
      first_row_writer.write(cur);
    }

    if (prev != nullptr && !agg_op_eval.is_same_group(prev, cur)) {
      agg_op_eval.reset_group();
    }
    agg_op_eval.aggregate(cur);
  }
  last_group_writer.write(agg_op_eval.evaluate());

  *first_row = first_row_writer.output_buffer();
  *first_row_length = first_row_writer.output_size();

  *last_group = last_group_writer.output_buffer();
  *last_group_length = last_group_writer.output_size();
}

void non_oblivious_aggregate_step2(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *next_partition_first_row, size_t next_partition_first_row_length,
  uint8_t *prev_partition_last_group, size_t prev_partition_last_group_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersAggOpEvaluator agg_op_eval(agg_op, agg_op_length);
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  EncryptedBlocksToRowReader next_partition_first_row_reader(
    next_partition_first_row, next_partition_first_row_length);
  EncryptedBlocksToRowReader prev_partition_last_group_reader(
    prev_partition_last_group, prev_partition_last_group_length);
  FlatbuffersRowWriter w;

  check(next_partition_first_row_reader.num_rows() <= 1,
        "Incorrect number of starting rows from next partition passed: expected 0 or 1, got %d\n",
        next_partition_first_row_reader.num_rows());
  check(prev_partition_last_group_reader.num_rows() <= 1,
        "Incorrect number of ending groups from prev partition passed: expected 0 or 1, got %d\n",
        prev_partition_last_group_reader.num_rows());

  const tuix::Row *next_partition_first_row_ptr =
    next_partition_first_row_reader.has_next() ? next_partition_first_row_reader.next() : nullptr;

  agg_op_eval.set(
    prev_partition_last_group_reader.has_next() ? prev_partition_last_group_reader.next() : nullptr);
  const tuix::Row *cur = nullptr, *next;
  while (next != next_partition_first_row_ptr) {
    // Populate cur and next to enable lookahead
    if (cur == nullptr) cur = r.next(); else cur = next;
    if (r.has_next()) next = r.next(); else next = next_partition_first_row_ptr;

    agg_op_eval.aggregate(cur);

    // Output the current aggregate if it is the last aggregate for its run
    if (next == nullptr || !agg_op_eval.is_same_group(cur, next)) {
      w.write(agg_op_eval.evaluate());
    }
  }

  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
}
