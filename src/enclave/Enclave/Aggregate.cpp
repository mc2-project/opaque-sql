#include "Aggregate.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

void non_oblivious_aggregate_step1(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **first_row, size_t *first_row_length,
  uint8_t **last_group, size_t *last_group_length,
  uint8_t **last_row, size_t *last_row_length) {

  FlatbuffersAggOpEvaluator agg_op_eval(agg_op, agg_op_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter first_row_writer;
  RowWriter last_group_writer;
  RowWriter last_row_writer;

  FlatbuffersTemporaryRow prev, cur;
  while (r.has_next()) {
    prev.set(cur.get());
    cur.set(r.next());

    if (prev.get() == nullptr) {
      first_row_writer.append(cur.get());
    }

    if (!r.has_next()) {
      last_row_writer.append(cur.get());
    }

    if (prev.get() != nullptr && !agg_op_eval.is_same_group(prev.get(), cur.get())) {
      agg_op_eval.reset_group();
    }
    agg_op_eval.aggregate(cur.get());
  }
  last_group_writer.append(agg_op_eval.get_partial_agg());

  first_row_writer.output_buffer(first_row, first_row_length);
  last_group_writer.output_buffer(last_group, last_group_length);
  last_row_writer.output_buffer(last_row, last_row_length);
}

void non_oblivious_aggregate_step2(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *next_partition_first_row, size_t next_partition_first_row_length,
  uint8_t *prev_partition_last_group, size_t prev_partition_last_group_length,
  uint8_t *prev_partition_last_row, size_t prev_partition_last_row_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersAggOpEvaluator agg_op_eval(agg_op, agg_op_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowReader next_partition_first_row_reader(
    BufferRefView<tuix::EncryptedBlocks>(
      next_partition_first_row, next_partition_first_row_length));
  RowReader prev_partition_last_group_reader(
    BufferRefView<tuix::EncryptedBlocks>(
      prev_partition_last_group, prev_partition_last_group_length));
  RowReader prev_partition_last_row_reader(
    BufferRefView<tuix::EncryptedBlocks>(
      prev_partition_last_row, prev_partition_last_row_length));
  RowWriter w;

  if (next_partition_first_row_reader.num_rows() > 1) {
      throw std::runtime_error(
          std::string("Incorrect number of starting rows from next partition passed: expected 0 or 1, got ")
          + std::to_string(next_partition_first_row_reader.num_rows()));
  }
  if (prev_partition_last_group_reader.num_rows() > 1) {
      throw std::runtime_error(
          std::string("Incorrect number of ending groups from prev partition passed: expected 0 or 1, got ")
          + std::to_string(prev_partition_last_group_reader.num_rows()));
  }
  if (prev_partition_last_row_reader.num_rows() > 1) {
      throw std::runtime_error(
          std::string("Incorrect number of ending rows from prev partition passed: expected 0 or 1, got ")
          + std::to_string(prev_partition_last_row_reader.num_rows()));
  }

  const tuix::Row *next_partition_first_row_ptr =
    next_partition_first_row_reader.has_next() ? next_partition_first_row_reader.next() : nullptr;
  agg_op_eval.set(prev_partition_last_group_reader.has_next() ?
                  prev_partition_last_group_reader.next() : nullptr);
  const tuix::Row *prev_partition_last_row_ptr =
    prev_partition_last_row_reader.has_next() ? prev_partition_last_row_reader.next() : nullptr;

  FlatbuffersTemporaryRow prev, cur(prev_partition_last_row_ptr), next;
  bool stop = false;
  if (r.has_next()) {
    next.set(r.next());
  } else {
    stop = true;
  }
  while (!stop) {
    // Populate prev, cur, next to enable lookbehind and lookahead
    prev.set(cur.get());
    cur.set(next.get());
    if (r.has_next()) {
      next.set(r.next());
    } else {
      next.set(next_partition_first_row_ptr);
      stop = true;
    }

    if (prev.get() != nullptr && !agg_op_eval.is_same_group(prev.get(), cur.get())) {
      agg_op_eval.reset_group();
    }
    agg_op_eval.aggregate(cur.get());

    // Output the current aggregate if it is the last aggregate for its run
    if (next.get() == nullptr || !agg_op_eval.is_same_group(cur.get(), next.get())) {
      w.append(agg_op_eval.evaluate());
    }
  }

  w.output_buffer(output_rows, output_rows_length);
}
