#include "Join.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

void scan_collect_last_primary(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (join_expr_eval.is_primary(row)) {
      w.clear();
      w.append(row);
    }
  }

  w.output_buffer(output_rows, output_rows_length);
}

void non_oblivious_sort_merge_join(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *join_row, size_t join_row_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowReader j(BufferRefView<tuix::EncryptedBlocks>(join_row, join_row_length));
  RowWriter w;

  if (j.num_rows() > 1) {
    throw std::runtime_error(
      std::string("Incorrect number of join rows passed: expected 0 or 1, got ")
      + std::to_string(j.num_rows()));
  }

  FlatbuffersTemporaryRow primary(j.has_next() ? j.next() : nullptr);

  while (r.has_next()) {
    const tuix::Row *current = r.next();

    if (join_expr_eval.is_primary(current)) {
      if (primary.get() && join_expr_eval.is_same_group(primary.get(), current)) {
        throw std::runtime_error(
          "non_oblivious_sort_merge_join - primary table uniqueness constraint violation: "
          "multiple rows from the primary table had the same join attribute");
      }
      // Advance to a new join attribute
      primary.set(current);
    } else {
      if (primary.get() != nullptr && join_expr_eval.is_same_group(primary.get(), current)) {
        w.append(primary.get(), current);
      }
    }
  }

  w.output_buffer(output_rows, output_rows_length);
}
