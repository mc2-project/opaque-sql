#include "Join.h"

#include "ExpressionEvaluation.h"
#include "common.h"

void scan_collect_last_primary(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  const tuix::Row *last_primary = nullptr;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (join_expr_eval.is_primary(row)) {
      last_primary = row;
    }
  }

  FlatbuffersRowWriter w;
  if (last_primary != nullptr) {
    w.write(last_primary);
  }
  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
}

void non_oblivious_sort_merge_join(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *join_row, size_t join_row_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  EncryptedBlocksToRowReader j(join_row, join_row_length);
  FlatbuffersRowWriter w;

  check(j.num_rows() <= 1,
        "Incorrect number of join rows passed: expected 0 or 1, got %d\n", j.num_rows());
  const tuix::Row *primary = j.has_next() ? j.next() : nullptr;

  while (r.has_next()) {
    const tuix::Row *current = r.next();

    if (join_expr_eval.is_primary(current)) {
      check(!primary || !join_expr_eval.is_same_group(primary, current),
            "non_oblivious_sort_merge_join - primary table uniqueness constraint violation: "
            "multiple rows from the primary table had the same join attribute\n");
      primary = current; // advance to a new join attribute
    } else {
      if (primary != nullptr && join_expr_eval.is_same_group(primary, current)) {
        w.write(primary, current);
      }
    }
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
}
