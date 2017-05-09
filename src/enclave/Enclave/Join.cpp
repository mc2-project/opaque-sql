#include "Join.h"

#include "ExpressionEvaluation.h"
#include "common.h"

void non_oblivious_sort_merge_join(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  flatbuffers::Verifier v(join_expr, join_expr_length);
  check(v.VerifyBuffer<tuix::FilterExpr>(nullptr),
        "Corrupt JoinExpr %p of length %d\n", join_expr, join_expr_length);

  const tuix::JoinExpr* join_expr_deser = flatbuffers::GetRoot<tuix::JoinExpr>(join_expr);
  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr_deser);
  check(join_expr_deser->join_type() == tuix::JoinType_Inner,
    "Only inner join is currently supported\n");

  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  const tuix::Row *primary = nullptr;
  while (r.has_next()) {
    const tuix::Row *current = r.next();
    print(current);

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
