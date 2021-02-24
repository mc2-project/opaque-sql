#include "NonObliviousSortMergeJoin.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

/** C++ implementation of a non-oblivious sort merge join.
 * Rows MUST be tagged primary or secondary for this to work.
 */
void non_oblivious_sort_merge_join(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  tuix::JoinType join_type = join_expr_eval.get_join_type();
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  RowWriter primary_group;
  FlatbuffersTemporaryRow last_primary_of_group;

  bool pk_fk_match = false;

  while (r.has_next()) {
    const tuix::Row *current = r.next();

    if (join_expr_eval.is_primary(current)) {
      if (last_primary_of_group.get()
          && join_expr_eval.eval_condition(last_primary_of_group.get(), current)) {
        // Add this primary row to the current group
        primary_group.append(current);
        last_primary_of_group.set(current);
      } else {
        // If a new primary group is encountered
        if (join_type == tuix::JoinType_LeftAnti && !pk_fk_match) {
          auto primary_group_buffer = primary_group.output_buffer();
          RowReader primary_group_reader(primary_group_buffer.view());
          
          while (primary_group_reader.has_next()) {
            const tuix::Row *primary = primary_group_reader.next();
            w.append(primary);
          }
        }

        primary_group.clear();
        primary_group.append(current);
        last_primary_of_group.set(current);
        
        pk_fk_match = false;
      }
    } else {
      // Output the joined rows resulting from this foreign row
      if (last_primary_of_group.get()
          && join_expr_eval.eval_condition(last_primary_of_group.get(), current)) {
        auto primary_group_buffer = primary_group.output_buffer();
        RowReader primary_group_reader(primary_group_buffer.view());
        while (primary_group_reader.has_next()) {
          const tuix::Row *primary = primary_group_reader.next();

          if (!join_expr_eval.eval_condition(primary, current)) {
            throw std::runtime_error(
              std::string("Invariant violation: rows of primary_group "
                          "are not of the same group: ")
              + to_string(primary)
              + std::string(" vs ")
              + to_string(current));
          }

          if (join_type == tuix::JoinType_Inner) {
            w.append(primary, current);
          } else if (join_type == tuix::JoinType_LeftSemi) {
            // Only output the pk group ONCE
            if (!pk_fk_match) {
              w.append(primary);
            }
          }
        }

        pk_fk_match = true;
      } else {
        // If pk_fk_match were true, and the code got to here, then that means the group match has not been "cleared" yet
        // It will be processed when the code advances to the next pk group
        pk_fk_match &= true;
      }
    }
  }

  if (join_type == tuix::JoinType_LeftAnti && !pk_fk_match) {
    auto primary_group_buffer = primary_group.output_buffer();
    RowReader primary_group_reader(primary_group_buffer.view());
          
    while (primary_group_reader.has_next()) {
      const tuix::Row *primary = primary_group_reader.next();
      w.append(primary);
    }
  }

  w.output_buffer(output_rows, output_rows_length);
}
