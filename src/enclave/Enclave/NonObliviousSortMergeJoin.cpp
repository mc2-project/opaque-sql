#include "NonObliviousSortMergeJoin.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

/** 
 * C++ implementation of a non-oblivious sort merge join.
 * Rows MUST be tagged primary or secondary for this to work.
 */

void test_rows_same_group(FlatbuffersJoinExprEvaluator &join_expr_eval,
                          const tuix::Row *primary,
                          const tuix::Row *current) {
  if (!join_expr_eval.is_same_group(primary, current)) {
    throw std::runtime_error(
                             std::string("Invariant violation: rows of primary_group "
                                         "are not of the same group: ")
                             + to_string(primary)
                             + std::string(" vs ")
                             + to_string(current));
  }
}

void write_output_rows(RowWriter &group, RowWriter &w) {
  auto group_buffer = group.output_buffer();
  RowReader group_reader(group_buffer.view());
          
  while (group_reader.has_next()) {
    const tuix::Row *row = group_reader.next();
    w.append(row);
  }  
}

/** 
 * Sort merge equi join algorithm
 * Input: the rows are unioned from both the primary (or left) table and the non-primary (or right) table
 * 
 * Outer loop: iterate over all input rows
 * 
 * If it's a row from the left table
 * - Add it to the current group
 * - Otherwise start a new group
 *   - If it's a left semi/anti join, output the primary_matched_rows/primary_unmatched_rows
 * 
 * If it's a row from the right table
 * - Inner join: iterate over current left group, output the joined row only if the condition is satisfied
 * - Left semi/anti join: iterate over `primary_unmatched_rows`, add a matched row to `primary_matched_rows` 
 *   and remove from `primary_unmatched_rows`
 * 
 * After loop: output the last group left semi/anti join
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
  RowWriter primary_matched_rows, primary_unmatched_rows; // This is only used for left semi/anti join

  while (r.has_next()) {
    const tuix::Row *current = r.next();

    if (join_expr_eval.is_primary(current)) {
      if (last_primary_of_group.get()
          && join_expr_eval.is_same_group(last_primary_of_group.get(), current)) {
        
        // Add this primary row to the current group
        // If this is a left semi/anti join, also add the rows to primary_unmatched_rows
        primary_group.append(current);
        if (join_type == tuix::JoinType_LeftSemi || join_type == tuix::JoinType_LeftAnti) {
          primary_unmatched_rows.append(current);
        }
        last_primary_of_group.set(current);
        
      } else {
        // If a new primary group is encountered
        if (join_type == tuix::JoinType_LeftSemi) {
          write_output_rows(primary_matched_rows, w);
        } else if (join_type == tuix::JoinType_LeftAnti) {
          write_output_rows(primary_unmatched_rows, w);
        }

        primary_group.clear();
        primary_unmatched_rows.clear();
        primary_matched_rows.clear();
        
        primary_group.append(current);
        primary_unmatched_rows.append(current);
        last_primary_of_group.set(current);
      }
    } else {
      if (last_primary_of_group.get()
          && join_expr_eval.is_same_group(last_primary_of_group.get(), current)) {
        if (join_type == tuix::JoinType_Inner) {       
          auto primary_group_buffer = primary_group.output_buffer();
          RowReader primary_group_reader(primary_group_buffer.view());
          while (primary_group_reader.has_next()) {
            const tuix::Row *primary = primary_group_reader.next();
            test_rows_same_group(join_expr_eval, primary, current);

            if (join_expr_eval.eval_condition(primary, current)) {
              w.append(primary, current);
            }
          }
        } else if (join_type == tuix::JoinType_LeftSemi || join_type == tuix::JoinType_LeftAnti) {
          auto primary_unmatched_rows_buffer = primary_unmatched_rows.output_buffer();
          RowReader primary_unmatched_rows_reader(primary_unmatched_rows_buffer.view());
          RowWriter new_primary_unmatched_rows;

          while (primary_unmatched_rows_reader.has_next()) {
            const tuix::Row *primary = primary_unmatched_rows_reader.next();
            test_rows_same_group(join_expr_eval, primary, current);
            if (join_expr_eval.eval_condition(primary, current)) {
              primary_matched_rows.append(primary);
            } else {
              new_primary_unmatched_rows.append(primary);
            }
          }
           
          // Reset primary_unmatched_rows
          primary_unmatched_rows.clear();
          auto new_primary_unmatched_rows_buffer = new_primary_unmatched_rows.output_buffer();
          RowReader new_primary_unmatched_rows_reader(new_primary_unmatched_rows_buffer.view());
          while (new_primary_unmatched_rows_reader.has_next()) {
            primary_unmatched_rows.append(new_primary_unmatched_rows_reader.next());
          }
        }
      }
    }
  }

  if (join_type == tuix::JoinType_LeftSemi) {
    write_output_rows(primary_matched_rows, w);
  } else if (join_type == tuix::JoinType_LeftAnti) {
    write_output_rows(primary_unmatched_rows, w);
  }

  w.output_buffer(output_rows, output_rows_length);
}
