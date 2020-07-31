#include "Join.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"
#include "EnclaveContext.h"

void scan_collect_last_primary(
  uint8_t *join_expr, size_t join_expr_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length) {

  FlatbuffersJoinExprEvaluator join_expr_eval(join_expr, join_expr_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  FlatbuffersTemporaryRow last_primary;

  // Accumulate all primary table rows from the same group as the last primary row into `w`.
  //
  // Because our distributed sorting algorithm uses range partitioning over the join keys, all
  // primary rows belonging to the same group will be colocated in the same partition. (The
  // corresponding foreign rows may be in the same partition or the next partition.) Therefore it is
  // sufficient to send primary rows at most one partition forward.
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (join_expr_eval.is_primary(row)) {
      if (!last_primary.get() || !join_expr_eval.is_same_group(last_primary.get(), row)) {
        w.clear();
        last_primary.set(row);
      }

      w.append(row);
    } else {
      w.clear();
      last_primary.set(nullptr);
    }
  }

  w.output_buffer(output_rows, output_rows_length, std::string("scanCollectLastPrimary"));
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

  RowWriter primary_group; // All rows in this group
  FlatbuffersTemporaryRow last_primary_of_group; // Last seen row
  while (j.has_next()) {
    const tuix::Row *row = j.next();
    primary_group.append(row);
    last_primary_of_group.set(row);
  }

  while (r.has_next()) {
    const tuix::Row *current = r.next();
    // std::cout << "CURRENT ROW: ";
    // for (unsigned int i = 1; i < current->field_values()->size(); i++) {
      // auto field = current->field_values()->Get(i);
      // auto field_val = static_cast<const tuix::IntegerField *> (field->value());
      // auto val = field_val->value();
      // std::cout << val << " ";
    // }

    if (join_expr_eval.is_primary(current)) {
      // If current row is from primary table
      if (last_primary_of_group.get()
          && join_expr_eval.is_same_group(last_primary_of_group.get(), current)) {
        // Add this row to the current group
        std::cout << std::endl;
        primary_group.append(current);
        last_primary_of_group.set(current);
      } else {
        // Advance to a new group
        primary_group.clear();
        primary_group.append(current);
        last_primary_of_group.set(current);
      }
    } else {
      // Current row isn't from primary table
      // Output the joined rows resulting from this foreign row
      if (last_primary_of_group.get()
          && join_expr_eval.is_same_group(last_primary_of_group.get(), current)) {
        auto primary_group_buffer = primary_group.output_buffer(std::string("NULL"));
        RowReader primary_group_reader(primary_group_buffer.view());
        while (primary_group_reader.has_next()) {
          // For each foreign key row, join all primary key rows in same group with it
          const tuix::Row *primary = primary_group_reader.next();

          if (!join_expr_eval.is_same_group(primary, current)) {
            throw std::runtime_error(
              std::string("Invariant violation: rows of primary_group "
                          "are not of the same group: ")
              + to_string(primary)
              + std::string(" vs ")
              + to_string(current));
          }

          w.append(primary, current, std::string("nonObliviousSortMergeJoin"));
        }
      }
    }
  }

  w.output_buffer(output_rows, output_rows_length, std::string("nonObliviousSortMergeJoin"));
}
