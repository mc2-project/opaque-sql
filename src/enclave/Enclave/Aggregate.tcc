// -*- mode: C++ -*-

#include "util.h"
#include "NewInternalTypes.h"

int printf(const char *fmt, ...);

// non-oblivious aggregation
// a single scan, assume that external_sort is already called
template<typename AggregatorType>
void non_oblivious_aggregate(Verify *verify_set,
                             uint8_t *input_rows, uint32_t input_rows_length,
                             uint32_t num_rows,
                             uint8_t *output_rows, uint32_t output_rows_length,
                             uint32_t *actual_output_rows_length, uint32_t *num_output_rows) {

  (void)output_rows_length;
  
  RowReader reader(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter writer(output_rows);
  writer.set_self_task_id(verify_set->get_self_task_id());

  NewRecord prev_row, cur_row, output_row;
  AggregatorType agg;

  uint32_t num_output_rows_result = 0;
  for (uint32_t i = 0; i < num_rows; i++) {
    if (i == 0) {
      reader.read(&prev_row);
      continue;
    }

    agg.aggregate(&prev_row);
    reader.read(&cur_row);

    if (!agg.grouping_attrs_equal(&cur_row)) {
      output_row.clear();
      agg.append_result(&output_row, false);
      writer.write(&output_row);
      num_output_rows_result++;
    }

    if (i == num_rows - 1) {
      agg.aggregate(&cur_row);

      output_row.clear();
      agg.append_result(&output_row, false);
      writer.write(&output_row);
      num_output_rows_result++;
    }

    prev_row.set(&cur_row);
  }

  if (num_rows == 1) {
    agg.aggregate(&prev_row);
    output_row.clear();
    agg.append_result(&output_row, false);
    writer.write(&output_row);
    num_output_rows_result++;
  }

  writer.close();
  *actual_output_rows_length = writer.bytes_written();
  *num_output_rows = num_output_rows_result;
}
