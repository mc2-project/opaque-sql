#include "Filter.h"

#include <string>

void filter(int op_code,
            Verify *verify_set,
            uint8_t *input_rows, uint32_t input_rows_length,
            uint32_t num_rows,
            uint8_t *output_rows, uint32_t output_rows_length,
            uint32_t *actual_output_rows_length, uint32_t *num_output_rows) {
  (void)output_rows_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewRecord cur;

  uint32_t num_output_rows_result = 0;
  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    if (filter_single_row(op_code, &cur)) {
      w.write(&cur);
      num_output_rows_result++;
    }
  }

  w.close();
  *actual_output_rows_length = w.bytes_written();
  *num_output_rows = num_output_rows_result;
}

bool filter_single_row(int op_code, NewRecord *cur) {
  switch (op_code) {
  case OP_FILTER_COL2_GT3:
    return *reinterpret_cast<const uint32_t *>(cur->get_attr_value(2)) > 3;
  case OP_FILTER_COL4_GT_25:
    return *reinterpret_cast<const uint32_t *>(cur->get_attr_value(4)) > 25;
  case OP_FILTER_COL4_GT_40:
    return *reinterpret_cast<const uint32_t *>(cur->get_attr_value(4)) > 40;
  case OP_FILTER_COL4_GT_45:
    return *reinterpret_cast<const uint32_t *>(cur->get_attr_value(4)) > 45;
  case OP_BD1_FILTER:
    return *reinterpret_cast<const uint32_t *>(cur->get_attr_value(2)) > 1000;
  case OP_BD2_FILTER_NOT_DUMMY:
  case OP_FILTER_NOT_DUMMY:
    return !cur->is_dummy();
  case OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01:
  {
    uint64_t date = *reinterpret_cast<const uint64_t *>(cur->get_attr_value(1));
    return date >= 315561600 && date <= 323424000;
  }
  case OP_FILTER_COL2_CONTAINS_MAROON:
  {
    std::string p_name(
      reinterpret_cast<const char *>(cur->get_attr_value(2)), cur->get_attr_len(2));
    return p_name.find("maroon") != std::string::npos;
  }
  default:
    printf("filter_single_row: unknown opcode %d\n", op_code);
    assert(false);
  }
  return true;
}
