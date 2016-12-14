#include "Filter.h"

#include <string>

#include "FilterExpr_generated.h"

void filter(uint8_t *condition, size_t condition_length,
            Verify *verify_set,
            uint8_t *input_rows, uint32_t input_rows_length, uint32_t num_rows,
            uint8_t **output_rows, uint32_t *output_rows_length, uint32_t *num_output_rows) {
  (void)condition;
  (void)condition_length;
  (void)verify_set;

  flatbuffers::Verifier v(condition, condition_length);
  check(tuix::VerifyFilterExprBuffer(v),
        "Corrupt FilterExpr %p of length %d\n", condition, condition_length);

  const tuix::FilterExpr* condition_expr = tuix::GetFilterExpr(condition);
  FlatbuffersExpressionEvaluator condition_eval(condition_expr->condition());

  FlatbuffersRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;
  const tuix::Row *in;

  for (uint32_t i = 0; i < num_rows; i++) {
    in = r.next();
    print(in);

    w.write(in);
//    if (filter_single_row(op_code, &cur)) {
//      w.write(&cur);
//      num_output_rows_result++;
//    }
  }

  w.close();
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
  *num_output_rows = w.output_num_rows();
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
  case OP_FILTER_COL3_DATE_BETWEEN_1980_01_01_AND_1980_04_01:
  {
    uint64_t date = *reinterpret_cast<const uint64_t *>(cur->get_attr_value(3));
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
