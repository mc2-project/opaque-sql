#include "Filter.h"

#include "Flatbuffers.h"
#include "common.h"

using namespace edu::berkeley::cs::rise::opaque;

void filter(uint8_t *condition, size_t condition_length,
            Verify *verify_set,
            uint8_t *input_rows, uint32_t input_rows_length, uint32_t num_rows,
            uint8_t **output_rows, uint32_t *output_rows_length, uint32_t *num_output_rows) {
  (void)verify_set;

  flatbuffers::Verifier v(condition, condition_length);
  check(v.VerifyBuffer<tuix::FilterExpr>(nullptr),
        "Corrupt FilterExpr %p of length %d\n", condition, condition_length);

  const tuix::FilterExpr* condition_expr = flatbuffers::GetRoot<tuix::FilterExpr>(condition);
  FlatbuffersExpressionEvaluator condition_eval(condition_expr->condition());

  FlatbuffersRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;
  const tuix::Row *in;

  for (uint32_t i = 0; i < num_rows; i++) {
    in = r.next();

    const tuix::Field *condition_result = condition_eval.eval(in);
    check(condition_result->value_type() == tuix::FieldUnion_BooleanField,
          "Filter expression returned %s instead of BooleanField\n",
          tuix::EnumNameFieldUnion(condition_result->value_type()));
    check(!condition_result->is_null(),
          "Filter expression returned null\n");

    bool keep_row = static_cast<const tuix::BooleanField *>(condition_result->value())->value();
    if (keep_row) {
      w.write(in);
    }
  }

  w.close();
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
  *num_output_rows = w.output_num_rows();
}
