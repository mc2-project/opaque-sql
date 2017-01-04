#include "Filter.h"

#include "ExpressionEvaluation.h"
#include "common.h"

using namespace edu::berkeley::cs::rise::opaque;

void filter(uint8_t *condition, size_t condition_length,
            uint8_t *input_rows, uint32_t input_rows_length,
            uint8_t **output_rows, uint32_t *output_rows_length, uint32_t *num_output_rows) {

  flatbuffers::Verifier v(condition, condition_length);
  check(v.VerifyBuffer<tuix::FilterExpr>(nullptr),
        "Corrupt FilterExpr %p of length %d\n", condition, condition_length);

  const tuix::FilterExpr* condition_expr = flatbuffers::GetRoot<tuix::FilterExpr>(condition);
  FlatbuffersExpressionEvaluator condition_eval(condition_expr->condition());

  FlatbuffersRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  for (auto it = r.begin(); it != r.end(); ++it) {
    const tuix::Field *condition_result = condition_eval.eval(*it);
    check(condition_result->value_type() == tuix::FieldUnion_BooleanField,
          "Filter expression returned %s instead of BooleanField\n",
          tuix::EnumNameFieldUnion(condition_result->value_type()));
    check(!condition_result->is_null(),
          "Filter expression returned null\n");

    bool keep_row = static_cast<const tuix::BooleanField *>(condition_result->value())->value();
    if (keep_row) {
      w.write(*it);
    }
  }

  w.close();
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
  *num_output_rows = w.output_num_rows();
}
