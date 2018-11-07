#include "Filter.h"

#include "ExpressionEvaluation.h"
#include "common.h"

using namespace edu::berkeley::cs::rise::opaque;

void filter(uint8_t *condition, size_t condition_length,
            uint8_t *input_rows, size_t input_rows_length,
            uint8_t **output_rows, size_t *output_rows_length) {

  flatbuffers::Verifier v(condition, condition_length);
  if (!v.VerifyBuffer<tuix::FilterExpr>(nullptr)) {
      throw std::runtime_error(
          std::string("Corrupt FilterExpr buffer of length ")
          + std::to_string(condition_length));
  }

  const tuix::FilterExpr* condition_expr = flatbuffers::GetRoot<tuix::FilterExpr>(condition);
  FlatbuffersExpressionEvaluator condition_eval(condition_expr->condition());

  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    const tuix::Field *condition_result = condition_eval.eval(row);
    if (condition_result->value_type() != tuix::FieldUnion_BooleanField) {
      throw std::runtime_error(
        std::string("Filter expression expected to return BooleanField, instead returned ")
        + std::string(tuix::EnumNameFieldUnion(condition_result->value_type())));
    }
    if (condition_result->is_null()) {
      throw std::runtime_error("Filter expression returned null");
    }

    bool keep_row = static_cast<const tuix::BooleanField *>(condition_result->value())->value();
    if (keep_row) {
      w.write(row);
    }
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}
