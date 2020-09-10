#include "Filter.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

using namespace edu::berkeley::cs::rise::opaque;

void filter(uint8_t *condition, size_t condition_length,
            uint8_t *input_rows, size_t input_rows_length,
            uint8_t **output_rows, size_t *output_rows_length) {

  BufferRefView<tuix::FilterExpr> condition_buf(condition, condition_length);
  condition_buf.verify();
  FlatbuffersExpressionEvaluator condition_eval(condition_buf.root()->condition());
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    const tuix::Field *condition_result = condition_eval.eval(row);
    if (condition_result->value_type() != tuix::FieldUnion_BooleanField) {
      throw std::runtime_error(
        std::string("Filter expression expected to return BooleanField, instead returned ")
        + std::string(tuix::EnumNameFieldUnion(condition_result->value_type())));
    }

    // If condition_result is NULL, then always return false
    bool keep_row = !condition_result->is_null() &&
      static_cast<const tuix::BooleanField *>(condition_result->value())->value();
    if (keep_row) {
      w.append(row);
    }
  }

  w.output_buffer(output_rows, output_rows_length, std::string("filter"));
}
