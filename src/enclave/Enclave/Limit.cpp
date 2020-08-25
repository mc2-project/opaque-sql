#include "Limit.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

using namespace edu::berkeley::cs::rise::opaque;

// Count the number of rows in a single partition
// The partition ID must be known
void count_rows_per_partition(uint8_t *input_rows, size_t input_rows_length,
                              uint8_t **output_rows, size_t *output_rows_length) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;
  uint32_t num_rows = r.num_rows();

  flatbuffers::FlatBufferBuilder builder;
  std::vector<const tuix::Field *> output(1);
  output[0] = flatbuffers::GetTemporaryPointer(
    builder,
    tuix::CreateField(builder,
                      tuix::FieldUnion_IntegerField,
                      tuix::CreateIntegerField(builder, num_rows).Union(),
                      true));
  
  w.append(output);
  w.output_buffer(output_rows, output_rows_length);
}

// Based on the limit, calculate the number of rows to return for each partition
void compute_num_rows_per_partition(uint32_t limit,
                                    uint8_t *input_rows, size_t input_rows_length,
                                    uint8_t **output_rows, size_t *output_rows_length) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  uint32_t current_num_rows = 0;
  flatbuffers::FlatBufferBuilder builder;
  std::vector<const tuix::Field *> output(1);

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    uint32_t num_rows = static_cast<uint32_t>(
                          static_cast<const tuix::IntegerField *>(row->field_values()->Get(0)->value())->value());
    if (current_num_rows >= limit) {
      num_rows = 0;
    }
    else if (current_num_rows + num_rows >= limit) {
      num_rows = current_num_rows - limit;
    }
    output[0] = flatbuffers::GetTemporaryPointer(
      builder,
      tuix::CreateField(builder,
                        tuix::FieldUnion_IntegerField,
                        tuix::CreateIntegerField(builder, num_rows).Union(),
                        true));
    w.append(output);
    current_num_rows += num_rows;
  }
  w.output_buffer(output_rows, output_rows_length);
}

void limit_return_rows(uint8_t *limit_rows, size_t limit_rows_length,
                       uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length) {
  RowReader r_limit(BufferRefView<tuix::EncryptedBlocks>(limit_rows, limit_rows_length));
  const tuix::Row *limit_row = r_limit.next();
  uint32_t num_rows = static_cast<uint32_t>(
                        static_cast<const tuix::IntegerField *>(limit_row->field_values()->Get(0)->value())->value());

  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;
  
  if (num_rows > 0) {
    uint32_t current_num_rows = 0;
  
    while (r.has_next() && current_num_rows < num_rows) {
      const tuix::Row *row = r.next();
      w.append(row);
      ++current_num_rows;
    }
  }
  w.output_buffer(output_rows, output_rows_length);
}
