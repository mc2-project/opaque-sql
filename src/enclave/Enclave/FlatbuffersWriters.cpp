#include "FlatbuffersWriters.h"

void RowWriter::clear() {
  builder.Clear();
  rows_vector.clear();
  total_num_rows = 0;
  enc_block_builder.Clear();
  enc_block_vector.clear();
  finished = false;
}

void RowWriter::append(const tuix::Row *row) {
  rows_vector.push_back(flatbuffers_copy(row, builder));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  flatbuffers::uoffset_t num_fields = row_fields.size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    field_values[i] = flatbuffers_copy<tuix::Field>(row_fields[i], builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  flatbuffers::uoffset_t num_fields = row1->field_values()->size() + row2->field_values()->size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  flatbuffers::uoffset_t i = 0;
  for (auto it = row1->field_values()->begin(); it != row1->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  for (auto it = row2->field_values()->begin(); it != row2->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

UntrustedBufferRef<tuix::EncryptedBlocks> RowWriter::output_buffer() {
  if (!finished) {
    finish_blocks();
  }

  uint8_t *buf_ptr;
  ocall_malloc(enc_block_builder.GetSize(), &buf_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(), enc_block_builder.GetBufferPointer(), enc_block_builder.GetSize());

  UntrustedBufferRef<tuix::EncryptedBlocks> buffer(
    std::move(buf), enc_block_builder.GetSize());
  return buffer;
}

void RowWriter::output_buffer(uint8_t **output_rows, size_t *output_rows_length) {
  auto result = output_buffer();
  *output_rows = result.buf.release();
  *output_rows_length = result.len;
}

uint32_t RowWriter::num_rows() {
  return total_num_rows;
}


void RowWriter::maybe_finish_block() {
  if (builder.GetSize() >= MAX_BLOCK_SIZE) {
    finish_block();
  }
}

void RowWriter::finish_block() {
  builder.Finish(tuix::CreateRowsDirect(builder, &rows_vector));
  size_t enc_rows_len = enc_size(builder.GetSize());

  uint8_t *enc_rows_ptr = nullptr;
  ocall_malloc(enc_rows_len, &enc_rows_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> enc_rows(enc_rows_ptr, &ocall_free);
  encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows.get());

  enc_block_vector.push_back(
    tuix::CreateEncryptedBlock(
      enc_block_builder,
      rows_vector.size(),
      enc_block_builder.CreateVector(enc_rows.get(), enc_rows_len)));

  builder.Clear();
  rows_vector.clear();
}

flatbuffers::Offset<tuix::EncryptedBlocks> RowWriter::finish_blocks() {
  if (rows_vector.size() > 0) {
    finish_block();
  }
  auto result = tuix::CreateEncryptedBlocksDirect(enc_block_builder, &enc_block_vector);
  enc_block_builder.Finish(result);
  enc_block_vector.clear();

  finished = true;

  return result;
}

void SortedRunsWriter::clear() {
  container.clear();
  runs.clear();
}

void SortedRunsWriter::append(const tuix::Row *row) {
  container.append(row);
}

void SortedRunsWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  container.append(row_fields);
}

void SortedRunsWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  container.append(row1, row2);
}

void SortedRunsWriter::finish_run() {
  runs.push_back(container.finish_blocks());
}

uint32_t SortedRunsWriter::num_runs() {
  return runs.size();
}

UntrustedBufferRef<tuix::SortedRuns> SortedRunsWriter::output_buffer() {
  container.enc_block_builder.Finish(
    tuix::CreateSortedRunsDirect(container.enc_block_builder, &runs));

  uint8_t *buf_ptr;
  ocall_malloc(container.enc_block_builder.GetSize(), &buf_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(),
         container.enc_block_builder.GetBufferPointer(),
         container.enc_block_builder.GetSize());

  UntrustedBufferRef<tuix::SortedRuns> buffer(
    std::move(buf), container.enc_block_builder.GetSize());
  return buffer;
}

RowWriter *SortedRunsWriter::as_row_writer() {
  if (runs.size() > 1) {
    throw std::runtime_error("Invalid attempt to convert SortedRunsWriter with more than one run "
                             "to RowWriter");
  }

  return &container;
}
