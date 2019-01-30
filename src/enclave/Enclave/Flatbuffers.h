// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <algorithm>
#include <time.h>

#include "EncryptedBlock_generated.h"
#include "Expr_generated.h"
#include "Rows_generated.h"
#include "operators_generated.h"

#include "Crypto.h"
#include "common.h"
#include "Enclave_t.h"
#include "util.h"

#ifndef FLATBUFFERS_H
#define FLATBUFFERS_H

using namespace edu::berkeley::cs::rise::opaque;

/** Wrapper around a date that provides conversions to various types. */
class Date {
public:
  Date(const int32_t &days_since_epoch) : days_since_epoch(days_since_epoch) {}

  explicit operator int32_t() const { return days_since_epoch; }
  explicit operator int64_t() const { return days_since_epoch; }
  explicit operator float() const { return days_since_epoch; }
  explicit operator double() const { return days_since_epoch; }

  int32_t days_since_epoch;
};

std::string to_string(const Date &date);
std::string to_string(const tuix::Field *f);
std::string to_string(const tuix::BooleanField *f);
std::string to_string(const tuix::IntegerField *f);
std::string to_string(const tuix::LongField *f);
std::string to_string(const tuix::FloatField *f);
std::string to_string(const tuix::DoubleField *f);
std::string to_string(const tuix::StringField *f);
std::string to_string(const tuix::DateField *f);
std::string to_string(const tuix::BinaryField *f);
std::string to_string(const tuix::ByteField *f);
std::string to_string(const tuix::CalendarIntervalField *f);
std::string to_string(const tuix::NullField *f);
std::string to_string(const tuix::ShortField *f);
std::string to_string(const tuix::TimestampField *f);
std::string to_string(const tuix::ArrayField *f);
std::string to_string(const tuix::MapField *f);

template<typename T>
flatbuffers::Offset<T> flatbuffers_copy(
  const T *flatbuffers_obj, flatbuffers::FlatBufferBuilder& builder, bool force_null = false);
template<>
flatbuffers::Offset<tuix::Row> flatbuffers_copy(
  const tuix::Row *row, flatbuffers::FlatBufferBuilder& builder, bool force_null);
template<>
flatbuffers::Offset<tuix::Field> flatbuffers_copy(
  const tuix::Field *field, flatbuffers::FlatBufferBuilder& builder, bool force_null);

/** Helper function for casting among primitive types and strings. */
template<typename InputTuixField, typename InputType>
flatbuffers::Offset<tuix::Field> flatbuffers_cast(
  const tuix::Cast *cast, const tuix::Field *value, flatbuffers::FlatBufferBuilder& builder,
  bool result_is_null) {
  InputType value_eval = static_cast<const InputTuixField *>(value->value())->value();
  switch (cast->target_type()) {
  case tuix::ColType_IntegerType:
  {
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_IntegerField,
      tuix::CreateIntegerField(builder, static_cast<int32_t>(value_eval)).Union(),
      result_is_null);
  }
  case tuix::ColType_LongType:
  {
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_LongField,
      tuix::CreateLongField(builder, static_cast<int64_t>(value_eval)).Union(),
      result_is_null);
  }
  case tuix::ColType_FloatType:
  {
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_FloatField,
      tuix::CreateFloatField(builder, static_cast<float>(value_eval)).Union(),
      result_is_null);
  }
  case tuix::ColType_DoubleType:
  {
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_DoubleField,
      tuix::CreateDoubleField(builder, static_cast<double>(value_eval)).Union(),
      result_is_null);
  }
  case tuix::ColType_StringType:
  {
    using std::to_string;
    std::string str = to_string(value_eval);
    std::vector<uint8_t> str_vec(str.begin(), str.end());
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_StringField,
      tuix::CreateStringFieldDirect(builder, &str_vec, str_vec.size()).Union(),
      result_is_null);
  }
  default:
  {
    throw std::runtime_error(
      std::string("Can't cast ")
      + std::string(typeid(InputTuixField).name())
      + std::string(" to ")
      + std::string(tuix::EnumNameColType(cast->target_type())));
  }
  }
}

template<typename T> flatbuffers::Offset<T> GetOffset(
  flatbuffers::FlatBufferBuilder &builder, const T *pointer) {
  return flatbuffers::Offset<T>(builder.GetCurrentBufferPointer() + builder.GetSize()
                                - reinterpret_cast<const uint8_t *>(pointer));
}

class EncryptedBlocksToEncryptedBlockReader {
public:
  EncryptedBlocksToEncryptedBlockReader(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v(buf, len);
    if (!v.VerifyBuffer<tuix::EncryptedBlocks>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt EncryptedBlocks buffer of length ")
        + std::to_string(len));
    }
    encrypted_blocks = flatbuffers::GetRoot<tuix::EncryptedBlocks>(buf);
    debug("EncryptedBlocksToEncryptedBlockReader: %d blocks\n", encrypted_blocks->blocks()->size());
  }
  flatbuffers::Vector<flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator begin() {
    return encrypted_blocks->blocks()->begin();
  }
  flatbuffers::Vector<flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator end() {
    return encrypted_blocks->blocks()->end();
  }

private:
  const tuix::EncryptedBlocks *encrypted_blocks;
};

class EncryptedBlockToRowReader {
public:
  EncryptedBlockToRowReader() : rows(nullptr), initialized(false) {}

  void reset(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v(buf, len);
    if (!v.VerifyBuffer<tuix::EncryptedBlock>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt EncryptedBlock buffer of length ")
        + std::to_string(len));
    }
    auto encrypted_block = flatbuffers::GetRoot<tuix::EncryptedBlock>(buf);
    init(encrypted_block);
  }

  void reset(const tuix::EncryptedBlock *encrypted_block) {
    init(encrypted_block);
  }

  bool has_next() {
    return initialized && row_idx < rows->rows()->size();
  }

  const tuix::Row *next() {
    return rows->rows()->Get(row_idx++);
  }

  flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator begin() {
    return rows->rows()->begin();
  }

  flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator end() {
    return rows->rows()->end();
  }

private:
  void init(const tuix::EncryptedBlock *encrypted_block) {
    uint32_t num_rows = encrypted_block->num_rows();

    const size_t rows_len = dec_size(encrypted_block->enc_rows()->size());
    rows_buf.reset(new uint8_t[rows_len]);
    decrypt(encrypted_block->enc_rows()->data(), encrypted_block->enc_rows()->size(),
            rows_buf.get());
    flatbuffers::Verifier v(rows_buf.get(), rows_len);
    if (!v.VerifyBuffer<tuix::Rows>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt Rows buffer of length ")
        + std::to_string(rows_len));
    }

    rows = flatbuffers::GetRoot<tuix::Rows>(rows_buf.get());
    if (rows->rows()->size() != num_rows) {
      throw std::runtime_error(
        std::string("EncryptedBlock claimed to contain ")
        + std::to_string(num_rows)
        + std::string("rows but actually contains ")
        + std::to_string(rows->rows()->size())
        + std::string(" rows"));
    }

    row_idx = 0;
    initialized = true;
  }

  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
  uint32_t row_idx;
  bool initialized;
};

class EncryptedBlocksToRowReader {
  typedef flatbuffers::Vector<
    flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator EncryptedBlockIterator;
  typedef flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator RowIterator;

public:
  EncryptedBlocksToRowReader(uint8_t *buf, size_t len)
    : block_idx(0) {
    flatbuffers::Verifier v(buf, len);
    if (!v.VerifyBuffer<tuix::EncryptedBlocks>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt EncryptedBlocks buffer of length ")
        + std::to_string(len));
    }
    encrypted_blocks = flatbuffers::GetRoot<tuix::EncryptedBlocks>(buf);
    init_row_reader();
  }

  EncryptedBlocksToRowReader(const tuix::EncryptedBlocks *encrypted_blocks)
    : encrypted_blocks(encrypted_blocks), block_idx(0) {
    init_row_reader();
  }

  uint32_t num_rows() {
    uint32_t result = 0;
    for (auto it = encrypted_blocks->blocks()->begin();
         it != encrypted_blocks->blocks()->end(); ++it) {
      result += it->num_rows();
    }
    return result;
  }

  bool has_next() {
    return r.has_next() || block_idx + 1 < encrypted_blocks->blocks()->size();
  }

  const tuix::Row *next() {
    // Note: this will invalidate any pointers returned by previous invocations of this method
    if (!r.has_next()) {
      assert(block_idx + 1 < encrypted_blocks->blocks()->size());
      block_idx++;
      init_row_reader();
    }

    return r.next();
  }

private:
  void init_row_reader() {
    if (block_idx < encrypted_blocks->blocks()->size()) {
      r.reset(encrypted_blocks->blocks()->Get(block_idx));
    }
  }

  const tuix::EncryptedBlocks *encrypted_blocks;
  uint32_t block_idx;
  EncryptedBlockToRowReader r;
};

class SortedRunsReader {
public:
  SortedRunsReader(uint8_t *buf, size_t len)
    : buf(nullptr) {
    reset(buf, len);
  }

  void reset(uint8_t *buf, size_t len) {
    this->buf = buf;

    flatbuffers::Verifier v(buf, len);
    if (!v.VerifyBuffer<tuix::SortedRuns>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt SortedRuns buffer of length ")
        + std::to_string(len));
    }
    sorted_runs = flatbuffers::GetRoot<tuix::SortedRuns>(buf);

    run_readers.clear();
    for (auto it = sorted_runs->runs()->begin(); it != sorted_runs->runs()->end(); ++it) {
      run_readers.push_back(EncryptedBlocksToRowReader(*it));
    }
  }

  uint32_t num_runs() {
    return sorted_runs->runs()->size();
  }

  bool run_has_next(uint32_t run_idx) {
    return run_readers[run_idx].has_next();
  }

  const tuix::Row *next_from_run(uint32_t run_idx) {
    return run_readers[run_idx].next();
  }

private:
  uint8_t *buf;
  const tuix::SortedRuns *sorted_runs;
  std::vector<EncryptedBlocksToRowReader> run_readers;
};


class UntrustedMemoryAllocator : public flatbuffers::Allocator {
public:
  virtual uint8_t *allocate(size_t size) {
    uint8_t *result = nullptr;
    ocall_malloc(size, &result);
    return result;
  }
  virtual void deallocate(uint8_t *p, size_t size) {
    (void)size;
    ocall_free(p);
  }
};

class FlatbuffersRowWriter {
public:
  FlatbuffersRowWriter()
    : builder(), rows_vector(), total_num_rows(0), untrusted_alloc(),
      enc_block_builder(1024, &untrusted_alloc) {}

  void clear() {
    builder.Clear();
    rows_vector.clear();
    total_num_rows = 0;
    enc_block_builder.Clear();
    enc_block_vector.clear();
  }

  /** Copy the given Row to the output. */
  void write(const tuix::Row *row) {
    rows_vector.push_back(flatbuffers_copy(row, builder));
    total_num_rows++;
    maybe_finish_block();
  }

  /** Copy the given Fields to the output. */
  void write(const std::vector<const tuix::Field *> &row_fields) {
    flatbuffers::uoffset_t num_fields = row_fields.size();
    std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
    for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
      field_values[i] = flatbuffers_copy<tuix::Field>(row_fields[i], builder);
    }
    rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
    total_num_rows++;
    maybe_finish_block();
  }

  /**
   * Concatenate the fields of the two given Rows and write the resulting single Row to the output.
   */
  void write(const tuix::Row *row1, const tuix::Row *row2) {
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

  void write_encrypted_block() {
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

  flatbuffers::Offset<tuix::EncryptedBlocks> write_encrypted_blocks() {
    if (rows_vector.size() > 0) {
      write_encrypted_block();
    }
    auto result = tuix::CreateEncryptedBlocksDirect(enc_block_builder, &enc_block_vector);
    enc_block_vector.clear();
    return result;
  }

  flatbuffers::Offset<tuix::SortedRuns> write_sorted_runs(
    std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> &enc_blocks_vector) {
    return tuix::CreateSortedRunsDirect(enc_block_builder, &enc_blocks_vector);
  }

  void write_shuffle_output(
    flatbuffers::Offset<tuix::EncryptedBlocks> encrypted_blocks,
    uint32_t destination_partition) {

    shuffle_output_vector.push_back(
      tuix::CreateShuffleOutputDirect(
        enc_block_builder,
        destination_partition,
        encrypted_blocks));
  }

  flatbuffers::Offset<tuix::ShuffleOutputs> write_shuffle_outputs() {
    return tuix::CreateShuffleOutputsDirect(enc_block_builder, &shuffle_output_vector);
  }

  template<typename T>
  void finish(flatbuffers::Offset<T> root) {
    enc_block_builder.Finish<T>(root);
  }

  std::unique_ptr<uint8_t, decltype(&ocall_free)> output_buffer() {
    uint8_t *buf_ptr;
    ocall_malloc(output_size(), &buf_ptr);

    std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
    memcpy(buf.get(), enc_block_builder.GetBufferPointer(), output_size());
    return buf;
  }

  size_t output_size() {
    return enc_block_builder.GetSize();
  }

  uint32_t output_num_rows() {
    return total_num_rows;
  }

private:
  void maybe_finish_block() {
    if (builder.GetSize() >= MAX_BLOCK_SIZE) {
      write_encrypted_block();
    }
  }

  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<tuix::Row>> rows_vector;
  uint32_t total_num_rows;

  // For writing the resulting EncryptedBlocks
  UntrustedMemoryAllocator untrusted_alloc;
  flatbuffers::FlatBufferBuilder enc_block_builder;
  std::vector<flatbuffers::Offset<tuix::EncryptedBlock>> enc_block_vector;

  // Optionally, for writing the resulting [ShuffleOutput]s
  std::vector<flatbuffers::Offset<tuix::ShuffleOutput>> shuffle_output_vector;
};

class FlatbuffersTemporaryRow {
public:
  FlatbuffersTemporaryRow() : builder(), row(nullptr) {}
  FlatbuffersTemporaryRow(const tuix::Row *row) : FlatbuffersTemporaryRow() {
    if (row != nullptr) {
      set(row);
    }
  }

  void set(const tuix::Row *row) {
    if (row != nullptr) {
      builder.Clear();
      builder.Finish(flatbuffers_copy(row, builder));
      const uint8_t *buf = builder.GetBufferPointer();
      size_t len = builder.GetSize();
      flatbuffers::Verifier v(buf, len);
      if (!v.VerifyBuffer<tuix::Row>(nullptr)) {
        throw std::runtime_error(
          std::string("Corrupt Row buffer of length ")
          + std::to_string(len));
      }
      this->row = flatbuffers::GetRoot<tuix::Row>(buf);
    } else {
      this->row = nullptr;
    }
  }

  const tuix::Row *get() {
    return row;
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  const tuix::Row *row;
};

void print(const tuix::Row *in);
void print(const tuix::Field *field);

#endif
