// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <algorithm>

#include "EncryptedBlock_generated.h"
#include "Expr_generated.h"
#include "Rows_generated.h"
#include "operators_generated.h"

#include "Crypto.h"
#include "common.h"
#include "Enclave_t.h"

int printf(const char *fmt, ...);

#define check(test, ...) do {                   \
    bool result = test;                         \
    if (!result) {                              \
      printf(__VA_ARGS__);                      \
      assert(result);                           \
    }                                           \
  } while (0)

#ifndef FLATBUFFERS_H
#define FLATBUFFERS_H

using namespace edu::berkeley::cs::rise::opaque;

template<typename T>
flatbuffers::Offset<T> flatbuffers_copy(
  const T *flatbuffers_obj, flatbuffers::FlatBufferBuilder& builder, bool force_null = false);
template<>
flatbuffers::Offset<tuix::Row> flatbuffers_copy(
  const tuix::Row *row, flatbuffers::FlatBufferBuilder& builder, bool force_null);
template<>
flatbuffers::Offset<tuix::Field> flatbuffers_copy(
  const tuix::Field *field, flatbuffers::FlatBufferBuilder& builder, bool force_null);

template<typename T> flatbuffers::Offset<T> GetOffset(
  flatbuffers::FlatBufferBuilder &builder, const T *pointer) {
  return flatbuffers::Offset<T>(builder.GetCurrentBufferPointer() + builder.GetSize()
                                - reinterpret_cast<const uint8_t *>(pointer));
}

class EncryptedBlocksToEncryptedBlockReader {
public:
  EncryptedBlocksToEncryptedBlockReader(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::EncryptedBlocks>(nullptr),
          "Corrupt EncryptedBlocks %p of length %d\n", buf, len);
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
  EncryptedBlockToRowReader() {}

  void reset(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::EncryptedBlock>(nullptr),
          "Corrupt EncryptedBlock %p of length %d\n", buf, len);
    auto encrypted_block = flatbuffers::GetRoot<tuix::EncryptedBlock>(buf);
    init(encrypted_block);
  }

  void reset(const tuix::EncryptedBlock *encrypted_block) {
    init(encrypted_block);
  }

  bool has_next() {
    printf("EncryptedBlockToRowReader::has_next: row_idx=%d, num rows=%d\n",
           row_idx, rows->rows()->size());
    return row_idx < rows->rows()->size();
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
    printf("Decrypted %d rows, plaintext is %d bytes\n", num_rows, rows_len);
    flatbuffers::Verifier v(rows_buf.get(), rows_len);
    check(v.VerifyBuffer<tuix::Rows>(nullptr),
          "Corrupt Rows %p of length %d\n", rows_buf.get(), rows_len);

    rows = flatbuffers::GetRoot<tuix::Rows>(rows_buf.get());
    check(rows->rows()->size() == num_rows,
          "EncryptedBlock claimed to contain %d rows but actually contains %d rows\n",
          num_rows == rows->rows()->size());

    row_idx = 0;
  }

  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
  uint32_t row_idx;
};

class EncryptedBlocksToRowReader {
  typedef flatbuffers::Vector<
    flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator EncryptedBlockIterator;
  typedef flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator RowIterator;

public:
  EncryptedBlocksToRowReader(uint8_t *buf, size_t len)
    : block_idx(0) {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::EncryptedBlocks>(nullptr),
          "Corrupt EncryptedBlocks %p of length %d\n", buf, len);
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
    printf("Next from EncryptedBlocksToRowReader. On block %d of %d\n",
           block_idx, encrypted_blocks->blocks()->size());
    if (!r.has_next()) {
      printf("Calling init_row_reader from next()\n");
      assert(block_idx + 1 < encrypted_blocks->blocks()->size());
      block_idx++;
      init_row_reader();
    }

    return r.next();
  }

private:
  void init_row_reader() {
    printf("init_row_reader: block_idx=%d, num blocks=%d\n",
           block_idx, encrypted_blocks->blocks()->size());
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
    if (this->buf != nullptr) {
      ocall_free(this->buf);
    }
    this->buf = buf;

    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::SortedRuns>(nullptr),
          "Corrupt SortedRuns %p of length %d\n", buf, len);
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


class UntrustedMemoryAllocator : public flatbuffers::simple_allocator {
public:
  virtual uint8_t *allocate(size_t size) const {
    uint8_t *result = nullptr;
    ocall_malloc(size, &result);
    return result;
  }
  virtual void deallocate(uint8_t *p) const {
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
    uint8_t *enc_rows = nullptr;
    ocall_malloc(enc_rows_len, &enc_rows);
    encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows);

    enc_block_vector.push_back(
      tuix::CreateEncryptedBlock(
        enc_block_builder,
        rows_vector.size(),
        enc_block_builder.CreateVector(enc_rows, enc_rows_len)));

    ocall_free(enc_rows);

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

  template<typename T>
  void finish(flatbuffers::Offset<T> root) {
    enc_block_builder.Finish<T>(root);
  }

  uint8_t *output_buffer() {
    uint8_t *buf = nullptr;
    ocall_malloc(output_size(), &buf);
    memcpy(buf, enc_block_builder.GetBufferPointer(), output_size());
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
};

void print(const tuix::Row *in);
void print(const tuix::Field *field);

#endif
