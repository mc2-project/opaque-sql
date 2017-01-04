// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <algorithm>

#include "EncryptedBlock_generated.h"
#include "Expr_generated.h"
#include "Rows_generated.h"
#include "operators_generated.h"

#include "Crypto.h"
#include "common.h"

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
  const T *flatbuffers_obj, flatbuffers::FlatBufferBuilder& builder);
template<>
flatbuffers::Offset<tuix::Row> flatbuffers_copy(
  const tuix::Row *row, flatbuffers::FlatBufferBuilder& builder);
template<>
flatbuffers::Offset<tuix::Field> flatbuffers_copy(
  const tuix::Field *field, flatbuffers::FlatBufferBuilder& builder);

class FlatbuffersEncryptedBlockReader {
public:
  FlatbuffersEncryptedBlockReader(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v1(buf, len);
    check(v1.VerifyBuffer<tuix::EncryptedBlocks>(nullptr),
          "Corrupt EncryptedBlocks %p of length %d\n", buf, len);
    encrypted_blocks = flatbuffers::GetRoot<tuix::EncryptedBlocks>(buf);
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

class FlatbuffersRowReader {
public:
  FlatbuffersRowReader(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v1(buf, len);
    check(v1.VerifyBuffer<tuix::EncryptedBlock>(nullptr),
          "Corrupt EncryptedBlock %p of length %d\n", buf, len);
    auto encrypted_block = flatbuffers::GetRoot<tuix::EncryptedBlock>(buf);
    init(encrypted_block);
  }

  FlatbuffersRowReader(const tuix::EncryptedBlock *encrypted_block) {
    init(encrypted_block);
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
    flatbuffers::Verifier v2(rows_buf.get(), rows_len);
    check(v2.VerifyBuffer<tuix::Rows>(nullptr),
          "Corrupt Rows %p of length %d\n", rows_buf.get(), rows_len);

    rows = flatbuffers::GetRoot<tuix::Rows>(rows_buf.get());
    check(rows->rows()->size() == num_rows,
          "EncryptedBlock claimed to contain %d rows but actually contains %d rows\n",
          num_rows == rows->rows()->size());
  }

  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
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
    : builder(), rows_vector(), untrusted_alloc() {
  }

  /** Copy the given Row to the output. */
  void write(const tuix::Row *row) {
    rows_vector.push_back(flatbuffers_copy(row, builder));
  }

  /** Copy the given Fields to the output. */
  void write(const std::vector<const tuix::Field *> &row_fields) {
    flatbuffers::uoffset_t num_fields = row_fields.size();
    std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
    for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
      field_values[i] = flatbuffers_copy<tuix::Field>(row_fields[i], builder);
    }
    rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  }

  void close() {
    builder.Finish(tuix::CreateRowsDirect(builder, &rows_vector));
    size_t enc_rows_len = enc_size(builder.GetSize());
    uint8_t *enc_rows = nullptr;
    ocall_malloc(enc_rows_len, &enc_rows);
    encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows);

    enc_block_builder.reset(
      new flatbuffers::FlatBufferBuilder(enc_rows_len * 2, &untrusted_alloc));
    enc_block_builder->Finish(
      tuix::CreateEncryptedBlock(
        *enc_block_builder,
        rows_vector.size(),
        enc_block_builder->CreateVector(enc_rows, enc_rows_len)));

    ocall_free(enc_rows);
  }

  uint8_t *output_buffer() {
    uint8_t *buf = nullptr;
    ocall_malloc(output_size(), &buf);
    memcpy(buf, enc_block_builder->GetBufferPointer(), output_size());
    return buf;
  }

  size_t output_size() {
    return enc_block_builder->GetSize();
  }

  uint32_t output_num_rows() {
    return rows_vector.size();
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<tuix::Row>> rows_vector;

  // For writing the resulting EncryptedBlock
  UntrustedMemoryAllocator untrusted_alloc;
  std::unique_ptr<flatbuffers::FlatBufferBuilder> enc_block_builder;
};

void print(const tuix::Row *in);

#endif
