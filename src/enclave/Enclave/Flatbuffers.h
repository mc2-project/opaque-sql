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

class FlatbuffersRowReader {
public:
  FlatbuffersRowReader(uint8_t *buf, size_t len)
    : rows_read(0) {
    flatbuffers::Verifier v1(buf, len);
    check(v1.VerifyBuffer<tuix::EncryptedBlock>(nullptr),
          "Corrupt EncryptedBlock %p of length %d\n", buf, len);
    auto encrypted_block = flatbuffers::GetRoot<tuix::EncryptedBlock>(buf);
    num_rows = encrypted_block->num_rows();

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

  const tuix::Row *next() {
    return rows->rows()->Get(rows_read++);
  }

private:
  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
  uint32_t rows_read;
  uint32_t num_rows;
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

class FlatbuffersExpressionEvaluator {
public:
  FlatbuffersExpressionEvaluator(const tuix::Expr *expr) : builder(), expr(expr) {}

  /**
   * Evaluate the stored expression on the given row. Return a Field containing the result.
   * Warning: The Field points to internally-managed memory that may be overwritten the next time
   * eval is called. Therefore it is only valid until the next call to eval.
   */
  const tuix::Field *eval(const tuix::Row *row) {
    builder.Clear();
    flatbuffers::Offset<tuix::Field> result_offset = eval_helper(row, expr);
    return flatbuffers::GetTemporaryPointer<tuix::Field>(builder, result_offset);
  }

private:
  /**
   * Evaluate the given expression on the given row. Return the offset (within builder) of the Field
   * containing the result. This offset is only valid until the next call to eval.
   */
  flatbuffers::Offset<tuix::Field> eval_helper(const tuix::Row *row, const tuix::Expr *expr) {
    switch (expr->expr_type()) {
    case tuix::ExprUnion_Col:
    {
      uint32_t col_num = static_cast<const tuix::Col *>(expr->expr())->col_num();
      return flatbuffers_copy<tuix::Field>(
        row->field_values()->Get(col_num), builder);
    }
    case tuix::ExprUnion_Literal:
      return flatbuffers_copy<tuix::Field>(
        static_cast<const tuix::Literal *>(expr->expr())->value(), builder);
    case tuix::ExprUnion_GreaterThan:
    {
      auto gt = static_cast<const tuix::GreaterThan *>(expr->expr());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->left()));
      const tuix::Field *right =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->right()));
      check(left->value_type() == right->value_type(),
            "tuix::GreaterThan can't compare values of different types (%s and %s)\n",
            tuix::EnumNameFieldUnion(left->value_type()),
            tuix::EnumNameFieldUnion(right->value_type()));
      bool result_is_null = left->is_null() || right->is_null();
      bool result = false;
      if (!result_is_null) {
        switch (left->value_type()) {
        case tuix::FieldUnion_IntegerField:
          result = static_cast<const tuix::IntegerField *>(left->value())->value()
            > static_cast<const tuix::IntegerField *>(right->value())->value();
          break;
        case tuix::FieldUnion_LongField:
          result = static_cast<const tuix::LongField *>(left->value())->value()
            > static_cast<const tuix::LongField *>(right->value())->value();
          break;
        case tuix::FieldUnion_FloatField:
          result = static_cast<const tuix::FloatField *>(left->value())->value()
            > static_cast<const tuix::FloatField *>(right->value())->value();
          break;
        case tuix::FieldUnion_DoubleField:
          result = static_cast<const tuix::DoubleField *>(left->value())->value()
            > static_cast<const tuix::DoubleField *>(right->value())->value();
          break;
        default:
          printf("Can't evaluate tuix::GreaterThan on %s\n",
                 tuix::EnumNameFieldUnion(left->value_type()));
          assert(false);
        }
      }
      // Writing the result invalidates the left and right temporary pointers
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        result_is_null);
    }
    case tuix::ExprUnion_Substring:
    {
      auto ss = static_cast<const tuix::Substring *>(expr->expr());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *str =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->str()));
      const tuix::Field *pos =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->pos()));
      const tuix::Field *len =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->len()));
      check(str->value_type() == tuix::FieldUnion_StringField &&
            pos->value_type() == tuix::FieldUnion_IntegerField &&
            len->value_type() == tuix::FieldUnion_IntegerField,
            "tuix::Substring requires str String, pos Integer, len Integer, not "
            "str %s, pos %s, len %s)\n",
            tuix::EnumNameFieldUnion(str->value_type()),
            tuix::EnumNameFieldUnion(pos->value_type()),
            tuix::EnumNameFieldUnion(len->value_type()));
      bool result_is_null = str->is_null() || pos->is_null() || len->is_null();
      if (!result_is_null) {
        auto str_field = static_cast<const tuix::StringField *>(str->value());
        auto pos_val = static_cast<const tuix::IntegerField *>(pos->value())->value();
        auto len_val = static_cast<const tuix::IntegerField *>(len->value())->value();
        auto start_idx = std::min(static_cast<uint32_t>(pos_val), str_field->length());
        auto end_idx = std::min(start_idx + len_val, str_field->length());
        // TODO: oblivious string lengths
        std::vector<uint8_t> substring(
          flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                        start_idx),
          flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                        end_idx));
        // Writing the result invalidates the str, pos, len temporary pointers
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &substring, end_idx - start_idx).Union(),
          result_is_null);
      } else {
        // Writing the result invalidates the str, pos, len temporary pointers
        // TODO: oblivious string lengths
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, nullptr, 0).Union(),
          result_is_null);
      }
    }
    default:
      printf("Can't evaluate expression of type %s\n",
             tuix::EnumNameExprUnion(expr->expr_type()));
      assert(false);
      return flatbuffers::Offset<tuix::Field>();
    }
  }

  flatbuffers::FlatBufferBuilder builder;
  const tuix::Expr *expr;
};

#endif
