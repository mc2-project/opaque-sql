// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <algorithm>
#include <time.h>
#include <typeinfo>

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

/**
 * A read-only, typed Flatbuffers buffer, along with its length. Does not own its buffer.
 */
template<typename T> struct BufferRefView {
  BufferRefView() : buf(nullptr), len(0) {}
  BufferRefView(uint8_t *_buf, flatbuffers::uoffset_t _len)
    : buf(_buf), len(_len) {}

  const T *root() const { return flatbuffers::GetRoot<T>(buf); }

  void verify() {
    flatbuffers::Verifier verifier(buf, len);
    if (!verifier.VerifyBuffer<T>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt ")
        + std::string(typeid(T).name())
        + std::string(" buffer of length ")
        + std::to_string(len));
    }
  }

  const uint8_t *buf;
  flatbuffers::uoffset_t len;
};

/**
 * A typed Flatbuffers buffer stored in untrusted memory, along with its length. Owns its buffer.
 */
template<typename T> struct UntrustedBufferRef {
  UntrustedBufferRef() : buf(nullptr), len(0) {}
  UntrustedBufferRef(
    std::unique_ptr<uint8_t, decltype(&ocall_free)> _buf,
    flatbuffers::uoffset_t _len)
    : buf(std::move(_buf)), len(_len) {}

  const T *root() const { return flatbuffers::GetRoot<T>(buf); }

  void verify() {
    flatbuffers::Verifier verifier(buf, len);
    if (!verifier.VerifyBuffer<T>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt ")
        + std::string(typeid(T).name())
        + std::string(" buffer of length ")
        + std::to_string(len));
    }
  }

  BufferRefView<T> view() const {
    return BufferRefView<T>(buf.get(), len);
  }

  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf;
  flatbuffers::uoffset_t len;
};

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

std::string to_string(const tuix::Row *row);
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

/**
 * Container for a single row which can be accessed and reassigned.
 */
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
