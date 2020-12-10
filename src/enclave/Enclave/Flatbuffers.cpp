// -*- c-basic-offset: 2 -*-

#include "Flatbuffers.h"

std::string to_string(const tuix::Row *row) {
  std::string s;
  flatbuffers::uoffset_t num_fields = row->field_values()->size();
  s.append("[");
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    s.append(to_string(row->field_values()->Get(i)));
    if (i + 1 < num_fields) {
      s.append(",");
    }
  }
  s.append("]");
  return s;
}

std::string to_string(const Date &date) {
  uint64_t seconds_per_day = 60 * 60 * 24L;
  uint64_t secs = date.days_since_epoch * seconds_per_day;
  struct tm tm;
  secs_to_tm(secs, &tm);
  char buffer[80];
  strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm);
  return std::string(buffer);
}

std::string to_string(const tuix::Field *f) {
  if (f->is_null()) {
    return "null";
  }
  switch (f->value_type()) {
  case tuix::FieldUnion_BooleanField:
    return to_string(f->value_as_BooleanField());
  case tuix::FieldUnion_IntegerField:
    return to_string(f->value_as_IntegerField());
  case tuix::FieldUnion_LongField:
    return to_string(f->value_as_LongField());
  case tuix::FieldUnion_FloatField:
    return to_string(f->value_as_FloatField());
  case tuix::FieldUnion_DoubleField:
    return to_string(f->value_as_DoubleField());
  case tuix::FieldUnion_StringField:
    return to_string(f->value_as_StringField());
  case tuix::FieldUnion_DateField:
    return to_string(f->value_as_DateField());
  case tuix::FieldUnion_BinaryField:
    return to_string(f->value_as_BinaryField());
  case tuix::FieldUnion_ByteField:
    return to_string(f->value_as_ByteField());
  case tuix::FieldUnion_CalendarIntervalField:
    return to_string(f->value_as_CalendarIntervalField());
  case tuix::FieldUnion_NullField:
    return to_string(f->value_as_NullField());
  case tuix::FieldUnion_ShortField:
    return to_string(f->value_as_ShortField());
  case tuix::FieldUnion_TimestampField:
    return to_string(f->value_as_TimestampField());
  case tuix::FieldUnion_ArrayField:
    return to_string(f->value_as_ArrayField());
  case tuix::FieldUnion_MapField:
    return to_string(f->value_as_MapField());
  default:
    throw std::runtime_error(
      std::string("to_string(tuix::Field): Unknown field type ")
      + std::to_string(f->value_type()));
  }
}

std::string to_string(const tuix::BooleanField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::IntegerField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::LongField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::FloatField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::DoubleField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::StringField *f) {
  return std::string(f->value()->begin(), f->value()->end());
}

std::string to_string(const tuix::DateField *f) {
  return to_string(Date(f->value()));
}

std::string to_string(const tuix::BinaryField *f) {
  std::string s;
  for (uint8_t byte : *f->value()) {
    s.append(string_format("%02X", byte));
  }
  return s;
}

std::string to_string(const tuix::ByteField *f) {
  std::string s;
  s.append(string_format("%02X", f->value()));
  return s;
}

std::string to_string(const tuix::CalendarIntervalField *f) {
  (void)f;
  throw std::runtime_error("Can't convert CalendarIntervalField to string");
}

std::string to_string(const tuix::NullField *f) {
  (void)f;
  return "null";
}

std::string to_string(const tuix::ShortField *f) {
  return std::to_string(f->value());
}

std::string to_string(const tuix::TimestampField *f) {
  // TODO: Support fractional seconds to mimic the behavior of java.sql.Timestamp#toString
  uint64_t secs = f->value() / 1000;
  struct tm tm;
  secs_to_tm(secs, &tm);
  char buffer[80];
  strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
  return std::string(buffer);
}

std::string to_string(const tuix::ArrayField *f) {
  std::string s;
  s.append("[");
  bool first = true;
  for (auto field : *f->value()) {
    if (!first) {
      s.append(", ");
    }
    first = false;
    s.append(to_string(field));
  }
  s.append("]");
  return s;
}

std::string to_string(const tuix::MapField *f) {
  std::string s;
  s.append("{");
  for (uint32_t i = 0; i < f->keys()->size(); ++i) {
    if (i != 0) {
      s.append(", ");
    }

    s.append(to_string(f->keys()->Get(i)));
    s.append(": ");
    s.append(to_string(f->values()->Get(i)));
  }
  s.append("}");
  return s;
}

void print(const tuix::Row *in) {
  flatbuffers::uoffset_t num_fields = in->field_values()->size();
  ocall_print_string("[");
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    print(in->field_values()->Get(i));
    if (i + 1 < num_fields) {
      ocall_print_string(",");
    }
  }
  ocall_print_string("]\n");
}

void print(const tuix::Field *field) {
  ocall_print_string(to_string(field).c_str());
}

template<>
flatbuffers::Offset<tuix::Row> flatbuffers_copy(
  const tuix::Row *row, flatbuffers::FlatBufferBuilder& builder, bool force_null) {

  flatbuffers::uoffset_t num_fields = row->field_values()->size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    field_values[i] = flatbuffers_copy<tuix::Field>(
      row->field_values()->Get(i), builder, force_null);
  }
  return tuix::CreateRowDirect(builder, &field_values);
}

template<>
flatbuffers::Offset<tuix::Field> flatbuffers_copy(
  const tuix::Field *field, flatbuffers::FlatBufferBuilder& builder, bool force_null) {

  bool is_null = force_null || field->is_null();
  switch (field->value_type()) {
  case tuix::FieldUnion_BooleanField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_BooleanField,
      tuix::CreateBooleanField(
        builder,
        static_cast<const tuix::BooleanField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_IntegerField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_IntegerField,
      tuix::CreateIntegerField(
        builder,
        static_cast<const tuix::IntegerField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_LongField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_LongField,
      tuix::CreateLongField(
        builder,
        static_cast<const tuix::LongField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_FloatField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_FloatField,
      tuix::CreateFloatField(
        builder,
        static_cast<const tuix::FloatField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_DoubleField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_DoubleField,
      tuix::CreateDoubleField(
        builder,
        static_cast<const tuix::DoubleField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_StringField:
  {
    auto string_field = static_cast<const tuix::StringField *>(field->value());
    std::vector<uint8_t> string_data(string_field->value()->begin(),
                                     string_field->value()->end());
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_StringField,
      tuix::CreateStringFieldDirect(
        builder, &string_data, string_field->length()).Union(),
      is_null);
  }
  case tuix::FieldUnion_DateField:
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_DateField,
      tuix::CreateDateField(
        builder,
        static_cast<const tuix::DateField *>(field->value())->value()).Union(),
      is_null);
  case tuix::FieldUnion_CalendarIntervalField:
  {
    auto cif = static_cast<const tuix::CalendarIntervalField *>(field->value());
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_CalendarIntervalField,
      tuix::CreateCalendarIntervalField(
        builder,
        cif->months(),
        cif->microseconds()).Union(),
      is_null);
  }
  case tuix::FieldUnion_ArrayField:
  {
    auto array_field = static_cast<const tuix::ArrayField *>(field->value());
    std::vector<flatbuffers::Offset<tuix::Field>> copied_fields;
    for (auto f : *array_field->value()) {
      copied_fields.push_back(flatbuffers_copy(f, builder, false));
    }
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_ArrayField,
      tuix::CreateArrayFieldDirect(
        builder, &copied_fields).Union(),
      is_null);
  }
  case tuix::FieldUnion_MapField:
  {
    auto map_field = static_cast<const tuix::MapField *>(field->value());
    std::vector<flatbuffers::Offset<tuix::Field>> copied_key_fields;
    std::vector<flatbuffers::Offset<tuix::Field>> copied_value_fields;
    for (auto f : *map_field->keys()) {
      copied_key_fields.push_back(flatbuffers_copy(f, builder, false));
    }
    for (auto f : *map_field->values()) {
      copied_value_fields.push_back(flatbuffers_copy(f, builder, false));
    }
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_MapField,
      tuix::CreateMapFieldDirect(
        builder, &copied_key_fields, &copied_value_fields).Union(),
      is_null);
  }
  default:
    throw std::runtime_error(
      std::string("flatbuffers_copy tuix::Field: Unknown field type ")
                  + std::to_string(field->value_type()));
  }
}
