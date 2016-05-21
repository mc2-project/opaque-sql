// -*- c-basic-offset: 2 -*-

#include "NewInternalTypes.h"

bool attrs_equal(const uint8_t *a, const uint8_t *b) {
  const uint8_t *a_ptr = a;
  const uint8_t *b_ptr = b;
  if (*a_ptr != *b_ptr) return false;
  a_ptr++; b_ptr++;
  uint32_t a_size = *reinterpret_cast<const uint32_t *>(a_ptr); a_ptr += 4;
  uint32_t b_size = *reinterpret_cast<const uint32_t *>(b_ptr); b_ptr += 4;
  if (a_size != b_size) return false;
  return cmp(a_ptr, b_ptr, a_size) == 0;
}

uint32_t copy_attr(uint8_t *dst, const uint8_t *src) {
  const uint8_t *src_ptr = src;
  uint8_t *dst_ptr = dst;
  *dst_ptr++ = *src_ptr++;
  uint32_t len = *reinterpret_cast<const uint32_t *>(src_ptr); src_ptr += 4;
  *(reinterpret_cast<uint32_t *>(dst_ptr)) = len; dst_ptr += 4;
  memcpy(dst_ptr, src_ptr, len); src_ptr += len; dst_ptr += len;
  return dst_ptr - dst;
}

template<>
uint32_t write_attr<uint32_t>(uint8_t *output, uint32_t value, bool dummy) {
  uint8_t *output_ptr = output;
  *output_ptr++ = dummy ? DUMMY_INT : INT;
  uint32_t len = attr_upper_bound(INT);
  *reinterpret_cast<uint32_t *>(output_ptr) = len; output_ptr += 4;
  *reinterpret_cast<uint32_t *>(output_ptr) = value; output_ptr += len;
  return output_ptr - output;
}

template<>
uint32_t write_attr<float>(uint8_t *output, float value, bool dummy) {
  uint8_t *output_ptr = output;
  *output_ptr++ = dummy ? DUMMY_FLOAT : FLOAT;
  uint32_t len = attr_upper_bound(FLOAT);
  *reinterpret_cast<uint32_t *>(output_ptr) = len; output_ptr += 4;
  *reinterpret_cast<float *>(output_ptr) = value; output_ptr += len;
  return output_ptr - output;
}

template<>
uint32_t read_attr<uint32_t>(uint8_t *input, uint8_t *value) {
  return read_attr_internal(input, value, INT);
}
template<>
uint32_t read_attr<float>(uint8_t *input, uint8_t *value) {
  return read_attr_internal(input, value, FLOAT);
}

uint32_t read_attr_internal(uint8_t *input, uint8_t *value, uint8_t expected_type) {
  uint8_t *input_ptr = input;
  uint8_t type = *input_ptr++;
  if (type != expected_type) {
    printf("read_attr expected type %d but got %d\n", expected_type, type);
    assert(false);
  }
  uint32_t len = *reinterpret_cast<uint32_t *>(input_ptr); input_ptr += 4;
  if (len != attr_upper_bound(type)) {
    printf("read_attr on type %d expected len %d but got %d\n", type, attr_upper_bound(type), len);
    assert(false);
  }
  memcpy(value, input_ptr, len); input_ptr += len;
  return input_ptr - input;
}

void NewRecord::init(uint8_t *types, uint32_t types_len) {
  uint8_t *row_ptr = this->row;
  *( (uint32_t *) row_ptr) = types_len;
  row_ptr += 4;

  for (uint32_t i = 0; i < types_len; i++) {
    uint8_t t = types[i];
    *row_ptr++ = t;
    uint32_t len = attr_upper_bound(t);
    *( (uint32_t *) row_ptr) = len; row_ptr += 4;
    row_ptr += len;
  }

  this->row_length = (row_ptr - row);
}

void NewRecord::set(NewRecord *other) {
  memcpy(this->row, other->row, other->row_length);
  this->row_length = other->row_length;
}

uint32_t NewRecord::read_plaintext(uint8_t *input) {
  uint8_t *input_ptr = input;
  uint8_t *row_ptr = this->row;

  *( (uint32_t *) row_ptr) = *( (uint32_t *) input_ptr);
  input_ptr += 4;
  row_ptr += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    uint32_t len = copy_attr(row_ptr, input_ptr);
    input_ptr += len;
    row_ptr += len;
  }

  this->row_length = (row_ptr - row);

  return (input_ptr - input);
}

uint32_t NewRecord::read_encrypted(uint8_t *input) {
  uint8_t *input_ptr = input;
  uint8_t *row_ptr = this->row;

  *( (uint32_t *) row_ptr) = *( (uint32_t *) input_ptr);
  input_ptr += 4;
  row_ptr += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    decrypt_attribute(&input_ptr, &row_ptr);
  }

  this->row_length = (row_ptr - row);

  return (input_ptr - input);
}

uint32_t NewRecord::write_encrypted(uint8_t *output) {
  uint8_t *input_ptr = this->row;
  uint8_t *output_ptr = output;

  *( (uint32_t *) (output_ptr)) = *( (uint32_t *) (input_ptr));
  input_ptr += 4;
  output_ptr += 4;

  for (uint32_t i = 0; i < this->num_cols(); i++) {
    encrypt_attribute(&input_ptr, &output_ptr);
  }

  return (output_ptr - output);
}

void NewRecord::print() {
  uint8_t *row_ptr = this->row;

  printf("NewRecord[num_attrs=%d", *( (uint32_t *) (row_ptr)));
  row_ptr += 4;

  for (uint32_t i = 0; i < this->num_cols(); i++) {
    uint8_t type = *row_ptr++;
    uint32_t len = *reinterpret_cast<uint32_t *>(row_ptr); row_ptr += 4;
    printf(", attr_%d=[type=%d, len=%d, value=", i, type, len);
    switch (type) {
    case INT: printf("%d]", *reinterpret_cast<uint32_t *>(row_ptr)); break;
    case FLOAT: printf("%f]", *reinterpret_cast<float *>(row_ptr)); break;
    case STRING:
    {
      char *str = (char *) malloc(len + 1);
      memcpy(str, row_ptr, len);
      str[len] = NULL;
      printf("%s]", str);
      free(str);
      break;
    }
    default: printf("?]");
    }
    row_ptr += len;
  }

  printf("]\n");

  check(row_length == row_ptr - row, "row length mismatch: %d != %d\n", row_length, row_ptr - row);
}

uint32_t NewRecord::write_decrypted(uint8_t *output) {
  cpy(output, this->row, this->row_length);
  return this->row_length;
}

const uint8_t *NewRecord::get_attr(uint32_t attr_idx) const {
  check(attr_idx > 0 && attr_idx <= num_cols(),
        "attr_idx %d out of bounds (%d cols)\n", attr_idx, num_cols());
  uint8_t *row_ptr = this->row;
  row_ptr += 4;
  for (uint32_t i = 0; i < attr_idx - 1; i++) {
    row_ptr++;
    uint32_t attr_len = *reinterpret_cast<uint32_t *>(row_ptr); row_ptr += 4;
    row_ptr += attr_len;
  }
  return row_ptr;
}

const uint8_t *NewRecord::get_attr_value(uint32_t attr_idx) const {
  const uint8_t *result = get_attr(attr_idx);
  result++;
  result += 4;
  return result;
}

void NewRecord::mark_dummy() {
  uint8_t *row_ptr = this->row;
  row_ptr += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    *row_ptr = get_dummy_type(*row_ptr); row_ptr++;
    uint32_t len = *reinterpret_cast<uint32_t *>(row_ptr); row_ptr += 4;
    row_ptr += len;
  }
}

bool NewRecord::is_dummy() {
  uint8_t *row_ptr = this->row;
  row_ptr += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    if (is_dummy_type(*row_ptr)) return true; row_ptr++;
    uint32_t len = *reinterpret_cast<uint32_t *>(row_ptr); row_ptr += 4;
    row_ptr += len;
  }
  return false;
}

NewProjectRecord::~NewProjectRecord() {
  delete project_attributes;
}

uint32_t NewProjectRecord::read_encrypted(uint8_t *input) {
  uint32_t result = r.read_encrypted(input);
  this->set_project_attributes();
  return result;
}

void NewProjectRecord::set_project_attributes() {
  if (project_attributes == NULL) {
    project_attributes = new ProjectAttributes(op_code, r.row + 4, r.num_cols());
    project_attributes->init();
    project_attributes->evaluate();
  } else {
    project_attributes->re_init(r.row + 4);
    project_attributes->evaluate();
  }

}

uint32_t NewProjectRecord::write_encrypted(uint8_t *output) {
  // Flushes only the eval attributes
  uint8_t *output_ptr = output;

  ProjectAttributes *attrs = this->project_attributes;

  *( (uint32_t *) (output_ptr)) = attrs->num_eval_attr;
  output_ptr += 4;

  uint8_t temp[ROW_UPPER_BOUND];
  uint8_t *temp_ptr = temp;

  for (uint32_t i = 0; i < attrs->num_eval_attr; i++) {
    temp_ptr = temp;
    attrs->eval_attributes[i]->flush(temp);
    encrypt_attribute(&temp_ptr, &output_ptr);
  }

  return (output_ptr - output);
}

uint32_t NewJoinRecord::read_encrypted(uint8_t *input) {
  decrypt(input, enc_size(JOIN_ROW_UPPER_BOUND), this->row);
  return enc_size(JOIN_ROW_UPPER_BOUND);
}

void NewJoinRecord::set(bool is_primary, NewRecord *record) {
  uint8_t *row_ptr = this->row;
  if (is_primary) {
    cpy(row_ptr, primary_id, TABLE_ID_SIZE);
  } else {
    cpy(row_ptr, foreign_id, TABLE_ID_SIZE);
  }
  row_ptr += TABLE_ID_SIZE;

  row_ptr += record->write_decrypted(row_ptr);
}

void NewJoinRecord::set(NewJoinRecord *other) {
  cpy(this->row, other->row, JOIN_ROW_UPPER_BOUND);
  this->join_attr.copy_attribute(&other->join_attr);
}

uint32_t NewJoinRecord::write_encrypted(uint8_t *output) {
  encrypt(this->row, JOIN_ROW_UPPER_BOUND, output);
  return enc_size(JOIN_ROW_UPPER_BOUND);
}

void NewJoinRecord::merge(NewJoinRecord *other, uint32_t secondary_join_attr, NewRecord *merge) {
  uint8_t *merge_ptr = merge->row;
  *( (uint32_t *) merge_ptr) = this->num_cols() + other->num_cols() - 1;
  merge_ptr += 4;

  uint8_t *input_ptr = this->row + TABLE_ID_SIZE + 4;
  uint32_t value_len;
  for (uint32_t i = 0; i < this->num_cols(); i++) {
    value_len = *( (uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    cpy(merge_ptr, input_ptr, value_len);
    merge_ptr += value_len;
    input_ptr += value_len;
  }

  input_ptr = other->row + TABLE_ID_SIZE + 4;
  for (uint32_t i = 0; i < other->num_cols(); i++) {
    value_len = *( (uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    if (i + 1 != secondary_join_attr) {
      cpy(merge_ptr, input_ptr, value_len);
      merge_ptr += value_len;
    }
    input_ptr += value_len;
  }
}

bool NewJoinRecord::is_primary() {
  return cmp(this->row, primary_id, TABLE_ID_SIZE) == 0;
}

bool NewJoinRecord::is_dummy() {
  return test_dummy(this->row, JOIN_ROW_UPPER_BOUND) == 0;
}

void NewJoinRecord::reset_to_dummy() {
  write_dummy(this->row, JOIN_ROW_UPPER_BOUND);
}

void NewJoinRecord::init_join_attribute(int op_code) {
  get_join_attribute(op_code, this->num_cols(),
                     this->row + TABLE_ID_SIZE + 4,
                     &this->join_attr);
}
