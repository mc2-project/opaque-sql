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

bool attr_less_than(const uint8_t *a, const uint8_t *b) {
  const uint8_t *a_ptr = a;
  const uint8_t *b_ptr = b;

  check(*a_ptr == *b_ptr,
        "attr_less_than: Can't compare different types %d and %d\n", *a_ptr, *b_ptr);
  uint8_t type = *a_ptr; a_ptr++; b_ptr++;

  uint32_t a_len = *reinterpret_cast<const uint32_t *>(a_ptr); a_ptr += 4;
  uint32_t b_len = *reinterpret_cast<const uint32_t *>(b_ptr); b_ptr += 4;
  switch (type) {
  case INT:
  {
    uint32_t a_val = *reinterpret_cast<const uint32_t *>(a_ptr); a_ptr += 4;
    uint32_t b_val = *reinterpret_cast<const uint32_t *>(b_ptr); b_ptr += 4;
    return a_val < b_val;
  }

  case FLOAT:
  {
    uint32_t a_val = *reinterpret_cast<const float *>(a_ptr); a_ptr += 4;
    uint32_t b_val = *reinterpret_cast<const float *>(b_ptr); b_ptr += 4;
    return a_val < b_val;
  }

  case LONG:
  case DATE:
  {
    uint32_t a_val = *reinterpret_cast<const uint64_t *>(a_ptr); a_ptr += 8;
    uint32_t b_val = *reinterpret_cast<const uint64_t *>(b_ptr); b_ptr += 8;
    return a_val < b_val;
  }

  case STRING:
  case URL_TYPE:
  case C_CODE:
  case L_CODE:
  case IP_TYPE:
  case USER_AGENT_TYPE:
  case SEARCH_WORD_TYPE:
  {
    uint32_t min_len = a_len < b_len ? a_len : b_len;
    for (uint32_t i = 0; i < min_len; i++) {
      if (*a_ptr < *b_ptr) {
        return true;
      } else if (*a_ptr > *b_ptr) {
        return false;
      }
      a_ptr++;
      b_ptr++;
    }

    if (a_len < b_len) {
      return true;
    } else {
      return false;
    }
  }

  default:
    printf("attr_less_than: Unknown type %d\n", type);
    assert(false);
  }
  return false;
}

uint32_t attr_key_prefix(const uint8_t *attr) {
  const uint8_t *attr_ptr = attr;
  uint8_t type = *attr_ptr; attr_ptr++;

  uint32_t attr_len = *reinterpret_cast<const uint32_t *>(attr_ptr); attr_ptr += 4;
  switch (type) {
  case INT:
  case LONG:
  case DATE:
    return *reinterpret_cast<const uint32_t *>(attr_ptr);

  case FLOAT:
  {
    // Transform any IEEE float into an unsigned integer that can be sorted using integer comparison
    // From http://stereopsis.com/radix.html
    uint32_t bits = *reinterpret_cast<const uint32_t *>(attr_ptr);
    return bits ^ (-static_cast<int32_t>(bits >> 31) | 0x80000000);
  }

  case STRING:
  case URL_TYPE:
  case C_CODE:
  case L_CODE:
  case IP_TYPE:
  case USER_AGENT_TYPE:
  case SEARCH_WORD_TYPE:
  {
    // Copy up to the first 4 bytes of the string into an integer, zero-padding for strings shorter
    // than 4 bytes
    uint32_t bits = 0;
    uint32_t cpy_len = attr_len < 4 ? attr_len : 4;
    // Need to ensure big-endian byte order so integer comparison is equivalent to lexicographic
    // string comparison
    for (uint32_t i = 0; i < cpy_len; i++) {
      bits |= static_cast<uint32_t>(attr_ptr[i]) << (24 - 8 * i);
    }
    return bits;
  }

  default:
    printf("attr_key_prefix: Unknown type %d\n", type);
    assert(false);
  }
  return 0;
}

void NewRecord::clear() {
  *reinterpret_cast<uint32_t *>(row) = 0;
  row_length = 4;
}

void NewRecord::init(const uint8_t *types, uint32_t types_len) {
  uint8_t *row_ptr = this->row;
  *( (uint32_t *) row_ptr) = types_len;
  row_ptr += 4;

  for (uint32_t i = 0; i < types_len; i++) {
    uint8_t t = types[i];
    *row_ptr = t; row_ptr++;
    uint32_t len = 0;
    *( (uint32_t *) row_ptr) = len; row_ptr += 4;
    row_ptr += len;
  }

  this->row_length = (row_ptr - row);
}

void NewRecord::set(const NewRecord *other) {
  memcpy(this->row, other->row, other->row_length);
  this->row_length = other->row_length;
}

void NewRecord::append(const NewRecord *other) {
  memcpy(row + row_length, other->row + 4, other->row_length - 4);
  this->row_length += other->row_length - 4;
  set_num_cols(num_cols() + other->num_cols());
}

uint32_t NewRecord::read(const uint8_t *input) {
  const uint8_t *input_ptr = input;
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

void NewRecord::print() const {
  const uint8_t *row_ptr = this->row;

  printf("NewRecord[num_attrs=%d", *( (const uint32_t *) (row_ptr)));
  row_ptr += 4;

  for (uint32_t i = 0; i < this->num_cols(); i++) {
    uint8_t type = *row_ptr++;
    uint32_t len = *reinterpret_cast<const uint32_t *>(row_ptr); row_ptr += 4;
    printf(", attr_%d=[type=%d, len=%d, value=", i, type, len);
    switch (type) {
    case INT: printf("%d]", *reinterpret_cast<const uint32_t *>(row_ptr)); break;

    case FLOAT: printf("%f]", *reinterpret_cast<const float *>(row_ptr)); break;

    case STRING:
    case URL_TYPE:
    case C_CODE:
    case L_CODE:
    case IP_TYPE:
    case USER_AGENT_TYPE:
    case SEARCH_WORD_TYPE:
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

uint32_t NewRecord::write(uint8_t *output) const {
  memcpy(output, this->row, this->row_length);
  return this->row_length;
}

bool NewRecord::less_than(const NewRecord *other, int op_code) const {
  switch (op_code) {
  case OP_SORT_COL1:
    return attr_less_than(get_attr(1), other->get_attr(1));
  case OP_SORT_COL2:
    return attr_less_than(get_attr(2), other->get_attr(2));
  case OP_SORT_COL2_IS_DUMMY_COL1:
  {
    bool this_is_dummy = is_dummy_type(get_attr_type(2));
    bool other_is_dummy = is_dummy_type(other->get_attr_type(2));
    if (this_is_dummy != other_is_dummy) {
      return other_is_dummy;
    } else {
      return attr_less_than(get_attr(1), other->get_attr(1));
    }
  }
  case OP_SORT_COL3_IS_DUMMY_COL1:
  {
    bool this_is_dummy = is_dummy_type(get_attr_type(3));
    bool other_is_dummy = is_dummy_type(other->get_attr_type(3));
    if (this_is_dummy != other_is_dummy) {
      return other_is_dummy;
    } else {
      return attr_less_than(get_attr(1), other->get_attr(1));
    }
  }
  case OP_SORT_COL4_IS_DUMMY_COL2:
  {
    bool this_is_dummy = is_dummy_type(get_attr_type(4));
    bool other_is_dummy = is_dummy_type(other->get_attr_type(4));
    if (this_is_dummy != other_is_dummy) {
      return other_is_dummy;
    } else {
      return attr_less_than(get_attr(2), other->get_attr(2));
    }
  }
  default:
    printf("NewRecord::less_than: Unknown opcode %d\n", op_code);
    assert(false);
  }
  return false;
}

uint32_t NewRecord::get_key_prefix(int op_code) const {
  switch (op_code) {
  case OP_SORT_COL1:
    return attr_key_prefix(get_attr(1));
  case OP_SORT_COL2:
    return attr_key_prefix(get_attr(2));
  case OP_SORT_COL2_IS_DUMMY_COL1:
  {
    uint32_t dummy_bit = is_dummy_type(get_attr_type(2)) ? 1 : 0;
    uint32_t attr_prefix = attr_key_prefix(get_attr(1));
    return (dummy_bit << 31) | (attr_prefix >> 1);
  }
  case OP_SORT_COL3_IS_DUMMY_COL1:
  {
    uint32_t dummy_bit = is_dummy_type(get_attr_type(3)) ? 1 : 0;
    uint32_t attr_prefix = attr_key_prefix(get_attr(1));
    return (dummy_bit << 31) | (attr_prefix >> 1);
  }
  case OP_SORT_COL4_IS_DUMMY_COL2:
  {
    uint32_t dummy_bit = is_dummy_type(get_attr_type(4)) ? 1 : 0;
    uint32_t attr_prefix = attr_key_prefix(get_attr(2));
    return (dummy_bit << 31) | (attr_prefix >> 1);
  }
  default:
    printf("NewRecord::get_key_prefix: Unknown opcode %d\n", op_code);
    assert(false);
  }
  return 0;
}

uint32_t NewRecord::row_upper_bound() const {
  uint32_t result = 0;

  const uint8_t *row_ptr = row;
  row_ptr += 4;
  result += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    uint8_t type = *row_ptr; row_ptr++; result++;
    uint32_t len = *reinterpret_cast<const uint32_t *>(row_ptr); row_ptr += 4; result += 4;
    row_ptr += len;
    result += attr_upper_bound(type);
  }

  return result;
}

uint8_t *get_attr_internal(uint8_t *row, uint32_t attr_idx, uint32_t num_cols) {
  check(attr_idx > 0 && attr_idx <= num_cols,
        "attr_idx %d out of bounds (%d cols)\n", attr_idx, num_cols);
  uint8_t *row_ptr = row;
  row_ptr += 4;
  for (uint32_t i = 0; i < attr_idx - 1; i++) {
    row_ptr++;
    uint32_t attr_len = *reinterpret_cast<uint32_t *>(row_ptr); row_ptr += 4;
    row_ptr += attr_len;
  }
  return row_ptr;
}

const uint8_t *NewRecord::get_attr(uint32_t attr_idx) const {
  return get_attr_internal(row, attr_idx, num_cols());
}

uint8_t NewRecord::get_attr_type(uint32_t attr_idx) const {
  const uint8_t *attr_ptr = get_attr(attr_idx);
  return *attr_ptr;
}

uint32_t NewRecord::get_attr_len(uint32_t attr_idx) const {
  const uint8_t *attr_ptr = get_attr(attr_idx);
  attr_ptr++;
  return *reinterpret_cast<const uint32_t *>(attr_ptr);
}

void NewRecord::set_attr_len(uint32_t attr_idx, uint32_t new_attr_len) {
  uint8_t *attr_start = get_attr_internal(row, attr_idx, num_cols());
  uint8_t *attr_ptr = attr_start;
  attr_ptr++;
  uint32_t old_attr_len = *reinterpret_cast<uint32_t *>(attr_ptr);
  *reinterpret_cast<uint32_t *>(attr_ptr) = new_attr_len; attr_ptr += 4;
  uint8_t *old_attr_end = attr_ptr + old_attr_len;
  uint8_t *new_attr_end = attr_ptr + new_attr_len;
  uint32_t new_row_len = row_length - old_attr_len + new_attr_len;
  uint32_t row_remaining_len = row + row_length - old_attr_end;
  // Move the rest of the row into place
  memmove(new_attr_end, old_attr_end, row_remaining_len);
  row_length = new_row_len;
}

const uint8_t *NewRecord::get_attr_value(uint32_t attr_idx) const {
  const uint8_t *result = get_attr(attr_idx);
  result++;
  result += 4;
  return result;
}

void NewRecord::set_attr_value(uint32_t attr_idx, const uint8_t *new_attr_value) {
  uint32_t attr_len = get_attr_len(attr_idx);
  uint8_t *attr_ptr = get_attr_internal(row, attr_idx, num_cols());
  attr_ptr++;
  attr_ptr += 4;
  memcpy(attr_ptr, new_attr_value, attr_len);
}

void NewRecord::add_attr(const NewRecord *other, uint32_t attr_idx) {
  row_length += copy_attr(row + row_length, other->get_attr(attr_idx));
  set_num_cols(num_cols() + 1);
}

void NewRecord::add_attr(uint8_t type, uint32_t len, const uint8_t *value) {
  uint8_t *row_ptr = row + row_length;
  *row_ptr = type; row_ptr++;
  *reinterpret_cast<uint32_t *>(row_ptr) = len; row_ptr += 4;
  memcpy(row_ptr, value, len); row_ptr += len;
  row_length += row_ptr - (row + row_length);
  set_num_cols(num_cols() + 1);
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

bool NewRecord::is_dummy() const {
  const uint8_t *row_ptr = this->row;
  row_ptr += 4;

  for (uint32_t i = 0; i < num_cols(); i++) {
    if (is_dummy_type(*row_ptr)) return true; row_ptr++;
    uint32_t len = *reinterpret_cast<const uint32_t *>(row_ptr); row_ptr += 4;
    row_ptr += len;
  }
  return false;
}

uint32_t NewJoinRecord::read(uint8_t *input) {
  memcpy(this->row, input, JOIN_ROW_UPPER_BOUND);
  return JOIN_ROW_UPPER_BOUND;
}

uint32_t NewJoinRecord::read_encrypted(uint8_t *input) {
  decrypt(input, enc_size(JOIN_ROW_UPPER_BOUND), this->row);
  return enc_size(JOIN_ROW_UPPER_BOUND);
}

uint32_t NewJoinRecord::write(uint8_t *output) {
  memcpy(output, this->row, JOIN_ROW_UPPER_BOUND);
  return JOIN_ROW_UPPER_BOUND;
}

uint32_t NewJoinRecord::write_encrypted(uint8_t *output) {
  encrypt(this->row, JOIN_ROW_UPPER_BOUND, output);
  return enc_size(JOIN_ROW_UPPER_BOUND);
}

void NewJoinRecord::set(bool is_primary, const NewRecord *record) {
  uint8_t *row_ptr = this->row;
  if (is_primary) {
    memcpy(row_ptr, primary_id, TABLE_ID_SIZE);
  } else {
    memcpy(row_ptr, foreign_id, TABLE_ID_SIZE);
  }
  row_ptr += TABLE_ID_SIZE;

  row_ptr += record->write(row_ptr);
}

void NewJoinRecord::set(NewJoinRecord *other) {
  memcpy(this->row, other->row, JOIN_ROW_UPPER_BOUND);
  if (other->join_attr != NULL) {
    this->join_attr = this->row + (other->join_attr - other->row);
  } else {
    this->join_attr = NULL;
  }
}

bool NewJoinRecord::less_than(const NewJoinRecord *other, int op_code) const {
  switch (op_code) {
  case OP_JOIN_COL1:
  case OP_JOIN_PAGERANK:
  {
    if (attrs_equal(get_attr(1), other->get_attr(1))) {
      // Join attributes are equal; sort primary rows before foreign rows
      return is_primary() && !other->is_primary();
    } else {
      return attr_less_than(get_attr(1), other->get_attr(1));
    }
  }
  case OP_JOIN_COL2:
  {
    if (attrs_equal(get_attr(2), other->get_attr(2))) {
      // Join attributes are equal; sort primary rows before foreign rows
      return is_primary() && !other->is_primary();
    } else {
      return attr_less_than(get_attr(2), other->get_attr(2));
    }
  }
  default:
    printf("NewJoinRecord::less_than: Unknown opcode %d\n", op_code);
    assert(false);
  }
  return false;
}

uint32_t NewJoinRecord::get_key_prefix(int op_code) const {
  switch (op_code) {
  case OP_JOIN_COL1:
  case OP_JOIN_PAGERANK:
    return attr_key_prefix(get_attr(1));
  case OP_JOIN_COL2:
    return attr_key_prefix(get_attr(2));
  default:
    printf("NewJoinRecord::get_key_prefix: Unknown opcode %d\n", op_code);
    assert(false);
  }
  return 0;
}

uint32_t NewJoinRecord::row_upper_bound() const {
  return JOIN_ROW_UPPER_BOUND;
}

void NewJoinRecord::merge(
  const NewJoinRecord *other, uint32_t secondary_join_attr, NewRecord *merge) const {

  uint8_t *merge_ptr = merge->row;
  *( (uint32_t *) merge_ptr) = this->num_cols() + other->num_cols() - 1;
  merge_ptr += 4;

  const uint8_t *input_ptr = this->row + TABLE_ID_SIZE + 4;
  uint32_t value_len;
  for (uint32_t i = 0; i < this->num_cols(); i++) {
    value_len = *( (const uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    memcpy(merge_ptr, input_ptr, value_len);
    merge_ptr += value_len;
    input_ptr += value_len;
  }

  input_ptr = other->row + TABLE_ID_SIZE + 4;
  for (uint32_t i = 0; i < other->num_cols(); i++) {
    value_len = *( (const uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    if (i + 1 != secondary_join_attr) {
      memcpy(merge_ptr, input_ptr, value_len);
      merge_ptr += value_len;
    }
    input_ptr += value_len;
  }

  merge->row_length = merge_ptr - merge->row;
}

void NewJoinRecord::init_join_attribute(int op_code) {
  uint32_t join_attr_idx = 0;
  switch (op_code) {
  case OP_JOIN_COL1:
  case OP_JOIN_PAGERANK:
    join_attr_idx = 1;
    break;
  case OP_JOIN_COL2:
    join_attr_idx = 2;
    break;
  default:
    printf("NewJoinRecord::init_join_attribute: Unknown opcode %d\n", op_code);
    assert(false);
  }
  join_attr = get_attr(join_attr_idx);
}

bool NewJoinRecord::join_attr_equals(const NewJoinRecord *other) const {
  if (join_attr != NULL && other->join_attr != NULL) {
    return attrs_equal(join_attr, other->join_attr);
  } else {
    return false;
  }
}

const uint8_t *NewJoinRecord::get_attr(uint32_t attr_idx) const {
  return get_attr_internal(row + TABLE_ID_SIZE, attr_idx, num_cols());
}

bool NewJoinRecord::is_primary() const {
  return cmp(this->row, primary_id, TABLE_ID_SIZE) == 0;
}

bool NewJoinRecord::is_dummy() const {
  return test_dummy(this->row, JOIN_ROW_UPPER_BOUND) == 0;
}

void NewJoinRecord::reset_to_dummy() {
  write_dummy(this->row, JOIN_ROW_UPPER_BOUND);
}
