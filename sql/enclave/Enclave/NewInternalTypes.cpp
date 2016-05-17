// -*- c-basic-offset: 2 -*-

#include "NewInternalTypes.h"

uint32_t NewRecord::read(uint8_t *input) {
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

uint32_t NewRecord::write_decrypted(uint8_t *output) {
  cpy(output, this->row, this->row_length);
  return this->row_length;
}

void NewRecord::mark_dummy(uint8_t *types, uint32_t num_cols) {
  uint8_t *row_ptr = this->row;
  *( (uint32_t *) row_ptr) = num_cols;
  row_ptr += 4;

  uint32_t upper_bound = 0;
  for (uint32_t i = 0; i < num_cols; i++) {
    uint8_t t = types[i];
    *row_ptr = t;
    row_ptr += TYPE_SIZE;

    upper_bound = attr_upper_bound(t);

    *( (uint32_t *) row_ptr) = upper_bound;
    row_ptr += 4;
    row_ptr += upper_bound;
  }

  this->row_length = (row_ptr - row);
}

NewProjectRecord::~NewProjectRecord() {
  delete project_attributes;
}

uint32_t NewProjectRecord::read(uint8_t *input) {
  uint32_t result = r.read(input);
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
  uint32_t value_len = 0;

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

uint32_t NewJoinRecord::read(uint8_t *input) {
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
  uint32_t num_cols = *( (uint32_t *) (this->row + TABLE_ID_SIZE));
  get_join_attribute(op_code, this->num_cols(),
                     this->row + TABLE_ID_SIZE + 4, this->is_primary() ? 0 : -1,
                     &this->join_attr);
}
