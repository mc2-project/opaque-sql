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

NewProjectRecord::~NewProjectRecord() {
  delete project_attributes;
}

uint32_t NewProjectRecord::read(uint8_t *input) {
  uint32_t result = NewRecord::read(input);
  this->set_project_attributes();
  return result;
}

void NewProjectRecord::set_project_attributes() {
  if (project_attributes == NULL) {
    project_attributes = new ProjectAttributes(op_code, row + 4, num_cols());
    project_attributes->init();
    project_attributes->evaluate();
  } else {
    project_attributes->re_init(row + 4);
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

void NewJoinRecord::set(bool is_primary, NewRecord *record) {
  uint8_t primary_table[TABLE_ID_SIZE];
  uint8_t foreign_table[TABLE_ID_SIZE];
  get_table_indicator(primary_table, foreign_table);

  uint8_t *row_ptr = this->row;
  if (is_primary) {
    cpy(row_ptr, primary_table, TABLE_ID_SIZE);
  } else {
    cpy(row_ptr, foreign_table, TABLE_ID_SIZE);
  }
  row_ptr += TABLE_ID_SIZE;

  row_ptr += record->write_decrypted(row_ptr);

  this->row_length = row_ptr - row;
}

uint32_t NewJoinRecord::write_encrypted(uint8_t *output) {
  encrypt(this->row, JOIN_ROW_UPPER_BOUND, output);
  return enc_size(JOIN_ROW_UPPER_BOUND);
}
