// -*- c-basic-offset: 2 -*-

#include "NewInternalTypes.h"

uint32_t NewRecord::decrypt_row(uint8_t *input) {
  uint8_t *input_ptr = input;
  uint8_t *row_ptr = this->row;

  this->num_cols = *( (uint32_t *) input_ptr);
  input_ptr += 4;

  for (uint32_t i = 0; i < this->num_cols; i++) {
    decrypt_attribute(&input_ptr, &row_ptr);
  }

  return (input_ptr - input);
}

NewProjectRecord::~NewProjectRecord() {
  delete project_attributes;
}

uint32_t NewProjectRecord::read(uint8_t *input) {
  uint32_t result = NewRecord::decrypt_row(input);
  this->set_project_attributes(op_code);
  return result;
}

void NewProjectRecord::set_project_attributes(int op_code) {
  if (project_attributes == NULL) {
    project_attributes = new ProjectAttributes(op_code, row, num_cols);
    project_attributes->init();
    project_attributes->evaluate();
  } else {
    project_attributes->re_init(this->row);
    project_attributes->evaluate();
  }

}

uint32_t NewProjectRecord::write(uint8_t *output) {
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
