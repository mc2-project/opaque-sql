#include "Project.h"

void project(int op_code,
			 uint8_t *input_rows, uint32_t input_rows_length,
			 uint32_t num_rows,
			 uint8_t *output_rows, uint32_t output_rows_length,
			 uint32_t *actual_output_rows_length) {

  uint8_t *input_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;
  uint8_t *enc_row_ptr = NULL;
  uint32_t enc_row_len = 0;

  uint8_t *prev_row_ptr = NULL;
  uint8_t prev_row_len = 0;
  
  uint8_t *enc_value_ptr = NULL;
  uint32_t enc_value_len = 0;

  uint8_t value_type = 0;
  uint32_t value_len = 0;
  uint8_t *value_ptr = NULL;


  ProjectRecord current_row;

  for (uint32_t i = 0; i < num_rows; i++) {
	get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);

	current_row.clear();
	current_row.consume_all_encrypted_attributes(enc_row_ptr);
	current_row.set_project_attributes(op_code);

	// after evaluation, write out to output_rows

	output_rows_ptr += current_row.flush_encrypt_eval_attributes(output_rows_ptr);
	
  }

  actual_output_rows_length = (output_rows_ptr - output_rows);
  
}
