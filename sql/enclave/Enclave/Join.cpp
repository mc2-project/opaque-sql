// Join in enclave: assumes that the records have been sorted
// by the join attribute already
// This method takes in a temporay row (which could be a dummy row)
// Then it compares with the following rows (which should contain the row info, as well as the table info

// TODO: format of the rows?
// TODO: how to determine an upper bound ??
// TODO: how to determine the output size?
void sort_join(uint8_t *enc_temp_row, uint32_t enc_temp_row_len,
			   uint8_t *input, uint32_t buffer_length, uint32_t list_length,
			   uint8_t *output, uint32_t output_length) {
  //uint8_t *temp_row = (uint8_t *) malloc(ROW_UPPER_BOUND);
  decrypt(enc_temp_row, enc_temp_row_len, temp_row);

  // iterate through the rows, and join
  
  
}
