#include "Filter.h"

int ecall_filter_single_row(int op_code, uint8_t *row, uint32_t length) {

  // row is in format of [num cols][enc_attr1 size][enc_attr1][enc_attr2 size][enc_attr2] ...
  // enc_attr's format is [type][len of plaintext attr][plaintext attr]
  // num cols is 4 bytes; size is also 4 bytes
  int ret = 1;

  uint32_t num_cols = get_num_col(row);
  if (num_cols == 0) {
    // printf("Number of columns: 0\n");
    return 0;
  }

  // printf("Number of columns: %u\n", num_cols);

  uint8_t *row_ptr = row + 4;

  const size_t decrypted_data_len = 1024;
  uint8_t decrypted_data[2048];
  uint8_t *decrypted_data_ptr = decrypted_data;


  uint8_t *enc_value_ptr = NULL;
  uint32_t enc_value_len = 0;

  uint8_t attr_type = 0;
  uint32_t attr_len = 0;
  uint8_t *attr_ptr = NULL;

  if (op_code == OP_FILTER_COL2_GT3) {
    // find the second attribute
		
	find_attribute(row_ptr, length, num_cols,
				   2,
				   &enc_value_ptr, &enc_value_len);

	//decrypt(enc_value_ptr, enc_value_len, decrypted_data);
	enc_value_ptr -= 4;
	decrypt_attribute(&enc_value_ptr, &decrypted_data_ptr);

    int *value_ptr = (int *) (decrypted_data + HEADER_SIZE);
	//printf("value is %u\n", *value_ptr);

    if (*value_ptr <= 3) {
      ret = 0;
    }

  } else if (op_code == OP_BD1) {
    find_attribute(row_ptr, length, num_cols,
                   2,
                   &enc_value_ptr, &enc_value_len);
    enc_value_ptr -= 4;
    decrypt_attribute(&enc_value_ptr, &decrypted_data_ptr);
    int *value_ptr = (int *) (decrypted_data + HEADER_SIZE);
    if (*value_ptr <= 1000) {
      ret = 0;
    }
  } else if (op_code == OP_FILTER_COL4_NOT_DUMMY) {
    // Filter out rows with a dummy attribute in the 4th column. Such rows represent partial aggregates
	decrypted_data_ptr = decrypted_data;

    find_attribute(row_ptr, length, num_cols, 4, &enc_value_ptr, &enc_value_len);
	//printf("attr 4, enc_value_len is %u\n", enc_value_len);
	enc_value_ptr -= 4;
	decrypt_attribute(&enc_value_ptr, &decrypted_data_ptr);

	attr_type = *decrypted_data;

	//printf("attr_type is %u\n", attr_type);

    if (is_dummy_type(attr_type)) {
      ret = 0;
    }

  } else if (op_code == OP_FILTER_TEST) {
    // this is for test only

	find_attribute(row_ptr, length, num_cols,
				   1,
				   &enc_value_ptr, &enc_value_len);
	decrypt(enc_value_ptr, enc_value_len, decrypted_data);
	get_attr(decrypted_data, &attr_type, &attr_len, &attr_ptr);
    
    int *value_ptr = (int *) attr_ptr;

    // printf("Input value is %u\n", *value_ptr);
    // printf("Attr len is  is %u\n", attr_len);
	
    ret = 0;
  } else {
    printf("Error in ecall_filter_single_row: unexpected op code %d\n", op_code);
    assert(false);
  }

  return ret;
}
