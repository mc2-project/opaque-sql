#include "DataGen.h"
#include "common.h"


uint32_t generate_random_attribute(uint8_t attribute_type,
								   uint8_t *output_buffer) {
  uint8_t *output_ptr = output_buffer;

  *((uint8_t *) output_ptr) = attribute_type;
  *((uint32_t *) (output_ptr + TYPE_SIZE)) = attr_upper_bound(attribute_type);

  if (attribute_type == STRING) {
	// let's just always copy 'a' attr_upper_bound # of times
	for (uint32_t i = 0; i < attr_upper_bound(attribute_type); i++) {
	  *(output_ptr + HEADER_SIZE + i) = 'a';
	}
  } else if (attribute_type == INT) {
	sgx_read_rand(output_ptr + HEADER_SIZE, 2);
	*(output_ptr + HEADER_SIZE + 3) = 0;
	*(output_ptr + HEADER_SIZE + 4) = 0;
  }  else {
   
	sgx_read_rand(output_ptr + HEADER_SIZE, attr_upper_bound(attribute_type));
  }

  return attr_upper_bound(attribute_type) + HEADER_SIZE;
}

uint32_t generate_random_row(uint32_t num_cols, uint8_t *column_types,
							 uint8_t *output_buffer) {
  uint8_t *output_ptr = output_buffer;
  uint32_t offset = 0;

  *((uint32_t *) output_ptr) = num_cols;
  output_ptr += 4;

  // first write 
  for (uint32_t i = 0; i < num_cols; i++) {
	offset = generate_random_attribute(column_types[i], output_ptr);
	output_ptr += offset;
  }

  return output_ptr - output_buffer;
}

uint32_t generate_random_rows(uint32_t num_cols, uint8_t *column_types,
							  uint32_t num_rows,
							  uint8_t *output_buffer) {
  uint8_t *output_ptr = output_buffer;
  uint32_t offset = 0;
  
  for (uint32_t i = 0; i < num_rows; i++) {
	offset = generate_random_row(num_cols, column_types, output_ptr);
	output_ptr += offset;
  }

  return output_ptr - output_buffer;
}

uint32_t generate_encrypted_block(uint32_t num_cols, uint8_t *column_types,
								  uint32_t num_rows,
								  uint8_t *output_buffer) {
  uint32_t row_upper_bound = 4 + HEADER_SIZE * num_cols;

  for (uint32_t i = 0; i < num_cols; i++) {
	row_upper_bound += attr_upper_bound(column_types[i]);
  }

  uint8_t *buf = (uint8_t *) malloc(num_rows * row_upper_bound);

  generate_random_rows(num_cols, column_types,
					   num_rows, buf);
  
  // just encrypt this entire block into output_buffer
  *((uint32_t *) (output_buffer)) = enc_size(num_rows * row_upper_bound);
  *((uint32_t *) (output_buffer + 4)) = num_rows;
  *((uint32_t *) (output_buffer + 8)) = row_upper_bound;
  encrypt(buf, num_rows * row_upper_bound, output_buffer + 12);
  
  free(buf);

  return 12 + enc_size(num_rows * row_upper_bound);
}
