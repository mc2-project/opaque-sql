#include "DataGen.h"
#include "common.h"

// All of these generate fake rows for unit tests

uint32_t generate_random_aggregate_attribute(uint8_t attribute_type,
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
	sgx_read_rand(output_ptr + HEADER_SIZE, 1);
	uint32_t *int_ptr = (uint32_t *) (output_ptr + HEADER_SIZE);
	uint32_t int_val = *int_ptr % 4;
	*int_ptr = int_val;
  }  else {
	sgx_read_rand(output_ptr + HEADER_SIZE, attr_upper_bound(attribute_type));
  }

  return attr_upper_bound(attribute_type) + HEADER_SIZE;
}


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

uint32_t generate_non_random_attribute(uint8_t attribute_type,
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
	uint32_t *int_ptr = (uint32_t *) (output_ptr + HEADER_SIZE);
	*int_ptr = 1;
  }  else {
	sgx_read_rand(output_ptr + HEADER_SIZE, attr_upper_bound(attribute_type));
  }

  return attr_upper_bound(attribute_type) + HEADER_SIZE;
}


uint32_t generate_random_join_p_attribute(uint8_t attribute_type,
										  uint8_t *output_buffer) {
  // primary has to be unique
  static int current_int = 0;
  
  uint8_t *output_ptr = output_buffer;

  *((uint8_t *) output_ptr) = attribute_type;
  *((uint32_t *) (output_ptr + TYPE_SIZE)) = attr_upper_bound(attribute_type);

  if (attribute_type == STRING) {
	// let's just always copy 'a' attr_upper_bound # of times
	for (uint32_t i = 0; i < attr_upper_bound(attribute_type); i++) {
	  *(output_ptr + HEADER_SIZE + i) = 'a';
	}
  } else if (attribute_type == INT) {
	uint32_t *int_ptr = (uint32_t *) (output_ptr + HEADER_SIZE);
	*int_ptr = current_int;
	++current_int;
  }  else {
	sgx_read_rand(output_ptr + HEADER_SIZE, attr_upper_bound(attribute_type));
  }

  return attr_upper_bound(attribute_type) + HEADER_SIZE;
}

uint32_t generate_random_join_f_attribute(uint8_t attribute_type,
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
	sgx_read_rand(output_ptr + HEADER_SIZE, 1);
	uint32_t *int_ptr = (uint32_t *) (output_ptr + HEADER_SIZE);
	uint32_t int_val = *int_ptr % 4;
	*int_ptr = int_val;
  }  else {
	sgx_read_rand(output_ptr + HEADER_SIZE, attr_upper_bound(attribute_type));
  }

  return attr_upper_bound(attribute_type) + HEADER_SIZE;
}

uint32_t generate_random_row(uint32_t num_cols, uint8_t *column_types,
							 uint8_t *output_buffer,
							 uint8_t type) {
  uint8_t *output_ptr = output_buffer;
  uint32_t offset = 0;

  *((uint32_t *) output_ptr) = num_cols;
  output_ptr += 4;

  // first write 
  for (uint32_t i = 0; i < num_cols; i++) {

	switch (type) {
	case DATA_GEN_AGG:
	  {
		if (i == 0) {
		  offset = generate_random_aggregate_attribute(column_types[i], output_ptr);
		} else {
		  offset = generate_non_random_attribute(column_types[i], output_ptr);
		}
	  }
	  break;
	  
	case DATA_GEN_JOIN_P:
	  {
		if (i == 0) {
		  offset = generate_random_join_p_attribute(column_types[i], output_ptr);
		} else {
		  offset = generate_random_attribute(column_types[i], output_ptr);
		}
	  }
	  break;
	  
	case DATA_GEN_JOIN_F:
	  {
		if (i == 0) {
		  offset = generate_random_join_f_attribute(column_types[i], output_ptr);
		} else {
		  offset = generate_random_attribute(column_types[i], output_ptr);
		}
	  }
	  break;
	  
	case DATA_GEN_REGULAR:
	  {
		offset = generate_random_attribute(column_types[i], output_ptr);
	  }
	  break;
	}
	
	output_ptr += offset;
  }

  return output_ptr - output_buffer;
}

uint32_t generate_random_rows(uint32_t num_cols, uint8_t *column_types,
							  uint32_t num_rows,
							  uint8_t *output_buffer,
							  uint8_t type) {
  
  uint8_t *output_ptr = output_buffer;
  uint32_t offset = 0;
  
  for (uint32_t i = 0; i < num_rows; i++) {
	offset = generate_random_row(num_cols, column_types, output_ptr, type);
	output_ptr += offset;
  }

  return output_ptr - output_buffer;
}

uint32_t generate_encrypted_block(uint32_t num_cols, uint8_t *column_types,
				  uint32_t num_rows,
				  uint8_t *output_buffer,
				  uint8_t type) {
  
  uint32_t row_upper_bound = 4 + HEADER_SIZE * num_cols;

  for (uint32_t i = 0; i < num_cols; i++) {
	row_upper_bound += attr_upper_bound(column_types[i]);
  }

  uint8_t *buf = (uint8_t *) malloc(num_rows * row_upper_bound + 1);

  uint32_t written_bytes = generate_random_rows(num_cols, column_types, num_rows, buf, type);

  printf("[generate_encrypted_block] Row upper bound is %u, num_cols is %u, written_bytes is %u\n", row_upper_bound, num_cols, written_bytes);
  
  // just encrypt this entire block into output_buffer
  *((uint32_t *) (output_buffer)) = enc_size(num_rows * row_upper_bound);
  *((uint32_t *) (output_buffer + 4)) = num_rows;
  *((uint32_t *) (output_buffer + 8)) = row_upper_bound;

  encrypt(buf, num_rows * row_upper_bound, output_buffer + 12);
  
  free(buf);

  return 12 + enc_size(num_rows * row_upper_bound);
}


uint32_t generate_encrypted_block_with_opcode(uint32_t num_cols, uint8_t *column_types,
					      uint32_t num_rows,
					      uint8_t *output_buffer,
					      uint8_t type,
					      uint32_t opcode) {
  
  uint32_t row_upper_bound = 4 + HEADER_SIZE * num_cols;

  for (uint32_t i = 0; i < num_cols; i++) {
    row_upper_bound += attr_upper_bound(column_types[i]);
  }

  uint8_t *buf = (uint8_t *) malloc(num_rows * row_upper_bound + 1);

  uint32_t written_bytes = generate_random_rows(num_cols, column_types, num_rows, buf, type);

  printf("[generate_encrypted_block] Row upper bound is %u, num_cols is %u, written_bytes is %u\n", row_upper_bound, num_cols, written_bytes);
  
  // just encrypt this entire block into output_buffer
  *((uint32_t *) (output_buffer)) = enc_size(num_rows * row_upper_bound);
  *((uint32_t *) (output_buffer + 4)) = num_rows;
  *((uint32_t *) (output_buffer + 8)) = row_upper_bound;
  *((uint32_t *) (output_buffer + 12)) = opcode;

  encrypt(buf, num_rows * row_upper_bound, output_buffer + 16);
  
  free(buf);

  return 16 + enc_size(num_rows * row_upper_bound);
}
