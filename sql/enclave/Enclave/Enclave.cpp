/**
*   Copyright(C) 2011-2015 Intel Corporation All Rights Reserved.
*
*   The source code, information  and  material ("Material") contained herein is
*   owned  by Intel Corporation or its suppliers or licensors, and title to such
*   Material remains  with Intel Corporation  or its suppliers or licensors. The
*   Material  contains proprietary information  of  Intel or  its  suppliers and
*   licensors. The  Material is protected by worldwide copyright laws and treaty
*   provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
*   modified, published, uploaded, posted, transmitted, distributed or disclosed
*   in any way  without Intel's  prior  express written  permission. No  license
*   under  any patent, copyright  or  other intellectual property rights  in the
*   Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
*   implication, inducement,  estoppel or  otherwise.  Any  license  under  such
*   intellectual  property  rights must  be express  and  approved  by  Intel in
*   writing.
*
*   *Third Party trademarks are the property of their respective owners.
*
*   Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
*   this  notice or  any other notice embedded  in Materials by Intel or Intel's
*   suppliers or licensors in any way.
*/

#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"

void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length,
		   uint8_t *ciphertext, uint32_t cipher_length) {

  // // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)
  assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);

  // uint8_t *iv_ptr = ciphertext;
  // sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  // uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;

  encrypt(plaintext, plaintext_length, ciphertext);
}

void ecall_decrypt(uint8_t *ciphertext, 
				   uint32_t ciphertext_length,
				   uint8_t *plaintext,
				   uint32_t plaintext_length) {

  // // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)
  assert(ciphertext_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);

  // uint8_t *iv_ptr = ciphertext;
  // sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  // uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;

  decrypt(ciphertext, ciphertext_length, plaintext);
}

void ecall_test_int(int *ptr) {
  *ptr = *ptr + 1;
}


// given op code, compare two input buffers
int compare(int op_code, 
	    uint8_t *value1, uint32_t value1_len,
	    uint8_t *value2, uint32_t value2_len) {
  
  
}

// TODO: swap two buffers
void swap(uint8_t *value1, uint32_t value1_len,
	  uint8_t *value2, uint32_t value2_len) {
  

}


int log_2(int value) {
  double dvalue = (double) value;
  int log_value = (int) ceil(log(dvalue) / log(2));
  return log_value;
}

int pow_2(int value) {
  double dvalue = (double) value;
  int pow_value = (int) pow(2, dvalue);
  return pow_value;
}

// TODO: this sorts integers only... put in custom comparison operators!
// TODO: how to make the write oblivious?
void osort_with_index_int(int *input, int low_idx, uint32_t len) {

  //printf("low_idx: %u, len: %u\n", low_idx, len);
  
  int log_len = log_2(len) + 1;
  int offset = low_idx;

  int swaps = 0;
  int min_val = 0;
  int max_val = 0;
  
  for (int stage = 1; stage <= log_len; stage++) {
    //printf("stage = %i\n", stage);
    for (int stage_i = stage; stage_i >= 1; stage_i--) {
      //printf("stage_i = %i\n", stage_i);
      int part_size = pow_2(stage_i);
      //printf("part_size = %i\n", part_size);
      int part_size_half = part_size / 2;

      if (stage_i == stage) {
	for (int i = offset; i <= (offset + len - 1); i += part_size) {
	  for (int j = 1; j <= part_size_half; j++) {
	    int idx = i + j - 1;
	    int pair_idx = i + part_size - j;

	    if (pair_idx < offset + len) {

	      int idx_value = *(input + idx);
	      int pair_idx_value = *(input + pair_idx);

	      min_val = idx_value < pair_idx_value ? idx_value : pair_idx_value;
	      max_val = idx_value > pair_idx_value ? idx_value : pair_idx_value;

	      *(input + idx) = min_val;
	      *(input + pair_idx) = max_val;

	    }
	  }
	}

      } else {	

	for (int i = offset; i <= (offset + len - 1); i += part_size) {
	  for (int j = 1; j <= part_size_half; j++) {
	    int idx = i + j - 1;
	    int pair_idx = idx + part_size_half;

 	    if (pair_idx < offset + len) {
	      int idx_value = *(input + idx);
	      int pair_idx_value = *(input + pair_idx);

	      min_val = idx_value < pair_idx_value ? idx_value : pair_idx_value;
	      max_val = idx_value > pair_idx_value ? idx_value : pair_idx_value;

	      *(input + idx) = min_val;
	      *(input + pair_idx) = max_val;

	    }

	  }
	}

      }

    }
  }

}

// only sorts integers!
void ecall_oblivious_sort_int(int *input, uint32_t input_len) {
  osort_with_index_int(input, 0, input_len);
}



// input_length is the length of input in bytes
// len is the number of records
template<typename T>
void osort_with_index(int op_code, T *input, uint32_t input_length, int low_idx, uint32_t len) {

  // First, iterate through and decrypt the data
  // Then store the decrypted data in a list of objects (based on the given op_code)

  
  
  int log_len = log_2(len) + 1;
  int offset = low_idx;

  int swaps = 0;
  int min_val = 0;
  int max_val = 0;
  
  for (int stage = 1; stage <= log_len; stage++) {
    //printf("stage = %i\n", stage);
    for (int stage_i = stage; stage_i >= 1; stage_i--) {
      //printf("stage_i = %i\n", stage_i);
      int part_size = pow_2(stage_i);
      //printf("part_size = %i\n", part_size);
      int part_size_half = part_size / 2;

      if (stage_i == stage) {
		for (int i = offset; i <= (offset + len - 1); i += part_size) {
		  for (int j = 1; j <= part_size_half; j++) {
			int idx = i + j - 1;
			int pair_idx = i + part_size - j;

			if (pair_idx < offset + len) {

			  T *idx_value = input + idx;
			  T *pair_idx_value = input + pair_idx;

			  compare_and_swap<T>(idx_value, pair_idx_value);
			}
		  }
		}

      } else {	

		for (int i = offset; i <= (offset + len - 1); i += part_size) {
		  for (int j = 1; j <= part_size_half; j++) {
			int idx = i + j - 1;
			int pair_idx = idx + part_size_half;

			if (pair_idx < offset + len) {
			  T *idx_value = input + idx;
			  T *pair_idx_value = input + pair_idx;

			  compare_and_swap<T>(idx_value, pair_idx_value);

			}

		  }
		}

      }

    }
  }

}

// TODO: format of this input array?
void ecall_oblivious_sort(int op_code, uint8_t *input, uint32_t buffer_length,
						  int low_idx, uint32_t list_length) {
  // iterate through all data, and then decrypt

  // op_code = 1 means it's a list of integers
  if (op_code == 1) {
	Integer * data = (Integer *) malloc(sizeof(Integer) * list_length);
	if (data == NULL) {
	  printf("data is NULL!\n");
	}

	uint8_t *input_ptr = input;
	uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *enc_value_ptr = NULL;
	uint32_t enc_value_len = 0;

	for (uint32_t i = 0; i < list_length; i++) {
	  get_next_value(&input_ptr, &enc_value_ptr, &enc_value_len);

	  //printf("enc_value_len is %u\n", enc_value_len);
	  
	  decrypt(enc_value_ptr, enc_value_len, data_ptr);
	  data_ptr += 4;
	}


	//void osort_with_index(int op_code, T *input, uint32_t input_length, int low_idx, uint32_t len)
	osort_with_index<Integer>(op_code, data, sizeof(Integer) * list_length, low_idx, list_length);

	// need to re-encrypt the data!
	// since none of the data has changed, let's try re-using the input

	uint32_t enc_size = 4 + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
	input_ptr = input;
	data_ptr = (uint8_t *) data;

	uint32_t *size_ptr = (uint32_t *) data_ptr;

	for (uint32_t i = 0; i < list_length; i++) {
	  size_ptr = (uint32_t *) input_ptr;
	  *size_ptr = enc_size;
	  encrypt(data_ptr, 4, input_ptr + 4);

	  data_ptr += 4;
	  input_ptr += enc_size + 4;
	}

	free(data);
	
  } else if (op_code == 2) {
	// this needs to sort a row of data

	uint32_t sort_attr_num = 2;

	Record *data = (Record *) malloc(sizeof(Record) * list_length);
	if (data == NULL) {
	  printf("Could not allocate enough data\n");
	}

	uint8_t *input_ptr = input;
	uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *enc_row_ptr = NULL;
	uint32_t enc_row_len = 0;


	uint8_t *enc_value_ptr = NULL;
	uint32_t enc_value_len = 0;
 	uint8_t *value_ptr = NULL;
	uint32_t value_len = 0;

	uint8_t *data_ptr_ = NULL;

	for (uint32_t i = 0; i < list_length; i++) {
	  get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);
	  data[i].num_cols = *( (uint32_t *) enc_row_ptr);
	  
	  // 4 * data[i].num_cols represents the 4 bytes for each encrypted value
	  // the extra 4 represents the 4 bytes used to store number of columns
	  uint32_t plaintext_data_size = enc_row_len - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE + 4) * data[i].num_cols - 4;
	  data[i].data = (uint8_t *) malloc(plaintext_data_size);
	  data[i].sort_attribute = NULL;

	  data_ptr_ = data[i].data;
	  enc_row_ptr += 4;
	  enc_value_ptr = enc_row_ptr;

	  //printf("Data item %u; has %u columns\n", i, data[i].num_cols);
	  //printf("plaintext_data_size: %u\n", plaintext_data_size);

	  // iterate through encrypted attributes, 
	  for (uint32_t j = 0; j < data[i].num_cols; j++) {

		assert(enc_value_ptr <= input_ptr);
		// [enc len]encrypted{[value type][value len][value]}

		enc_value_len = *( (uint32_t *) enc_value_ptr);
		enc_value_ptr += 4;
		// value_len includes the type's length as well
		value_len = enc_value_len - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);

		//printf("Attribute %u: encrypted value's length is %u; data[i].num_cols is %u\n", j, enc_value_len, data[i].num_cols);
		
		decrypt(enc_value_ptr, enc_value_len, data_ptr_);

		if (j + 1 == sort_attr_num) {
		  data[i].sort_attribute = data_ptr_;
		}

		enc_value_ptr += enc_value_len;
		data_ptr_ += value_len;

	  }
	}
	
	osort_with_index<Record>(op_code, data, sizeof(Record) * list_length, low_idx, list_length);

	//printf("Sorted data\n");
	
	// TODO: need to return enrypted result

	input_ptr = input;
	value_ptr = NULL;
	value_len = 0;

	for (uint32_t i = 0; i < list_length; i++) {

	  value_ptr = data[i].data;
	  print_bytes(value_ptr, 5);
	  *( (uint32_t *) input_ptr) = data[i].num_cols;
	  input_ptr += 4;

	  //printf("Num cols is %u\n", data[i].num_cols);

	  // need to encrypt each attribute separately
	  for (uint32_t c = 0; c < data[i].num_cols; c++) {

		value_len = *( (uint32_t *) (value_ptr + 1) ) + 5;

		*( (uint32_t *)  input_ptr) = value_len + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
		input_ptr += 4;

		encrypt(value_ptr, value_len, input_ptr);
		input_ptr += value_len + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;

		//printf("attr's length is %u\n", value_len);

		value_ptr += value_len;
	  }
	}

	//printf("Encrypted data\n");

	//printf("Encrypted data's length is %u\n", (input_ptr - input));


	// TODO: free data, including record pointers

	for (uint32_t i = 0; i < list_length; i++) {
	  free(data[i].data);
	}

	free(data);

	//printf("Freed data\n");
  } else if (op_code == 3) {
	// get the primary/foreign table indicators
	uint8_t primary_table[TABLE_ID_SIZE];
	uint8_t foreign_table[TABLE_ID_SIZE];
	get_table_indicator(primary_table, foreign_table);
	
	// this is a sort used for sort-merge join!
	// that means there are two different join attributes:
	// one for the primary key table, the other the foreign key table
	
	uint32_t sort_attr_num_p = 2;
	uint32_t sort_attr_num_f = 2;
	
	JoinRecord *data = (JoinRecord *) malloc(sizeof(JoinRecord) * list_length);
	if (data == NULL) {
	  printf("Could not allocate enough data\n");
	  assert(false);
	}

	// allocate memory for record
	uint8_t *input_ptr = input;
	uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *data_ptr_ = NULL;
	uint8_t *table_id = NULL;
	uint8_t *value_ptr = NULL;
	uint32_t value_len = 0;
	
	for (uint32_t i = 0; i < list_length; i++) {
	  // allocate JOIN_ROW_UPPER_BOUND
	  Record *record = &(data[i].record);
	  
	  record->data = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND);
	  decrypt(input_ptr, enc_size(JOIN_ROW_UPPER_BOUND), record->data);
	  input_ptr += enc_size(JOIN_ROW_UPPER_BOUND);

	  table_id = record->data;
	  data_ptr_ = record->data + TABLE_ID_SIZE;
	  record->num_cols = *( (uint32_t *) data_ptr_);
	  
	  value_ptr = record->data + TABLE_ID_SIZE + 4;
	  
	  data[i].if_primary = cmp(table_id, primary_table, TABLE_ID_SIZE);

	  //printf("Num cols is %u, if_primary: %d\n", record->num_cols, data[i].if_primary);

	  // identify the join attribute
	  for (uint32_t j = 0; j < record->num_cols; j++) {
		value_len = *( (uint32_t *) (value_ptr + TYPE_SIZE));
		
		if (data[i].if_primary == 0) {
		  if (j + 1 == sort_attr_num_p) {
			record->sort_attribute = value_ptr;
			//print_attribute("primary table", record->sort_attribute);
		  }
		} else {
		  if (j + 1 == sort_attr_num_p) {
			record->sort_attribute = value_ptr;
			//print_attribute("foreign table", record->sort_attribute);
		  }
		}

		value_ptr += value_len + HEADER_SIZE;
	  }

	}

	osort_with_index<JoinRecord>(op_code, data, sizeof(JoinRecord) * list_length, low_idx, list_length);

	// Produce encrypted output rows
	input_ptr = input;
	for (uint32_t i = 0; i < list_length; i++) {
	  // simply encrypt the entire row all at once
	  //print_join_row("Join row ", data[i].record.data);
	  encrypt(data[i].record.data, JOIN_ROW_UPPER_BOUND, input_ptr);
	  input_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	}

	// free data
	for (uint32_t i = 0; i < list_length; i++) {
	  free(data[i].record.data);
	}

	free(data);
  }
}

// TODO: return encrypted random identifier
void ecall_random_id(uint8_t *ptr, uint32_t length) {
  sgx_status_t rand_status = sgx_read_rand(ptr, length);
}

void ecall_test() {
  agg_test();
}

/**** BEGIN Aggregation ****/
void ecall_scan_aggregation_count_distinct(int op_code,
										   uint8_t *input_rows, uint32_t input_rows_length,
										   uint32_t num_rows,
										   uint8_t *agg_row, uint32_t agg_row_buffer_length,
										   uint8_t *output_rows, uint32_t output_rows_length,
										   uint32_t *actual_size,
										   int flag) {
  scan_aggregation_count_distinct(op_code,
								  input_rows, input_rows_length, num_rows,
								  agg_row, agg_row_buffer_length,
								  output_rows, output_rows_length,
								  actual_size,
								  flag);
}

void ecall_process_boundary_records(int op_code,
									uint8_t *rows, uint32_t rows_size,
									uint32_t num_rows,
									uint8_t *out_agg_rows, uint32_t out_agg_row_size) {

  process_boundary_records(op_code,
						   rows, rows_size,
						   num_rows,
						   out_agg_rows, out_agg_row_size);
}


void ecall_final_aggregation(int op_code,
							 uint8_t *agg_rows, uint32_t agg_rows_length,
							 uint32_t num_rows,
							 uint8_t *ret, uint32_t ret_length) {
  final_aggregation(op_code,
					agg_rows, agg_rows_length,
					num_rows,
					ret, ret_length);
  
}

/**** END Aggregation ****/



/**** BEGIN Join ****/

size_t enc_table_id_size(const uint8_t *enc_table_id) {
  return (size_t) (enc_size(TABLE_ID_SIZE));
}

size_t join_row_size(const uint8_t *join_row) {
  printf("Enc join row size is %u\n", enc_size(JOIN_ROW_UPPER_BOUND));
  return (size_t) (enc_size(JOIN_ROW_UPPER_BOUND));
}


void ecall_join_sort_preprocess(int op_code,
								uint8_t *table_id, 
								uint8_t *input_row, uint32_t input_row_len,
								uint32_t num_rows,
								uint8_t *output_row, uint32_t output_row_len) {

  // pre-process the rows, make a call to join_sort_preprocess for each row
  uint8_t *input_row_ptr = input_row;
  uint8_t *enc_row_ptr = NULL;
  uint32_t enc_row_len = 0;

  uint8_t *output_row_ptr = output_row;
  
  for (uint32_t i = 0; i < num_rows; i++) {
	get_next_row(&input_row_ptr, &enc_row_ptr, &enc_row_len);
	
	join_sort_preprocess(op_code, table_id, 
						 enc_row_ptr, enc_row_len,
						 output_row_ptr, enc_size(JOIN_ROW_UPPER_BOUND));

	output_row_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
  }
}

void ecall_scan_collect_last_primary(int op_code,
									 uint8_t *input_rows, uint32_t input_rows_length,
									 uint32_t num_rows,
									 uint8_t *output, uint32_t output_length) {
  scan_collect_last_primary(op_code,
							input_rows, input_rows_length,
							num_rows,
							output, output_length);
}

void ecall_process_join_boundary(uint8_t *input_rows, uint32_t input_rows_length,
								 uint32_t num_rows,
								 uint8_t *output_rows, uint32_t output_rows_size,
								 uint8_t *enc_table_p, uint8_t *enc_table_f) {
  
  process_join_boundary(input_rows, input_rows_length,
						num_rows,
						output_rows, output_rows_size,
						enc_table_p, enc_table_f);
	
}


void ecall_sort_merge_join(int op_code,
						   uint8_t *input_rows, uint32_t input_rows_length,
						   uint32_t num_rows,
						   uint8_t *join_row, uint32_t join_row_length,
						   uint8_t *output_rows, uint32_t output_rows_length) {
  
  sort_merge_join(op_code,
				  input_rows, input_rows_length, num_rows,
				  join_row, join_row_length,
				  output_rows, output_rows_length);
}

/**** END Join ****/

