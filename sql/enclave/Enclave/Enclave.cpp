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
#include "util.h"

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

// returns the number of attributes for this row
uint32_t get_num_col(uint8_t *row) {
  uint32_t *num_col_ptr = (uint32_t *) row;
  return *num_col_ptr;
}

uint8_t *get_enc_attr(uint8_t **enc_attr_ptr, uint32_t *enc_attr_len, 
					  uint8_t *row_ptr, uint8_t *row, uint32_t length) {
  if (row_ptr >= row + length) {
    return NULL;
  }

  uint8_t *ret_row_ptr = row_ptr;

  *enc_attr_ptr = row_ptr + 4;
  *enc_attr_len = * ((uint32_t *) row_ptr);

  ret_row_ptr += 4 + *enc_attr_len;
  return ret_row_ptr;
}


void get_attr(uint8_t *dec_attr_ptr,
			  uint8_t *type, uint32_t *attr_len, uint8_t **attr_ptr) {

  // given a pointer to the encrypted attribute, return the type, attr len, attr pointer
  assert(dec_attr_ptr != NULL);
  *type = *dec_attr_ptr;

  uint32_t *attr_len_ptr = (uint32_t *) (dec_attr_ptr + 1);
  *attr_len = *(dec_attr_ptr + 1);
  
  *attr_ptr  = (dec_attr_ptr + 1 + *attr_len);
}


int ecall_filter_single_row(int op_code, uint8_t *row, uint32_t length) {

  // row is in format of [num cols][enc_attr1 size][enc_attr1][enc_attr2 size][enc_attr2] ...
  // enc_attr's format is [type][len of plaintext attr][plaintext attr]
  // num cols is 4 bytes; size is also 4 bytes
  int ret = 1;

  uint32_t num_cols = get_num_col(row);
  if (num_cols == 0) {
    printf("Number of columns: 0\n");    
    return 0;
  }

  printf("Number of columns: %u\n", num_cols);

  uint8_t *row_ptr = row + 4;

  const size_t decrypted_data_len = 1024;
  uint8_t decrypted_data[decrypted_data_len];


  uint8_t *enc_attr_ptr = NULL;
  uint32_t enc_attr_len = 0;

  uint8_t attr_type = 0;
  uint32_t attr_len = 0;
  uint8_t *attr_ptr = NULL;

  if (op_code == 0) {
    // find the second attribute
		
    row_ptr = get_enc_attr(&enc_attr_ptr, &enc_attr_len, row_ptr, row, length);
    row_ptr = get_enc_attr(&enc_attr_ptr, &enc_attr_len, row_ptr, row, length);

	decrypt(enc_attr_ptr, enc_attr_len, decrypted_data);

	get_attr(decrypted_data, &attr_type, &attr_len, &attr_ptr);
	

	// since op_code is 0, number should be "integer"
    int *value_ptr = (int *) attr_ptr;

    if (*value_ptr <= 3) {
      ret = 0;
    }

  } else if (op_code == -1) {
    // this is for test only

	row_ptr = get_enc_attr(&enc_attr_ptr, &enc_attr_len, row_ptr, row, length);
	decrypt(enc_attr_ptr, enc_attr_len, decrypted_data);
	get_attr(decrypted_data, &attr_type, &attr_len, &attr_ptr);
    
    int *value_ptr = (int *) attr_ptr;

    printf("Input value is %u\n", *value_ptr);
    printf("Attr len is  is %u\n", attr_len);
	
    ret = 0;
  }

  return ret;
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


typedef struct Integer {
  int value;
} Integer;

typedef struct String {
  uint32_t length;
  char *buffer;
} String;


typedef struct Record {
  uint32_t num_cols;
  // data in the format of [attr1][attr2] ...
  // Attribute's format is [type][size][data]
  uint8_t *data;
  // this is the attribute used for sort-based comparison/join
  // pointer is to the start of "type"
  uint8_t *sort_attribute;
} Record;

// -1 means value1 < value2
// 0 means value1 == value2
// 1 means value1 > value2
template<typename T>
int compare(T *value1, T *value2) {
  printf("compare(): Type not supported\n");
  assert(false);
}

template<typename T>
void compare_and_swap(T *value1, T* value2) {
  printf("compare_and_swap(): Type not supported\n");
  assert(false);
}


template<>
int compare<Integer>(Integer *value1, Integer *value2) {
  if (value1->value < value2->value) {
	return -1;
  } else if (value1->value > value2->value) {
	return 1;
  } else {
	return 0;
  }
}

// TODO: to make this completely oblivious, we would need to swap even if
// you don't need to! How to simulate this?
template<>
void compare_and_swap<Integer>(Integer *value1, Integer *value2) {
  int comp = compare<Integer>(value1, value2);

  if (comp == 1) {
	int temp = value1->value;
	value1->value = value2->value;
	value2->value = temp;
  }
}

template<>
int compare<String>(String *value1, String *value2) {
  // not yet implemented!
  assert(false);
}


template<>
void compare_and_swap<String>(String *value1, String *value2) {
  int comp = compare<String>(value1, value2);
  
  if (comp == 1) {
	uint32_t temp_len = value1->length;
	char *temp_buf = value1->buffer;
	value1->length = value2->length;
	value1->buffer = value2->buffer;
	value2->length = temp_len;
	value2->buffer = temp_buf;
  }
}


/** start Record functions **/
template<>
int compare<Record>(Record *value1, Record *value2) {
  uint8_t type1 = *(value1->sort_attribute);
  uint8_t type2 = *(value2->sort_attribute);

  //printf("Comparing %p and %p, type 1 is %u, type 2 is %u\n", value1->sort_attribute, value2->sort_attribute, type1, type2);

  assert(type1 == type2);

  switch(type1) {
  case 1:
	{
	  // Integer
	  Integer *int1 = (Integer *) (value1->sort_attribute + 5);
	  Integer *int2 = (Integer *) (value2->sort_attribute + 5);
	  return compare<Integer>(int1, int2);
	}
	break;
  case 2:
	{
	  // String
	  String *str1 = (String *) (value1->sort_attribute + 1);
	  String *str2 = (String *) (value2->sort_attribute + 1);
	  return compare<String>(str1, str2);
	}
	break;
  }
}

template<>
void compare_and_swap<Record>(Record *value1, Record *value2) {
  int comp = compare<Record>(value1, value2);
  
  if (comp == 1) {
	uint32_t num_cols = value1->num_cols;
	uint8_t *data = value1->data;
	uint8_t *sort_attribute = value1->sort_attribute;
	// swap
	value1->num_cols = value2->num_cols;
	value1->data = value2->data;
	value1->sort_attribute = value2->sort_attribute;
 	value2->num_cols = num_cols;
	value2->data = data;
	value2->sort_attribute = sort_attribute;

  }
}

/** end Record functions **/

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

	//printf("Must sort rows\n");

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
  }
}

// TODO: return encrypted random identifier
void ecall_random_id(uint8_t *ptr, uint32_t length) {
  sgx_status_t rand_status = sgx_read_rand(ptr, length);
}

// Aggregation
void ecall_scan_aggregation_count_distinct(int op_code,
										   uint8_t *input_rows, uint32_t input_rows_length,
										   uint32_t num_rows,
										   uint8_t *agg_row, uint32_t agg_row_buffer_length,
										   uint8_t *output_rows, uint32_t output_rows_length) {
  scan_aggregation_count_distinct(op_code, input_rows, input_rows_length, num_rows, agg_row, agg_row_buffer_length, output_rows, output_rows_length);
}

void ecall_test() {
  agg_test();
}
