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

/* 
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

const char *key_str = "helloworld123123";
const sgx_aes_gcm_128bit_key_t *key = (const sgx_aes_gcm_128bit_key_t *) key_str;

void printf(const char *fmt, ...)
{
    char buf[BUFSIZ] = {'\0'};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_print_string(buf);
}

void print_bytes(uint8_t *ptr, uint32_t len) {
  
  for (int i = 0; i < len; i++) {
    printf("%u", *(ptr + i));
    printf(" - ");
  }

  printf("\n");
}


// encrypt() and decrypt() should be called from enclave code only

// encrypt using a global key
// TODO: fix this; should use key obtained from client 
void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
			 uint8_t *ciphertext) {
  
  // key size is 12 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint8_t *iv_ptr = ciphertext;
  // generate random IV
  sgx_read_rand(iv_ptr, SGX_AESGCM_IV_SIZE);
  
  sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;

  //sgx_status_t rand_status = sgx_read_rand(iv, SGX_AESGCM_IV_SIZE);
  sgx_status_t status = sgx_rijndael128GCM_encrypt(key,
												   plaintext, plaintext_length,
												   ciphertext_ptr,
												   iv_ptr, SGX_AESGCM_IV_SIZE,
												   NULL, 0,
												   mac_ptr);
  switch(status) {
  case SGX_ERROR_INVALID_PARAMETER:
    break;
  case SGX_ERROR_OUT_OF_MEMORY:
    break;
  case SGX_ERROR_UNEXPECTED:
    break;
  }

  assert(status == SGX_SUCCESS);
}


void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, 
			 uint8_t *plaintext) {

  // decrypt using a global key
  // TODO: fix this; should use key obtained from client 
  
  // key size is 12 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint32_t plaintext_length = ciphertext_length - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;

  uint8_t *iv_ptr = (uint8_t *) ciphertext;
  sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  uint8_t *ciphertext_ptr = (uint8_t *) (ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);


  sgx_status_t status = sgx_rijndael128GCM_decrypt(key,
												   ciphertext_ptr, plaintext_length,
												   plaintext,
												   iv_ptr, SGX_AESGCM_IV_SIZE,
												   NULL, 0,
												   mac_ptr);

  if (status != SGX_SUCCESS) {
    switch(status) {
    case SGX_ERROR_INVALID_PARAMETER:
      printf("Decrypt: invalid parameter\n");
      break;

    case SGX_ERROR_OUT_OF_MEMORY:
      printf("Decrypt: out of enclave memory\n");
      break;

    case SGX_ERROR_UNEXPECTED:
      printf("Decrypt: unexpected error\n");
      break;

    case SGX_ERROR_MAC_MISMATCH:
      printf("Decrypt: MAC mismatch\n");
      break;

    default:
      printf("Decrypt: other error %#08x\n", status);
    }
  }
}

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


// -1 means value1 < value2
// 0 means value1 == value2
// 1 means value1 > value2
template<typename T>
int compare(T *value1, T *value2) {
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

template<>
int compare<String>(String *value1, String *value2) {
  // not yet implemented!
  assert(false);
}

template<typename T>
void compare_and_swap(T *value1, T* value2) {
  assert(false);
 
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

void get_next_row(uint8_t **ptr, uint8_t **enc_value_ptr, uint32_t *enc_value_len) {

  *enc_value_len = *( (uint32_t *)(*ptr) );
  *enc_value_ptr = *ptr + 4;

  *ptr = *ptr + 4 + *enc_value_len;
  
}

// TODO: format of this input array?
void ecall_oblivious_sort(int op_code, uint8_t *input, uint32_t buffer_length,
						  int low_idx, uint32_t list_length) {
  // iterate through all data, and then decrypt

  // op_code = 1 means it's a list of integers
  if (op_code == 1) {
	Integer * data = (Integer *) malloc(sizeof(Integer) * list_length);

	uint8_t *input_ptr = input;
	uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *enc_value_ptr = NULL;
	uint32_t enc_value_len = 0;

	while (1) {
	  get_next_row(&input_ptr, &enc_value_ptr, &enc_value_len);

	  if (input_ptr == NULL) {
		break;
	  }
	  
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
	
  } else {

  }
}
