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
#include <string.h>

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
void osort_with_index_int(int *input, int low_idx, uint32_t len) { }

// only sorts integers!
void ecall_oblivious_sort_int(int *input, uint32_t input_len) {
  osort_with_index_int(input, 0, input_len);
}


int qsort_cmp_sort(const void *a, const void *b) {
  return ( *((SortRecord **) a) )->compare(*((SortRecord **) b));
}

int qsort_cmp_join(const void *a, const void *b) {
  return ( *((JoinRecord **) a) )->compare( *((JoinRecord **) b) );
}

template<typename T>
void qsort_with_index(int op_code, T **input, uint32_t input_length, int low_idx, uint32_t len) { }

template<>
void qsort_with_index<SortRecord>(int op_code, SortRecord **input, uint32_t input_length, int low_idx, uint32_t len) {
  qsort(input, len, sizeof(SortRecord *), qsort_cmp_sort);
}

template<>
void qsort_with_index<JoinRecord>(int op_code, JoinRecord **input, uint32_t input_length, int low_idx, uint32_t len) {
  qsort(input, len, sizeof(JoinRecord *), qsort_cmp_join);
}

// input_length is the length of input in bytes
// len is the number of records
template<typename T>
void osort_with_index(int op_code, T **input, uint32_t input_length, int low_idx, uint32_t len) {

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

			  T *idx_value = input[idx];
			  T *pair_idx_value = input[pair_idx];

			  //idx_value->compare_and_swap(pair_idx_value);
			  if (idx_value->compare(pair_idx_value) == 1) {
				input[idx] = input[pair_idx];
				input[pair_idx] = idx_value;
			  }
			}
		  }
		}

      } else {	

		for (int i = offset; i <= (offset + len - 1); i += part_size) {
		  for (int j = 1; j <= part_size_half; j++) {
			int idx = i + j - 1;
			int pair_idx = idx + part_size_half;

			if (pair_idx < offset + len) {
			  T *idx_value = input[idx];
			  T *pair_idx_value = input[pair_idx];

			  //idx_value->compare_and_swap(pair_idx_value);
			  if (idx_value->compare(pair_idx_value) == 1) {
				input[idx] = input[pair_idx];
				input[pair_idx] = idx_value;
			  }
			}

		  }
		}

      }

    }
  }

}

// merges together two sorted arrays non-obliviously, in place
template<typename T>
void non_oblivious_merge(int op_code, T **input, uint32_t input_length, int offset, uint32_t len) {

  T **temp = (T **) malloc(sizeof(T *) * len);

  // initialize the two merge pointers
  uint32_t left_ptr = 0;
  uint32_t right_ptr = offset;

  for (uint32_t i = 0; i < len; i++) {

  	//printf("len: %u, left_ptr: %u, right_ptr: %u\n", len, left_ptr, right_ptr);
  	if (left_ptr == offset) {
  	  temp[i] = input[right_ptr++];
  	} else if (right_ptr == len) {
  	  temp[i] = input[left_ptr++];
  	} else {
	
  	  T *left_item = input[left_ptr];
  	  T *right_item = input[right_ptr];
	  
  	  int ret = left_item->compare(right_item);
	  
  	  //printf("ret: %d, left_ptr: %u, right_ptr: %u, offset: %u, len is %u\n", ret, left_ptr, right_ptr, offset, len);
  	  if (ret == -1 || ret == 0) {
  		temp[i] = left_item;
  		left_ptr += 1;
  	  } else if (ret == 1) {
  		temp[i] = right_item;
  		right_ptr += 1;
  	  }
  	}

  }

  //printf("Merge done\n");

  //change ordering of input

  for (uint32_t i = 0; i < len; i++) {
  	input[i] = temp[i];
  	//printf("input[%u] is %p\n", i, input[i]);
  }

  free(temp);
}

void oblivious_sort(int op_code, BufferReader *reader,
					int low_idx, uint32_t list_length,
					bool if_sort,
					SortRecord **sort_data,
					JoinRecord **join_data) {
  
  // iterate through all data, and then decrypt

  if (op_code == OP_SORT_INTEGERS_TEST) {
    // Sorting integers (test only)
	Integer **data = (Integer **) malloc(sizeof(Integer *) * list_length);
	if (data == NULL) {
	  printf("data is NULL!\n");
	}

	uint8_t *input_ptr = reader->get_ptr();
	uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *enc_value_ptr = NULL;
	uint32_t enc_value_len = 0;

	for (uint32_t i = 0; i < list_length; i++) {
	  get_next_value(&input_ptr, &enc_value_ptr, &enc_value_len);
	  data[i] = new Integer;
	  
	  decrypt(enc_value_ptr, enc_value_len, (uint8_t *) (&(data[i]->value)));
	}

	osort_with_index<Integer>(op_code, data, sizeof(Integer) * list_length, low_idx, list_length);

	// need to re-encrypt the data!
	// since none of the data has changed, let's try re-using the input

	uint32_t enc_data_size = enc_size(4);
	reader->reset();
	input_ptr = reader->get_ptr();
	data_ptr = (uint8_t *) data;

	uint32_t *size_ptr = (uint32_t *) data_ptr;

	for (uint32_t i = 0; i < list_length; i++) {
	  size_ptr = (uint32_t *) input_ptr;
	  *size_ptr = enc_data_size;

	  encrypt(( (uint8_t *) &(data[i]->value)), 4, input_ptr + 4);

	  input_ptr += enc_data_size + 4;
	}

	for (uint32_t i = 0; i < list_length; i++) {
	  delete data[i];
	}

	free(data);

  } else if (op_code == OP_SORT_COL1 || op_code == OP_SORT_COL2 || op_code == OP_SORT_COL4_IS_DUMMY_COL2) {
    // Sorting rows
	// this needs to sort a row of data
	
	//printf("op_code called is %u\n", op_code);
	
    //SortRecord **data = (SortRecord **) malloc(sizeof(SortRecord *) * list_length);
	if (sort_data == NULL) {
	  printf("Could not allocate enough data\n");
	}

	reader->reset();
	uint8_t *input_ptr = reader->get_ptr();
	//uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *enc_row_ptr = NULL;
	uint32_t enc_row_len = 0;

	uint8_t *enc_value_ptr = NULL;
	uint32_t enc_value_len = 0;
 	uint8_t *value_ptr = NULL;
	uint32_t value_len = 0;

	uint8_t *data_ptr_ = NULL;

	for (uint32_t i = 0; i < list_length; i++) {
	  //printf("list length is %u\n", list_length);

	  SortRecord *rec = sort_data[i];
			
	  if (if_sort) {
		input_ptr = reader->get_ptr();
		get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);
		
		// // for testing
		// enc_row_ptr = input_ptr;
		// enc_row_len = 1 + 4 + 4 + 1 + 4 + STRING_UPPER_BOUND + 1 + 4 + 4;
		// input_ptr += 4 + enc_row_len;
		// // end testing
		
		reader->inc_ptr(input_ptr);
		
		//data[i] = new SortRecord;
		rec->reset();
		
		uint32_t columns = *( (uint32_t *) enc_row_ptr);
		//printf("num cols is %u\n", columns);
		
		enc_row_ptr += 4;
		enc_value_ptr = enc_row_ptr;
		
		// iterate through encrypted attributes,
		for (uint32_t j = 0; j < columns; j++) {
		  assert(enc_value_ptr <= input_ptr);
		  // [enc len]encrypted{[value type][value len][value]}
		  rec->consume_encrypted_attribute(&enc_value_ptr);

		  // // for testing
		  // rec->row_ptr = rec->row;
		  // cpy(rec->row_ptr, enc_value_ptr, enc_row_len);
		  // rec->row_ptr += enc_row_len;
		  // rec->num_cols += 1;
		  // // end testing
		}
	  }
		
	  rec->set_sort_attributes(op_code);
	}

	if (if_sort) {
	  qsort_with_index<SortRecord>(op_code, sort_data, sizeof(SortRecord *) * list_length, 0, list_length);
	} else {
	  non_oblivious_merge<SortRecord>(op_code, sort_data, sizeof(SortRecord *) * list_length, low_idx, list_length);
	}

	//printf("Sorted data\n");

	if (false) {
	  reader->reset();
	  input_ptr = reader->get_ptr();
	  value_ptr = NULL;
	  value_len = 0;
	  uint8_t *test_ptr = NULL;

	  for (uint32_t i = 0; i < list_length; i++) {
		//data[i]->sort_attributes->print();
	  
		value_ptr = sort_data[i]->row;
		input_ptr = reader->get_ptr();
	  
		//test_ptr = input_ptr;
		*( (uint32_t *) input_ptr) = sort_data[i]->num_cols;
		input_ptr += 4;

		// need to encrypt each attribute separately
		for (uint32_t c = 0; c < sort_data[i]->num_cols; c++) {
		  encrypt_attribute(&value_ptr, &input_ptr);

		  // // for testing only
		  // cpy(input_ptr, value_ptr, 1 + 4 + 4 + 1 + 4 + STRING_UPPER_BOUND + 1 + 4 + 4);
		  // // end testing
		}

		//printf("encrypted row's length is %u\n", (input_ptr - test_ptr));
		reader->inc_ptr(input_ptr);
	  }
	}
	
  } else if (op_code == OP_JOIN_COL2) {
    // Sorting for Join

	//printf("join sort, op_code is %u\n", op_code);
	
	// get the primary/foreign table indicators
	uint8_t primary_table[TABLE_ID_SIZE];
	uint8_t foreign_table[TABLE_ID_SIZE];
	get_table_indicator(primary_table, foreign_table);
	
	// this is a sort used for sort-merge join!
	// that means there are two different join attributes:
	// one for the primary key table, the other the foreign key table
	
    //JoinRecord **data = (JoinRecord **) malloc(sizeof(JoinRecord *) * list_length);
	if (join_data == NULL) {
	  printf("Could not allocate enough data\n");
	  assert(false);
	}

	// allocate memory for record
	reader->reset();
	uint8_t *input_ptr = reader->get_ptr();
	//uint8_t *data_ptr = (uint8_t *) data;

	uint8_t *data_ptr_ = NULL;
	uint8_t *table_id = NULL;
	uint8_t *value_ptr = NULL;
	uint32_t value_len = 0;
	
	for (uint32_t i = 0; i < list_length; i++) {
	  // allocate JOIN_ROW_UPPER_BOUND
	  //JoinRecord *record = &(data[i].record);
	  //record->data = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND);

	  input_ptr = reader->get_ptr();

	  //data[i] = new JoinRecord;
	  JoinRecord *record = join_data[i];
	  record->reset();
	  
	  record->consume_encrypted_row(input_ptr);
	  input_ptr += enc_size(JOIN_ROW_UPPER_BOUND);

	  record->set_join_attributes(op_code);

	  //record->join_attributes->init();
	  //record->join_attributes->evaluate();
	  //record->join_attributes->print();

	  reader->inc_ptr(input_ptr);
	}
	
	if (if_sort) {
	  osort_with_index<JoinRecord>(op_code, join_data, sizeof(JoinRecord *) * list_length, 0, list_length);
	} else {
	  non_oblivious_merge<JoinRecord>(op_code, join_data, sizeof(JoinRecord *) * list_length, low_idx, list_length);
	}

	//osort_with_index<JoinRecord>(op_code, data, sizeof(JoinRecord *) * list_length, low_idx, list_length);

	// Produce encrypted output rows
	reader->reset();
	input_ptr = reader->get_ptr();
	for (uint32_t i = 0; i < list_length; i++) {
	  // simply encrypt the entire row all at once
	  //print_join_row("Join row ", data[i]->row);
	  input_ptr = reader->get_ptr();
	  
	  encrypt(join_data[i]->row, JOIN_ROW_UPPER_BOUND, input_ptr);
	  input_ptr += enc_size(JOIN_ROW_UPPER_BOUND);

	  reader->inc_ptr(input_ptr);
	}

	// // free data
	// for (uint32_t i = 0; i < list_length; i++) {
	//   free(data[i]);
	// }

	// free(data);
  } else {
    printf("ecall_oblivious_sort: unknown opcode %d\n", op_code);
    assert(false);
  }
}

// this uses the oblivious sort on equi-sized buffers
// this should be in row format (either sort_row, or join row)
// also: assume that all rows are equi-sized!
void ecall_external_oblivious_sort(int op_code,
								   uint32_t num_buffers,
								   uint8_t **buffer_list, 
								   uint32_t *buffer_lengths,
								   uint32_t *num_rows,
								   uint8_t *external_scratch) {

  //uint8_t *internal_buffer = (uint8_t *) malloc(MAX_SORT_BUFFER);

  // First, iterate through and decrypt the data
  // Then store the decrypted data in a list of objects (based on the given op_code)

  int len = num_buffers;
  int log_len = log_2(len) + 1;
  int offset = 0;

  int swaps = 0;
  int min_val = 0;
  int max_val = 0;

  //printf("External sort called\n");

  BufferReader reader;
  reader.clear();

  SortRecord **sort_data = NULL;
  JoinRecord **join_data = NULL;
  
  uint32_t max_list_length = num_rows[0];
  if (num_buffers > 1) {
	max_list_length += num_rows[1];
  }

  if (op_code == OP_SORT_COL1 || op_code == OP_SORT_COL2 || op_code == OP_SORT_COL4_IS_DUMMY_COL2) {
	sort_data = (SortRecord **) malloc(sizeof(SortRecord *) * max_list_length);
	for (uint32_t i = 0; i < max_list_length; i++) {
	  sort_data[i] = new SortRecord;
	}
  } else {
	join_data = (JoinRecord **) malloc(sizeof(JoinRecord *) * max_list_length);
	for (uint32_t i = 0; i < max_list_length; i++) {
	  join_data[i] = new JoinRecord;	  
	}
  }

  printf("max_list_length is %u\n", max_list_length);

  if (num_buffers == 1) {
	//assert(buffer_lengths[0] < MAX_SORT_BUFFER);
	printf("num buffers is 1\n");
	reader.add_buffer(buffer_list[0], buffer_lengths[0]);
	oblivious_sort(op_code, &reader, 0, num_rows[0], true, sort_data, join_data);

	if (sort_data != NULL) {
	  for (uint32_t i = 0; i < max_list_length; i++) {
		free(sort_data[i]);
	  }
	
	  free(sort_data);
	}

	if (join_data != NULL) {
	  for (uint32_t i = 0; i < max_list_length; i++) {
		free(join_data[i]);
	  }
	
	  free(join_data);
	}
	return;
  }

  uint8_t * scratch = (uint8_t *) malloc(MAX_SORT_BUFFER);
  uint8_t * scratch_ptr = scratch;
  uint8_t * external_scratch_ptr = external_scratch;
  
  uint8_t *external_scratch_list[128];
  uint32_t external_scratch_size[128];
  uint32_t row_size = 0;
  uint32_t padded_row_size = 0;
  uint32_t attr_len = 0;
  uint8_t *sort_data_ptr = NULL;

  printf("Start single partition oblivious sort, num buffers: %u\n", num_buffers);
  for (uint32_t i = 0; i < num_buffers; i++) {
  	reader.clear();
  	reader.reset();
  	reader.add_buffer(buffer_list[i], buffer_lengths[i]);
  	oblivious_sort(op_code, &reader, 0, num_rows[i], true, sort_data, join_data);
	
	scratch_ptr = scratch;
	// flush sort records/join records to ptr, then encrypt that all at once
	for (uint32_t r = 0; r < num_rows[i]; r++) {	  
	  *( (uint32_t *) scratch_ptr) = sort_data[r]->num_cols;
	  scratch_ptr += 4;
	  
	  row_size = sort_data[r]->row_ptr - sort_data[r]->row;
	  memcpy(scratch_ptr, sort_data[r]->row, row_size);
	  scratch_ptr += row_size;
	}

	// TODO: since rows should have the same content, take a look at row 0 and find the padded_row_size
	// hard-coded for now
	padded_row_size = 4;
	attr_len = 0;
	sort_data_ptr = sort_data[0]->row;
	for (uint32_t col = 0; col < sort_data[0]->num_cols; col++) {
	  attr_len = *( (uint32_t *) (sort_data_ptr + TYPE_SIZE));
	  padded_row_size += HEADER_SIZE + attr_upper_bound(sort_data_ptr);
	  sort_data_ptr += HEADER_SIZE + attr_len;
	}
	
	//padded_row_size = 4 + (HEADER_SIZE + INT_UPPER_BOUND) * 2 + (HEADER_SIZE + STRING_UPPER_BOUND);
	printf("padded_row_size is %u\n", padded_row_size);

	// there needs to be padding
	encrypt(scratch, padded_row_size * num_rows[i], external_scratch_ptr);
	
	external_scratch_list[i] = external_scratch_ptr;
	external_scratch_size[i] = enc_size(padded_row_size * num_rows[i]);

	external_scratch_ptr += enc_size(padded_row_size * num_rows[i]);
  }
  printf("End single partition oblivious sort\n");


  uint32_t merges = 0;
  uint8_t *row_ptr = NULL;
  uint32_t row_len = 0;
  
  for (int stage = 1; stage <= log_len; stage++) {

    for (int stage_i = stage; stage_i >= 1; stage_i--) {

      int part_size = pow_2(stage_i);
      int part_size_half = part_size / 2;

      if (stage_i == stage) {
		for (int i = offset; i <= (offset + len - 1); i += part_size) {
   		  for (int j = 1; j <= part_size_half; j++) {
			int idx = i + j - 1;
			int pair_idx = i + part_size - j;

			if (pair_idx < offset + len) {

			  // find two pointers to these buffers
			  uint8_t *buffer1_ptr = external_scratch_list[idx];
			  uint8_t *buffer2_ptr = external_scratch_list[pair_idx];

			  uint32_t buffer1_size = external_scratch_size[idx];
			  uint32_t buffer2_size = external_scratch_size[pair_idx];

			  //printf("external scratch sizes are %u and %u\n", external_scratch_size[idx], external_scratch_size[pair_idx]);

			  // if ((buffer1_size + buffer2_size) > MAX_SORT_BUFFER) {
			  // 	printf("assert failed\n");
			  // 	assert((buffer1_size + buffer2_size) <= MAX_SORT_BUFFER);
			  // }
			  
			  // sort these two buffers
			  reader.clear();
			  reader.add_buffer(buffer1_ptr, buffer1_size);
			  reader.add_buffer(buffer2_ptr, buffer2_size);

			  // note that these are padded!
			  decrypt(buffer1_ptr, buffer1_size, scratch);
			  scratch_ptr = scratch;
			  for (uint32_t r = 0; r < num_rows[idx]; r++) {
				get_next_plaintext_row(&scratch_ptr, &row_ptr, &row_len);
				// set num cols first
				sort_data[r]->num_cols = *( (uint32_t *) (row_ptr));
				memcpy(sort_data[r]->row, row_ptr + 4, row_len - 4);
				sort_data[r]->row_ptr = sort_data[r]->row + row_len - 4;
			  }

			  decrypt(buffer2_ptr, buffer2_size, scratch);
			  scratch_ptr = scratch;
			  for (uint32_t r = num_rows[idx]; r < num_rows[idx] + num_rows[pair_idx]; r++) {	
				get_next_plaintext_row(&scratch_ptr, &row_ptr, &row_len);
				// set num cols first
				sort_data[r]->num_cols = *( (uint32_t *) (row_ptr));
				memcpy(sort_data[r]->row, row_ptr + 4, row_len - 4);
				sort_data[r]->row_ptr = sort_data[r]->row + row_len - 4;
			  }

			  oblivious_sort(op_code, &reader,
			  				 num_rows[idx], num_rows[idx] + num_rows[pair_idx],
			  				 false,
			  				 sort_data, join_data);

			  // flush data out to scratch, encrypt
			  scratch_ptr = scratch;
			  for (uint32_t r = 0; r < num_rows[idx]; r++) {
				*( (uint32_t *) scratch_ptr) = sort_data[r]->num_cols;
				scratch_ptr += 4;

				//print_row("after merging", sort_data[r]->row, sort_data[r]->num_cols);
				
				row_size = sort_data[r]->row_ptr - sort_data[r]->row;
				memcpy(scratch_ptr, sort_data[r]->row, row_size);
				scratch_ptr += row_size;
			  }
			  encrypt(scratch, dec_size(buffer1_size), buffer1_ptr);
			  
			  scratch_ptr = scratch;
			  for (uint32_t r = num_rows[idx]; r < num_rows[idx] + num_rows[pair_idx]; r++) {
				*( (uint32_t *) scratch_ptr) = sort_data[r]->num_cols;
				scratch_ptr += 4;
				
				row_size = sort_data[r]->row_ptr - sort_data[r]->row;
				memcpy(scratch_ptr, sort_data[r]->row, row_size);
				scratch_ptr += row_size;
			  }
			  encrypt(scratch, dec_size(buffer2_size), buffer2_ptr);
			  
			  ++merges;
			}
		  }
		}

      } else {	

		for (int i = offset; i <= (offset + len - 1); i += part_size) {
		  for (int j = 1; j <= part_size_half; j++) {
			int idx = i + j - 1;
			int pair_idx = idx + part_size_half;

			if (pair_idx < offset + len) {
			  
			  // find two pointers to these buffers
			  uint8_t *buffer1_ptr = external_scratch_list[idx];
			  uint8_t *buffer2_ptr = external_scratch_list[pair_idx];

			  uint32_t buffer1_size = external_scratch_size[idx];
			  uint32_t buffer2_size = external_scratch_size[pair_idx];

			  reader.clear();
			  reader.add_buffer(buffer1_ptr, buffer1_size);
			  reader.add_buffer(buffer2_ptr, buffer2_size);

			  // note that these are padded!
			  decrypt(buffer1_ptr, buffer1_size, scratch);
			  scratch_ptr = scratch;
			  for (uint32_t r = 0; r < num_rows[idx]; r++) {
				get_next_plaintext_row(&scratch_ptr, &row_ptr, &row_len);
				// set num cols first
				sort_data[r]->num_cols = *( (uint32_t *) (row_ptr));
				memcpy(sort_data[r]->row, row_ptr + 4, row_len - 4);
				sort_data[r]->row_ptr = sort_data[r]->row + row_len - 4;
			  }

			  decrypt(buffer2_ptr, buffer2_size, scratch);
			  scratch_ptr = scratch;
			  for (uint32_t r = num_rows[idx]; r < num_rows[idx] + num_rows[pair_idx]; r++) {	
				get_next_plaintext_row(&scratch_ptr, &row_ptr, &row_len);
				// set num cols first
				sort_data[r]->num_cols = *( (uint32_t *) (row_ptr));
				memcpy(sort_data[r]->row, row_ptr + 4, row_len - 4);
				sort_data[r]->row_ptr = sort_data[r]->row + row_len - 4;			
			  }

			  oblivious_sort(op_code, &reader,
			  				 num_rows[idx], num_rows[idx] + num_rows[pair_idx],
			  				 false,
			  				 sort_data, join_data);

			  // flush data out to scratch, encrypt
			  scratch_ptr = scratch;
			  for (uint32_t r = 0; r < num_rows[idx]; r++) {
				*( (uint32_t *) scratch_ptr) = sort_data[r]->num_cols;
				scratch_ptr += 4;
				
				row_size = sort_data[r]->row_ptr - sort_data[r]->row;
				memcpy(scratch_ptr, sort_data[r]->row, row_size);
				scratch_ptr += row_size;
			  }
			  encrypt(scratch, dec_size(buffer1_size), buffer1_ptr);

			  
			  scratch_ptr = scratch;
			  for (uint32_t r = num_rows[idx]; r < num_rows[idx] + num_rows[pair_idx]; r++) {
				*( (uint32_t *) scratch_ptr) = sort_data[r]->num_cols;
				scratch_ptr += 4;
				
				row_size = sort_data[r]->row_ptr - sort_data[r]->row;
				memcpy(scratch_ptr, sort_data[r]->row, row_size);
				scratch_ptr += row_size;
			  }
			  encrypt(scratch, dec_size(buffer2_size), buffer2_ptr);
			  
			  ++merges;
			}

		  }
		}

      }

    }
  }

  uint8_t *input_ptr = NULL;
  uint8_t *value_ptr = NULL;
  uint32_t value_len = 0;
  // write out each encrypted row
  for (uint32_t i = 0; i < num_buffers; i++) {
  	reader.clear();
  	reader.reset();
  	reader.add_buffer(buffer_list[i], buffer_lengths[i]);
	
  	// note that these are padded!
  	decrypt(external_scratch_list[i], external_scratch_size[i], scratch);
  	scratch_ptr = scratch;
  	for (uint32_t r = 0; r < num_rows[i]; r++) {
  	  get_next_plaintext_row(&scratch_ptr, &row_ptr, &row_len);
  	  // set num cols first
  	  sort_data[r]->num_cols = *( (uint32_t *) (row_ptr));
  	  memcpy(sort_data[r]->row, row_ptr + 4, row_len - 4);
  	  sort_data[r]->row_ptr = sort_data[r]->row + row_len - 4;
  	}

  	reader.reset();
  	input_ptr = reader.get_ptr();
  	value_ptr = NULL;
  	value_len = 0;
  	uint8_t *test_ptr = NULL;

  	for (uint32_t r = 0; r < num_rows[i]; r++) {
  	  value_ptr = sort_data[r]->row;
  	  input_ptr = reader.get_ptr();
	  
  	  //test_ptr = input_ptr;
  	  *( (uint32_t *) input_ptr) = sort_data[r]->num_cols;
  	  input_ptr += 4;

  	  // need to encrypt each attribute separately
  	  for (uint32_t c = 0; c < sort_data[i]->num_cols; c++) {
  		encrypt_attribute(&value_ptr, &input_ptr);
  	  }
  	  reader.inc_ptr(input_ptr);
  	}
  }

  printf("number of merges: %u\n", merges);

  free(scratch);

  // free data
  
  if (sort_data != NULL) {
	for (uint32_t i = 0; i < max_list_length; i++) {
	  free(sort_data[i]);
	}
	
	free(sort_data);
  }

  if (join_data != NULL) {
	for (uint32_t i = 0; i < max_list_length; i++) {
	  free(join_data[i]);
	}
	
	free(join_data);
  }

}

// TODO: return encrypted random identifier
void ecall_random_id(uint8_t *ptr, uint32_t length) {
  sgx_status_t rand_status = sgx_read_rand(ptr, length);
}

void ecall_test() {
  uint8_t x[1024 * 1024];
  assert(sgx_is_within_enclave(x, 1024 * 1024) == 1);

  uint32_t malloc_length = 1024 * 1024;
  uint32_t malloc_size = 1024;

  for (uint32_t i = 0; i < malloc_length; i++) {
	malloc(malloc_size);
  }
  
  uint8_t *ptr = (uint8_t *) malloc(malloc_size);
  printf("Malloc within enclave? %u\n", sgx_is_within_enclave(ptr, malloc_size));
  free(ptr);
}

/**** BEGIN Aggregation ****/
void ecall_scan_aggregation_count_distinct(int op_code,
										   uint8_t *input_rows, uint32_t input_rows_length,
										   uint32_t num_rows,
										   uint8_t *agg_row, uint32_t agg_row_buffer_length,
										   uint8_t *output_rows, uint32_t output_rows_length,
										   uint32_t *actual_size,
										   int flag, uint32_t *cardinality) {
  scan_aggregation_count_distinct(op_code,
								  input_rows, input_rows_length, num_rows,
								  agg_row, agg_row_buffer_length,
								  output_rows, output_rows_length,
								  actual_size,
								  flag, cardinality);
}

void ecall_process_boundary_records(int op_code,
									uint8_t *rows, uint32_t rows_size,
									uint32_t num_rows,
									uint8_t *out_agg_rows, uint32_t out_agg_row_size,
									uint32_t *actual_out_agg_row_size) {

  process_boundary_records(op_code,
						   rows, rows_size,
						   num_rows,
						   out_agg_rows, out_agg_row_size,
						   actual_out_agg_row_size);
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
  // printf("Enc join row size is %u\n", enc_size(JOIN_ROW_UPPER_BOUND));
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
                           uint8_t *output_rows, uint32_t output_rows_length,
                           uint32_t *actual_output_length) {
  
  sort_merge_join(op_code,
				  input_rows, input_rows_length, num_rows,
				  join_row, join_row_length,
                  output_rows, output_rows_length,
                  actual_output_length);
}

/**** END Join ****/

void ecall_encrypt_attribute(uint8_t *input, uint32_t input_size,
							 uint8_t *output, uint32_t output_size,
							 uint32_t *actual_size) {
  uint8_t *input_ptr = input;
  uint8_t *output_ptr = output;

  encrypt_attribute(&input_ptr, &output_ptr);
  *actual_size = (output_ptr - output);
}
