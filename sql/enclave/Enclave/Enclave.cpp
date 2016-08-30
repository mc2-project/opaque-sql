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

void ecall_external_oblivious_sort(int op_code, uint32_t num_buffers, uint8_t **buffer_list,
                                   uint32_t *num_rows, uint32_t row_upper_bound) {

  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
    external_oblivious_sort<NewRecord>(
      op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
    break;
  case SORT_JOIN:
    external_oblivious_sort<NewJoinRecord>(
      op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
    break;
  default:
    printf("ecall_external_oblivious_sort: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }
}

void ecall_project(int op_code,
                   uint8_t *input_rows, uint32_t input_rows_length,
                   uint32_t num_rows,
                   uint8_t *output_rows, uint32_t output_rows_length,
                   uint32_t *actual_output_rows_length) {

  project(
    op_code, input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
    actual_output_rows_length);
}

void ecall_filter(int op_code,
                  uint8_t *input_rows, uint32_t input_rows_length,
                  uint32_t num_rows,
                  uint8_t *output_rows, uint32_t output_rows_length,
                  uint32_t *actual_output_rows_length, uint32_t *num_output_rows) {

  filter(
    op_code, input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
    actual_output_rows_length, num_output_rows);
}

/**** BEGIN Aggregation ****/
void ecall_aggregate_step1(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_length,
                           uint32_t *actual_size) {
  switch (op_code) {
  case OP_GROUPBY_COL1_SUM_COL2_INT_STEP1:
    aggregate_step1<Aggregator1<GroupBy<1>, Sum<2, uint32_t> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size);
    break;
  case OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1:
    aggregate_step1<Aggregator1<GroupBy<1>, Sum<2, float> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size);
    break;
  case OP_GROUPBY_COL2_SUM_COL3_INT_STEP1:
    aggregate_step1<Aggregator1<GroupBy<2>, Sum<3, uint32_t> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size);
    break;
  case OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1:
    aggregate_step1<Aggregator2<GroupBy<1>, Avg<2, uint32_t>, Sum<3, float> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size);
    break;
  case OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP1:
    aggregate_step1<Aggregator1<GroupBy2<1, 2>, Sum<3, float> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size);
    break;
  default:
    printf("ecall_aggregate_step1: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void ecall_process_boundary_records(int op_code,
                                    uint8_t *rows, uint32_t rows_size,
                                    uint32_t num_rows,
                                    uint8_t *out_agg_rows, uint32_t out_agg_row_size,
                                    uint32_t *actual_out_agg_row_size) {
  switch (op_code) {
  case OP_GROUPBY_COL1_SUM_COL2_INT_STEP1:
    aggregate_process_boundaries<Aggregator1<GroupBy<1>, Sum<2, uint32_t> > >(
      rows, rows_size, num_rows, out_agg_rows, out_agg_row_size,
      actual_out_agg_row_size);
    break;
  case OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1:
    aggregate_process_boundaries<Aggregator1<GroupBy<1>, Sum<2, float> > >(
      rows, rows_size, num_rows, out_agg_rows, out_agg_row_size,
      actual_out_agg_row_size);
    break;
  case OP_GROUPBY_COL2_SUM_COL3_INT_STEP1:
    aggregate_process_boundaries<Aggregator1<GroupBy<2>, Sum<3, uint32_t> > >(
      rows, rows_size, num_rows, out_agg_rows, out_agg_row_size,
      actual_out_agg_row_size);
    break;
  case OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1:
    aggregate_process_boundaries<Aggregator2<GroupBy<1>, Avg<2, uint32_t>, Sum<3, float> > >(
      rows, rows_size, num_rows, out_agg_rows, out_agg_row_size,
      actual_out_agg_row_size);
    break;
  case OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP1:
    aggregate_process_boundaries<Aggregator1<GroupBy2<1, 2>, Sum<3, float> > >(
      rows, rows_size, num_rows, out_agg_rows, out_agg_row_size,
      actual_out_agg_row_size);
    break;
  default:
    printf("ecall_process_boundary_records: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void ecall_aggregate_step2(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *boundary_info_row_ptr, uint32_t boundary_info_row_length,
                           uint8_t *output_rows, uint32_t output_rows_length,
                           uint32_t *actual_size) {
  switch (op_code) {
  case OP_GROUPBY_COL1_SUM_COL2_INT_STEP2:
    aggregate_step2<Aggregator1<GroupBy<1>, Sum<2, uint32_t> > >(
      input_rows, input_rows_length, num_rows, boundary_info_row_ptr, boundary_info_row_length,
      output_rows, output_rows_length, actual_size);
    break;
  case OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2:
    aggregate_step2<Aggregator1<GroupBy<1>, Sum<2, float> > >(
      input_rows, input_rows_length, num_rows, boundary_info_row_ptr, boundary_info_row_length,
      output_rows, output_rows_length, actual_size);
    break;
  case OP_GROUPBY_COL2_SUM_COL3_INT_STEP2:
    aggregate_step2<Aggregator1<GroupBy<2>, Sum<3, uint32_t> > >(
      input_rows, input_rows_length, num_rows, boundary_info_row_ptr, boundary_info_row_length,
      output_rows, output_rows_length, actual_size);
    break;
  case OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP2:
    aggregate_step2<Aggregator2<GroupBy<1>, Avg<2, uint32_t>, Sum<3, float> > >(
      input_rows, input_rows_length, num_rows, boundary_info_row_ptr, boundary_info_row_length,
      output_rows, output_rows_length, actual_size);
    break;
  case OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP2:
    aggregate_step2<Aggregator1<GroupBy2<1, 2>, Sum<3, float> > >(
      input_rows, input_rows_length, num_rows, boundary_info_row_ptr, boundary_info_row_length,
      output_rows, output_rows_length, actual_size);
    break;
  default:
    printf("ecall_aggregate_step2: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

/**** END Aggregation ****/

/**** BEGIN Join ****/

void ecall_join_sort_preprocess(int op_code,
                                uint8_t *primary_rows, uint32_t primary_rows_len,
                                uint32_t num_primary_rows,
                                uint8_t *foreign_rows, uint32_t foreign_rows_len,
                                uint32_t num_foreign_rows,
                                uint8_t *output_rows, uint32_t output_rows_len,
                                uint32_t *actual_output_len) {
  (void)op_code;
  join_sort_preprocess(
    primary_rows, primary_rows_len, num_primary_rows,
    foreign_rows, foreign_rows_len, num_foreign_rows,
    output_rows, output_rows_len, actual_output_len);
}

void ecall_scan_collect_last_primary(int op_code,
                                     uint8_t *input_rows, uint32_t input_rows_length,
                                     uint32_t num_rows,
                                     uint8_t *output, uint32_t output_length,
                                     uint32_t *actual_output_len) {
  scan_collect_last_primary(op_code,
                            input_rows, input_rows_length,
                            num_rows,
                            output, output_length, actual_output_len);
}

void ecall_process_join_boundary(int op_code,
                                 uint8_t *input_rows, uint32_t input_rows_length,
                                 uint32_t num_rows,
                                 uint8_t *output_rows, uint32_t output_rows_size,
                                 uint32_t *actual_output_length) {

  process_join_boundary(op_code, input_rows, input_rows_length,
                        num_rows,
                        output_rows, output_rows_size, actual_output_length);

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
  (void)input_size;
  (void)output_size;

  uint8_t *input_ptr = input;
  uint8_t *output_ptr = output;

  encrypt_attribute(&input_ptr, &output_ptr);
  *actual_size = (output_ptr - output);
}

template<typename RecordType>
void create_block(
  uint8_t *rows, uint32_t rows_len, uint32_t num_rows,
  uint8_t *block, uint32_t block_len, uint32_t *actual_size) {
  (void)rows_len;
  (void)block_len;

  IndividualRowReader r(rows);
  RowWriter w(block);
  RecordType cur;
  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    w.write(&cur);
  }
  w.close();

  *actual_size = w.bytes_written();
}

void ecall_create_block(
  uint8_t *rows, uint32_t rows_len, uint32_t num_rows, bool rows_are_join_rows,
  uint8_t *block, uint32_t block_len, uint32_t *actual_size) {
  if (rows_are_join_rows) {
    create_block<NewJoinRecord>(rows, rows_len, num_rows, block, block_len, actual_size);
  } else {
    create_block<NewRecord>(rows, rows_len, num_rows, block, block_len, actual_size);
  }
}

template<typename RecordType>
void split_block(
  uint8_t *block, uint32_t block_len,
  uint8_t *rows, uint32_t rows_len, uint32_t num_rows, uint32_t *actual_size) {
  (void)rows_len;
  (void)block_len;

  RowReader r(block);
  IndividualRowWriter w(rows);
  RecordType cur;
  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    w.write(&cur);
  }
  w.close();

  *actual_size = w.bytes_written();
}

void ecall_split_block(
  uint8_t *block, uint32_t block_len,
  uint8_t *rows, uint32_t rows_len, uint32_t num_rows, bool rows_are_join_rows,
  uint32_t *actual_size) {
  if (rows_are_join_rows) {
    split_block<NewJoinRecord>(block, block_len, rows, rows_len, num_rows, actual_size);
  } else {
    split_block<NewRecord>(block, block_len, rows, rows_len, num_rows, actual_size);
  }
}

void ecall_stream_encryption_test() {

  //const char *plaintext = "helloworld123456helloworld654321helloworld222222";
  const char *plaintext1 = "1357913579135791357913";
  const char *plaintext2 = "12345123451231234512345";

  uint8_t ciphertext[100];
  uint8_t decrypt_text[100];

  uint8_t *plaintext_ptr = NULL;
  
  StreamCipher enc(ciphertext);
  StreamDecipher dec(ciphertext, enc_size(22 * 2 + 23));

  plaintext_ptr =  (uint8_t *) plaintext1;
  enc.encrypt(plaintext_ptr, 22);
  enc.encrypt(plaintext_ptr, 22);
  
  plaintext_ptr = (uint8_t *) plaintext2;
  enc.encrypt(plaintext_ptr, 23);
  enc.finish();

  uint32_t enc_size;
  memcpy(&enc_size, ciphertext, sizeof(uint32_t));

  assert(dec_size(enc_size) == 22 * 2 + 23);

  dec.decrypt(decrypt_text, 22);
  int ret = memcmp(plaintext1, decrypt_text, 22);
  check(ret == 0, "Decryption wrong\n");
  
  dec.decrypt(decrypt_text, 22);
  ret = memcmp(plaintext1, decrypt_text, 22);
  check(ret == 0, "Decryption wrong\n");

  dec.decrypt(decrypt_text, 23);
  ret = memcmp(plaintext2, decrypt_text, 23);
  check(ret == 0, "Decryption wrong\n");

}

void ecall_generate_random_encrypted_block(uint32_t num_cols,
					   uint8_t *column_types,
					   uint32_t num_rows,
					   uint8_t *output_buffer,
					   uint32_t *encrypted_buffer_size,
					   uint8_t type) {
  
  uint32_t ret = generate_encrypted_block(num_cols,
					  column_types,
					  num_rows,
					  output_buffer,
					  type);
  *encrypted_buffer_size = ret;
}

void ecall_generate_random_encrypted_block_with_opcode(uint32_t num_cols,
						       uint8_t *column_types,
						       uint32_t num_rows,
						       uint8_t *output_buffer,
						       uint32_t *encrypted_buffer_size,
						       uint8_t type,
						       uint32_t opcode) {
  
  uint32_t ret = generate_encrypted_block_with_opcode(num_cols,
						      column_types,
						      num_rows,
						      output_buffer,
						      type,
						      opcode);
  *encrypted_buffer_size = ret;
}


void ecall_external_sort(int op_code,
			 uint32_t num_buffers,
			 uint8_t **buffer_list,
			 uint32_t *num_rows,
			 uint32_t row_upper_bound,
			 uint8_t *scratch) {
  
  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
    external_sort<NewRecord>(
      op_code, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);
    break;
  case SORT_JOIN:
    external_sort<NewJoinRecord>(
      op_code, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);
    break;
  default:
    printf("ecall_external_sort: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }
}

void ecall_sample(int op_code,
                  uint8_t *input_rows,
                  uint32_t input_rows_len,
                  uint32_t num_rows,
				  uint8_t *output_rows,
                  uint32_t *output_rows_len,
                  uint32_t *num_output_rows) {

  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
    sample<NewRecord>(
      input_rows, input_rows_len, num_rows, output_rows, output_rows_len, num_output_rows);
	break;

  case SORT_JOIN:
    sample<NewJoinRecord>(
      input_rows, input_rows_len, num_rows, output_rows, output_rows_len, num_output_rows);
	break;
	
  default:
    printf("ecall_sample: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }
  
}

void ecall_find_range_bounds(int op_code,
                             uint32_t num_partitions,
                             uint32_t num_buffers,
                             uint8_t **buffer_list,
                             uint32_t *num_rows,
                             uint32_t row_upper_bound,
                             uint8_t *output_rows,
                             uint32_t *output_rows_len,
                             uint8_t *scratch) {

  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
    find_range_bounds<NewRecord>(
      op_code, num_partitions, num_buffers, buffer_list, num_rows, row_upper_bound, output_rows,
      output_rows_len, scratch);
	break;

  case SORT_JOIN:
    find_range_bounds<NewJoinRecord>(
      op_code, num_partitions, num_buffers, buffer_list, num_rows, row_upper_bound, output_rows,
      output_rows_len, scratch);
	break;
	
  default:
    printf("ecall_find_range_bounds: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }  
}

void ecall_partition_for_sort(int op_code,
                              uint8_t num_partitions,
                              uint32_t num_buffers,
                              uint8_t **buffer_list,
                              uint32_t *num_rows,
                              uint32_t row_upper_bound,
                              uint8_t *boundary_rows,
                              uint8_t *output,
                              uint8_t **output_partition_ptrs,
                              uint32_t *output_partition_num_rows,
                              uint8_t *scratch) {

  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {

  case SORT_SORT:
    partition_for_sort<NewRecord>(
      op_code, num_partitions, num_buffers, buffer_list, num_rows, row_upper_bound, boundary_rows,
      output, output_partition_ptrs, output_partition_num_rows, scratch);
	break;

  case SORT_JOIN:
    partition_for_sort<NewJoinRecord>(
      op_code, num_partitions, num_buffers, buffer_list, num_rows, row_upper_bound, boundary_rows,
      output, output_partition_ptrs, output_partition_num_rows, scratch);
    break;

  default:
    printf("ecall_partition_for_sort: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }
}

void ecall_row_parser(uint8_t *enc_block, uint32_t input_num_rows) {

  StreamRowReader reader(enc_block);
  NewRecord row;

  uint32_t num_rows = input_num_rows;
  if (num_rows == 0) {
    num_rows = *((uint32_t *) (enc_block + 4));
  }
  printf("[ecall_row_parser] num_rows is %u\n", num_rows);
  
  for (uint32_t i = 0; i < num_rows; i++) {
	reader.read(&row);
	printf("Row %u\t\t", i);
	row.print();
  }
}


void ecall_non_oblivious_aggregate(int op_code,
				   uint8_t *input_rows, uint32_t input_rows_length,
				   uint32_t num_rows,
				   uint8_t *output_rows, uint32_t output_rows_length,
                                   uint32_t *actual_size, uint32_t *num_output_rows) {
  
  switch (op_code) {
  case OP_GROUPBY_COL1_SUM_COL2_INT:
  case OP_TEST_AGG:
    non_oblivious_aggregate<Aggregator1<GroupBy<1>, Sum<2, uint32_t> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size, num_output_rows);
    break;
  case OP_GROUPBY_COL1_SUM_COL2_FLOAT:
    non_oblivious_aggregate<Aggregator1<GroupBy<1>, Sum<2, float> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size, num_output_rows);
    break;
  case OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT:
    non_oblivious_aggregate<Aggregator2<GroupBy<1>, Avg<2, uint32_t>, Sum<3, float> > >(
      input_rows, input_rows_length, num_rows, output_rows, output_rows_length,
      actual_size, num_output_rows);
    break;
  default:
    printf("ecall_non_oblivious_aggregate: Unknown opcode %d\n", op_code);
    assert(false);
  }

}


void ecall_non_oblivious_sort_merge_join(int op_code,
										 uint8_t *input_rows, uint32_t input_rows_length,
										 uint32_t num_rows,
										 uint8_t *output_rows, uint32_t output_rows_length,
                                         uint32_t *actual_output_length,
                                         uint32_t *num_output_rows) {
  non_oblivious_sort_merge_join(op_code,
                                input_rows, input_rows_length,
                                num_rows,
                                output_rows, output_rows_length,
                                actual_output_length, num_output_rows);
}

// Column sort
//
// Step 1: Sort locally
// Step 2: Shuffle to transpose
// Step 3: Sort locally
// Step 4: Shuffle to un-transpose
// Step 5: Sort locally
// Step 6: Shift down
// Step 7: Sort locally
// Step 8: Shift up

void ecall_column_sort(int op_code,
					   int round, 
					   uint8_t *input_rows,
					   uint32_t *num_rows,
					   uint8_t **buffer_list,
					   uint32_t num_buffers, 
					   uint32_t row_upper_bound,
					   uint32_t column,
					   uint32_t r,
					   uint32_t s,
					   uint8_t **output_buffers,
					   uint32_t *output_buffer_sizes) {

  uint32_t total_num_rows = 0;

  for (uint32_t i = 0; i < num_buffers; i++) {
	total_num_rows += num_rows[i];
  }
  
  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
	{
	  	RowReader reader(input_rows);
		NewRecord row;
		
		printf("Num buffers is %u, num_rows is %u, total_num_rows is %u, column is %u\n", num_buffers, num_rows[0], total_num_rows, column);
		printf("input_rows is %p, buffer_list[0] is %p\n", input_rows, buffer_list[0]);
		printf("output_buffers: %p\n", output_buffers[0]);

	  if (round == 1) {
		external_oblivious_sort<NewRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		transpose<NewRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else if (round == 2) {
		external_oblivious_sort<NewRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		untranspose<NewRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else if (round == 3) {
		external_oblivious_sort<NewRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		shiftdown<NewRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else {
		external_oblivious_sort<NewRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		shiftup<NewRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  }
	}
	
	break;

  case SORT_JOIN:
	{
	  if (round == 1) {
		external_oblivious_sort<NewJoinRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		transpose<NewJoinRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else if (round == 2) {
		external_oblivious_sort<NewJoinRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		untranspose<NewJoinRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else if (round == 3) {
		external_oblivious_sort<NewJoinRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		shiftdown<NewJoinRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  } else {
		external_oblivious_sort<NewJoinRecord>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound);
		shiftup<NewJoinRecord>(input_rows, total_num_rows, row_upper_bound, column, r, s, output_buffers, output_buffer_sizes);
	  }

	}
	
	break;
	
  default:
    printf("ecall_sample: Unknown sort type %d for opcode %d\n", sort_op, op_code);
    assert(false);
  }
  
}
