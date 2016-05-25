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
                                   uint32_t *buffer_lengths, uint32_t *num_rows) {
  (void)buffer_lengths;

  int sort_op = get_sort_operation(op_code);
  switch (sort_op) {
  case SORT_SORT:
    external_oblivious_sort<NewRecord>(op_code, num_buffers, buffer_list, num_rows);
    break;
  case SORT_JOIN:
    external_oblivious_sort<NewJoinRecord>(op_code, num_buffers, buffer_list, num_rows);
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
  default:
    printf("ecall_aggregate_step2: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

/**** END Aggregation ****/

/**** BEGIN Join ****/

size_t join_row_size(const uint8_t *join_row) {
  (void)join_row;
  return (size_t) (enc_size(JOIN_ROW_UPPER_BOUND));
}

void ecall_join_sort_preprocess(int op_code,
                                uint8_t *table_id,
                                uint8_t *input_row, uint32_t input_row_len,
                                uint32_t num_rows,
                                uint8_t *output_row, uint32_t output_row_len) {
  (void)op_code;
  join_sort_preprocess(table_id, input_row, input_row_len, num_rows, output_row, output_row_len);
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



void ecall_stream_encryption_test() {

  //const char *plaintext = "helloworld123456helloworld654321helloworld222222";
  const char *plaintext1 = "1357913579135791357913";
  const char *plaintext2 = "12345123451231234512345";

  uint8_t ciphertext[100];
  uint8_t decrypt_text[100];

  uint8_t *plaintext_ptr = NULL;
  
  StreamCipher enc(ciphertext);
  StreamDecipher dec(ciphertext);

  plaintext_ptr =  (uint8_t *) plaintext1;
  enc.encrypt(plaintext_ptr, 22, false);
  enc.encrypt(plaintext_ptr, 22, false);
  
  plaintext_ptr = (uint8_t *) plaintext2;
  enc.encrypt(plaintext_ptr, 23, true);

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
