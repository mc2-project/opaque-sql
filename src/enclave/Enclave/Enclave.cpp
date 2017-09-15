#include "Enclave_t.h"

#include <cstdint>
#include <cassert>

#include "Aggregate.h"
#include "Crypto.h"
#include "Filter.h"
#include "Join.h"
#include "Project.h"
#include "Sort.h"
#include "isv_enclave.h"

void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length,
                   uint8_t *ciphertext, uint32_t cipher_length) {
  // IV (12 bytes) + ciphertext + mac (16 bytes)
  assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
  (void)cipher_length;
  (void)plaintext_length;
  encrypt(plaintext, plaintext_length, ciphertext);
}

void ecall_decrypt(uint8_t *ciphertext,
                   uint32_t ciphertext_length,
                   uint8_t *plaintext,
                   uint32_t plaintext_length) {
  // IV (12 bytes) + ciphertext + mac (16 bytes)
  assert(ciphertext_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
  (void)ciphertext_length;
  (void)plaintext_length;
  decrypt(ciphertext, ciphertext_length, plaintext);
}

void ecall_project(uint8_t *condition, size_t condition_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length) {
  project(condition, condition_length,
          input_rows, input_rows_length,
          output_rows, output_rows_length);
}

void ecall_filter(uint8_t *condition, size_t condition_length,
                  uint8_t *input_rows, size_t input_rows_length,
                  uint8_t **output_rows, size_t *output_rows_length) {
  filter(condition, condition_length,
         input_rows, input_rows_length,
         output_rows, output_rows_length);
}

void ecall_sample(uint8_t *input_rows, size_t input_rows_length,
                  uint8_t **output_rows, size_t *output_rows_length) {
  sample(input_rows, input_rows_length,
         output_rows, output_rows_length);
}

void ecall_find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                             uint32_t num_partitions,
                             uint8_t *input_rows, size_t input_rows_length,
                             uint8_t **output_rows, size_t *output_rows_length) {
  find_range_bounds(sort_order, sort_order_length,
                    num_partitions,
                    input_rows, input_rows_length,
                    output_rows, output_rows_length);
}

void ecall_partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                              uint32_t num_partitions,
                              uint8_t *input_rows, size_t input_rows_length,
                              uint8_t *boundary_rows, size_t boundary_rows_length,
                              uint8_t **output_partitions, size_t *output_partition_lengths) {
  partition_for_sort(sort_order, sort_order_length,
                     num_partitions,
                     input_rows, input_rows_length,
                     boundary_rows, boundary_rows_length,
                     output_partitions, output_partition_lengths);
}

void ecall_external_sort(uint8_t *sort_order, size_t sort_order_length,
                         uint8_t *input_rows, size_t input_rows_length,
                         uint8_t **output_rows, size_t *output_rows_length) {
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                output_rows, output_rows_length);
}

void ecall_scan_collect_last_primary(uint8_t *join_expr, size_t join_expr_length,
                                     uint8_t *input_rows, size_t input_rows_length,
                                     uint8_t **output_rows, size_t *output_rows_length) {
  scan_collect_last_primary(join_expr, join_expr_length,
                            input_rows, input_rows_length,
                            output_rows, output_rows_length);
}

void ecall_non_oblivious_sort_merge_join(uint8_t *join_expr, size_t join_expr_length,
                                         uint8_t *input_rows, size_t input_rows_length,
                                         uint8_t *join_row, size_t join_row_length,
                                         uint8_t **output_rows, size_t *output_rows_length) {
  non_oblivious_sort_merge_join(join_expr, join_expr_length,
                                input_rows, input_rows_length,
                                join_row, join_row_length,
                                output_rows, output_rows_length);
}

void ecall_non_oblivious_aggregate_step1(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **first_row, size_t *first_row_length,
  uint8_t **last_group, size_t *last_group_length,
  uint8_t **last_row, size_t *last_row_length) {
  non_oblivious_aggregate_step1(
    agg_op, agg_op_length,
    input_rows, input_rows_length,
    first_row, first_row_length,
    last_group, last_group_length,
    last_row, last_row_length);
}

void ecall_non_oblivious_aggregate_step2(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *next_partition_first_row, size_t next_partition_first_row_length,
  uint8_t *prev_partition_last_group, size_t prev_partition_last_group_length,
  uint8_t *prev_partition_last_row, size_t prev_partition_last_row_length,
  uint8_t **output_rows, size_t *output_rows_length) {
  non_oblivious_aggregate_step2(
    agg_op, agg_op_length,
    input_rows, input_rows_length,
    next_partition_first_row, next_partition_first_row_length,
    prev_partition_last_group, prev_partition_last_group_length,
    prev_partition_last_row, prev_partition_last_row_length,
    output_rows, output_rows_length);
}

sgx_status_t ecall_enclave_init_ra(int b_pse, sgx_ra_context_t *p_context) {
  return enclave_init_ra(b_pse, p_context);
}


void ecall_enclave_ra_close(sgx_ra_context_t context) {
  enclave_ra_close(context);
}

sgx_status_t ecall_verify_att_result_mac(sgx_ra_context_t context, uint8_t* message,
                                         size_t message_size, uint8_t* mac,
                                         size_t mac_size) {

  return verify_att_result_mac(context, message, message_size, mac, mac_size);
}

sgx_status_t ecall_put_secret_data(sgx_ra_context_t context,
                                   uint8_t* p_secret,
                                   uint32_t secret_size,
                                   uint8_t* gcm_mac) {

  return put_secret_data(context, p_secret, secret_size, gcm_mac);
}
