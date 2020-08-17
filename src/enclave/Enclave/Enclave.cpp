#include "Enclave_t.h"

#include <cstdint>
#include <cassert>

// #include <sgx_lfence.h>
// #include <sgx_tkey_exchange.h>

#include "Aggregate.h"
#include "Crypto.h"
#include "Filter.h"
#include "Join.h"
#include "Project.h"
#include "Sort.h"
#include "util.h"

#include "../Common/common.h"
#include "../Common/mCrypto.h"
#include <mbedtls/config.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>
#include "EnclaveContext.h"
#include <iostream>

// This file contains definitions of the ecalls declared in Enclave.edl. Errors originating within
// these ecalls are signaled by throwing a std::runtime_error, which is caught at the top level of
// the ecall (i.e., within these definitions), and are then rethrown as Java exceptions using
// ocall_throw.
static Crypto g_crypto;

// Input from this partition
// Output to this partition
void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length,
                   uint8_t *ciphertext, uint32_t cipher_length) {
  // Guard against encrypting or overwriting enclave memory
  // assert(sgx_is_outside_enclave(plaintext, plaintext_length) == 1);
  // assert(sgx_is_outside_enclave(ciphertext, cipher_length) == 1);
  // sgx_lfence();

  try {
    // debug("Ecall: Encrypt\n");
    // IV (12 bytes) + ciphertext + mac (16 bytes)
    assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
    (void)cipher_length;
    (void)plaintext_length;

    encrypt(plaintext, plaintext_length, ciphertext);
    // EnclaveContext::getInstance().finish_ecall();

    // FIXME: Do we even care about this operation? This is an encrypt exec call that likely doesn't fit into executed plan
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// Input from this partition
// Output to this partition
void ecall_project(uint8_t *condition, size_t condition_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length,
                   int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Project\n");
    EnclaveContext::getInstance().set_pid(pid);
    project(condition, condition_length,
            input_rows, input_rows_length,
            output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// Input from this partition
// Output to this partition
void ecall_filter(uint8_t *condition, size_t condition_length,
                  uint8_t *input_rows, size_t input_rows_length,
                  uint8_t **output_rows, size_t *output_rows_length,
                  int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Filter\n");
    std::cout << "begin ecall\n";
    EnclaveContext::getInstance().set_pid(pid);
    filter(condition, condition_length,
           input_rows, input_rows_length,
           output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
    std::cout << "end ecall\n";
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// Input from this partition
// Output to 1 partition (likely not this partition)
void ecall_sample(uint8_t *input_rows, size_t input_rows_length,
                  uint8_t **output_rows, size_t *output_rows_length,
                  int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Sample\n");
    EnclaveContext::getInstance().set_pid(pid);
    sample(input_rows, input_rows_length,
           output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// This call only run on one worker.
// Input from all partitions
// Output to all partitions
void ecall_find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                             uint32_t num_partitions,
                             uint8_t *input_rows, size_t input_rows_length,
                             uint8_t **output_rows, size_t *output_rows_length,
                             int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Find Range Bounds\n");
    EnclaveContext::getInstance().set_pid(pid);
    find_range_bounds(sort_order, sort_order_length,
                      num_partitions,
                      input_rows, input_rows_length,
                      output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// boundary_rows from one partition (partition 1)
// output_rows to all partitions
void ecall_partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                              uint32_t num_partitions,
                              uint8_t *input_rows, size_t input_rows_length,
                              uint8_t *boundary_rows, size_t boundary_rows_length,
                              uint8_t **output_partitions, size_t *output_partition_lengths,
                              int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // assert(sgx_is_outside_enclave(boundary_rows, boundary_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Partition for Sort\n");
    EnclaveContext::getInstance().set_pid(pid);
    partition_for_sort(sort_order, sort_order_length,
                       num_partitions,
                       input_rows, input_rows_length,
                       boundary_rows, boundary_rows_length,
                       output_partitions, output_partition_lengths);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// input either from one partition or all partitions
// output stays in partition
void ecall_external_sort(uint8_t *sort_order, size_t sort_order_length,
                         uint8_t *input_rows, size_t input_rows_length,
                         uint8_t **output_rows, size_t *output_rows_length,
                         int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: External Sort\n");
    EnclaveContext::getInstance().set_pid(pid);
    external_sort(sort_order, sort_order_length,
                  input_rows, input_rows_length,
                  output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// Output: last row of last primary group sent to next partition
// 1-1 shuffle
void ecall_scan_collect_last_primary(uint8_t *join_expr, size_t join_expr_length,
                                     uint8_t *input_rows, size_t input_rows_length,
                                     uint8_t **output_rows, size_t *output_rows_length,
                                     int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Scan Collect Last Primary\n");
    EnclaveContext::getInstance().set_pid(pid);
    scan_collect_last_primary(join_expr, join_expr_length,
                              input_rows, input_rows_length,
                              output_rows, output_rows_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

// Input: join_row usually from previous partition 
// Output: stays in this partition
void ecall_non_oblivious_sort_merge_join(uint8_t *join_expr, size_t join_expr_length,
                                         uint8_t *input_rows, size_t input_rows_length,
                                         uint8_t *join_row, size_t join_row_length,
                                         uint8_t **output_rows, size_t *output_rows_length,
                                         int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // assert(sgx_is_outside_enclave(join_row, join_row_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Non Oblivious Sort Merge Join\n");
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_sort_merge_join(join_expr, join_expr_length,
                                  input_rows, input_rows_length,
                                  join_row, join_row_length,
                                  output_rows, output_rows_length);

    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_non_oblivious_aggregate_step1(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **first_row, size_t *first_row_length,
  uint8_t **last_group, size_t *last_group_length,
  uint8_t **last_row, size_t *last_row_length,
  int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Non Oblivious Aggregate Step 1\n");
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_aggregate_step1(
      agg_op, agg_op_length,
      input_rows, input_rows_length,
      first_row, first_row_length,
      last_group, last_group_length,
      last_row, last_row_length);
    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_non_oblivious_aggregate_step2(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t *next_partition_first_row, size_t next_partition_first_row_length,
  uint8_t *prev_partition_last_group, size_t prev_partition_last_group_length,
  uint8_t *prev_partition_last_row, size_t prev_partition_last_row_length,
  uint8_t **output_rows, size_t *output_rows_length,
  int pid) {
  // Guard against operating on arbitrary enclave memory
  // assert(sgx_is_outside_enclave(input_rows, input_rows_length) == 1);
  // assert(sgx_is_outside_enclave(next_partition_first_row, next_partition_first_row_length) == 1);
  // assert(sgx_is_outside_enclave(prev_partition_last_group, prev_partition_last_group_length) == 1);
  // assert(sgx_is_outside_enclave(prev_partition_last_row, prev_partition_last_row_length) == 1);
  // sgx_lfence();

  try {
    debug("Ecall: Non Oblivious Aggregate Step 2\n");
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_aggregate_step2(
      agg_op, agg_op_length,
      input_rows, input_rows_length,
      next_partition_first_row, next_partition_first_row_length,
      prev_partition_last_group, prev_partition_last_group_length,
      prev_partition_last_row, prev_partition_last_row_length,
      output_rows, output_rows_length);

    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}


void ecall_ra_proc_msg4(
  uint8_t *msg4, uint32_t msg4_size, uint32_t partition_index) {
  try {
    oe_msg2_t* msg2 = (oe_msg2_t*)msg4;
    uint8_t shared_key_plaintext[SGX_AESGCM_KEY_SIZE];
    size_t shared_key_plaintext_size = sizeof(shared_key_plaintext);
    bool ret = g_crypto.decrypt(msg2->shared_key_ciphertext, msg4_size, shared_key_plaintext, &shared_key_plaintext_size);
    if (!ret)
    {
      ocall_throw("shared key decryption failed");
    }

    set_shared_key(shared_key_plaintext, shared_key_plaintext_size);
    EnclaveContext::getInstance().set_partition_index(partition_index);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_oe_proc_msg1(uint8_t **msg1_data,
                        size_t* msg1_data_size) {

  uint8_t public_key[OE_PUBLIC_KEY_SIZE] = {};
  size_t public_key_size = sizeof(public_key);
  uint8_t sha256[OE_SHA256_HASH_SIZE];
  uint8_t* report = NULL;
  size_t report_size = 0;
  oe_msg1_t* msg1 = NULL;

  if (msg1_data == NULL || msg1_data_size == NULL)
  {
    ocall_throw("Invalid parameter");
  }

  *msg1_data = NULL;
  *msg1_data_size = 0;

  g_crypto.retrieve_public_key(public_key);

  if (g_crypto.sha256(public_key, public_key_size, sha256) != 0)
  {
    ocall_throw("sha256 failed");
  }

#ifndef SIMULATE
  // get report
  oe_result_t result = oe_get_report(OE_REPORT_FLAGS_REMOTE_ATTESTATION,
                                     sha256, // Store sha256 in report_data field
                                     sizeof(sha256),
                                     NULL,
                                     0,
                                     &report,
                                     &report_size);

  if (result != OE_OK) {
    ocall_throw("oe_get_report failed");
  }
    
#endif

#ifndef SIMULATE
  if (report == NULL) {
    ocall_throw("OE report is NULL");
  }
#endif
    
  *msg1_data_size = sizeof(oe_msg1_t) + report_size;
  *msg1_data = (uint8_t*)oe_host_malloc(*msg1_data_size);
  if (*msg1_data == NULL) {
    ocall_throw("Out of memory");
  }
  msg1 = (oe_msg1_t*)(*msg1_data);

  // Fill oe_msg1_t
  memcpy_s(msg1->public_key, sizeof(((oe_msg1_t*)0)->public_key), public_key, public_key_size);
  msg1->report_size = report_size;
  if (report_size > 0) {
    memcpy_s(msg1->report, report_size, report, report_size);
  }
  oe_free_report(report);

}
