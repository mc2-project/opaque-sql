#include "Enclave_t.h"

#include <cstdint>
#include <cassert>

#include "Aggregate.h"
#include "Crypto.h"
#include "Filter.h"
#include "Join.h"
#include "Limit.h"
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

// This file contains definitions of the ecalls declared in Enclave.edl. Errors originating within
// these ecalls are signaled by throwing a std::runtime_error, which is caught at the top level of
// the ecall (i.e., within these definitions), and are then rethrown as Java exceptions using
// ocall_throw.

void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length,
                   uint8_t *ciphertext, uint32_t cipher_length) {
  // Guard against encrypting or overwriting enclave memory
  assert(oe_is_outside_enclave(plaintext, plaintext_length) == 1);
  assert(oe_is_outside_enclave(ciphertext, cipher_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Ecall: Encrypt\n");
    // IV (12 bytes) + ciphertext + mac (16 bytes)
    assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
    (void)cipher_length;
    (void)plaintext_length;
    encrypt(plaintext, plaintext_length, ciphertext);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Project\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    project(condition, condition_length,
            input_rows, input_rows_length,
            output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Filter\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    filter(condition, condition_length,
           input_rows, input_rows_length,
           output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Sample\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    sample(input_rows, input_rows_length,
           output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Find Range Bounds\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    find_range_bounds(sort_order, sort_order_length,
                      num_partitions,
                      input_rows, input_rows_length,
                      output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  assert(oe_is_outside_enclave(boundary_rows, boundary_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Partition for Sort\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    if (pid > 0) {
      EnclaveContext::getInstance().increment_job_id();
    }
    partition_for_sort(sort_order, sort_order_length,
                       num_partitions,
                       input_rows, input_rows_length,
                       boundary_rows, boundary_rows_length,
                       output_partitions, output_partition_lengths);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: External Sort\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    external_sort(sort_order, sort_order_length,
                  input_rows, input_rows_length,
                  output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Scan Collect Last Primary\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    scan_collect_last_primary(join_expr, join_expr_length,
                              input_rows, input_rows_length,
                              output_rows, output_rows_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  assert(oe_is_outside_enclave(join_row, join_row_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Non Oblivious Sort Merge Join\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_sort_merge_join(join_expr, join_expr_length,
                                  input_rows, input_rows_length,
                                  join_row, join_row_length,
                                  output_rows, output_rows_length);

    EnclaveContext::getInstance().finish_ecall();
  } catch (const std::runtime_error &e) {
    EnclaveContext::getInstance().finish_ecall();
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Non Oblivious Aggregate Step 1\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_aggregate_step1(
      agg_op, agg_op_length,
      input_rows, input_rows_length,
      first_row, first_row_length,
      last_group, last_group_length,
      last_row, last_row_length);
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
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  assert(oe_is_outside_enclave(next_partition_first_row, next_partition_first_row_length) == 1);
  assert(oe_is_outside_enclave(prev_partition_last_group, prev_partition_last_group_length) == 1);
  assert(oe_is_outside_enclave(prev_partition_last_row, prev_partition_last_row_length) == 1);
  __builtin_ia32_lfence();

  try {
    debug("Partition %i Ecall: Non Oblivious Aggregate Step 2\n", pid);
    EnclaveContext::getInstance().set_pid(pid);
    non_oblivious_aggregate_step2(
      agg_op, agg_op_length,
      input_rows, input_rows_length,
      next_partition_first_row, next_partition_first_row_length,
      prev_partition_last_group, prev_partition_last_group_length,
      prev_partition_last_row, prev_partition_last_row_length,
      output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    EnclaveContext::getInstance().finish_ecall();
    ocall_throw(e.what());
  }
}

void ecall_count_rows_per_partition(uint8_t *input_rows, size_t input_rows_length,	
                                    uint8_t **output_rows, size_t *output_rows_length) {	
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);	
  __builtin_ia32_lfence();	

  try {	
    count_rows_per_partition(input_rows, input_rows_length,	
                             output_rows, output_rows_length);	
  } catch (const std::runtime_error &e) {	
    ocall_throw(e.what());	
  }	
}	

void ecall_compute_num_rows_per_partition(uint32_t limit,	
                                          uint8_t *input_rows, size_t input_rows_length,	
                                          uint8_t **output_rows, size_t *output_rows_length) {	
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);	
  __builtin_ia32_lfence();	

  try {	
    compute_num_rows_per_partition(limit,	
                                   input_rows, input_rows_length,	
                                   output_rows, output_rows_length);	
  } catch (const std::runtime_error &e) {	
    ocall_throw(e.what());	
  }	
}	

void ecall_local_limit(uint32_t limit,	
                       uint8_t *input_rows, size_t input_rows_length,	
                       uint8_t **output_rows, size_t *output_rows_length) {	
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);	
  __builtin_ia32_lfence();	

  try {	
    limit_return_rows(limit,	
                      input_rows, input_rows_length,	
                      output_rows, output_rows_length);	
  } catch (const std::runtime_error &e) {	
    ocall_throw(e.what());	
  }	
}	

void ecall_limit_return_rows(uint64_t partition_id,	
                             uint8_t *limits, size_t limits_length,	
                             uint8_t *input_rows, size_t input_rows_length,	
                             uint8_t **output_rows, size_t *output_rows_length) {	
  assert(oe_is_outside_enclave(limits, limits_length) == 1);	
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);	
  __builtin_ia32_lfence();	

  try {	
    limit_return_rows(partition_id,	
                      limits, limits_length,	
                      input_rows, input_rows_length,	
                      output_rows, output_rows_length);	
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

static Crypto g_crypto;

void ecall_finish_attestation(uint8_t *shared_key_msg_input,
                              uint32_t shared_key_msg_size) {
  try {
    oe_shared_key_msg_t* shared_key_msg = (oe_shared_key_msg_t*) shared_key_msg_input;
    uint8_t shared_key_plaintext[SGX_AESGCM_KEY_SIZE];
    size_t shared_key_plaintext_size = sizeof(shared_key_plaintext);
    bool ret = g_crypto.decrypt(shared_key_msg->shared_key_ciphertext, shared_key_msg_size, 
        shared_key_plaintext, &shared_key_plaintext_size);
    if (!ret) {
      ocall_throw("shared key decryption failed");
    }

    set_shared_key(shared_key_plaintext, shared_key_plaintext_size);
    EnclaveContext::getInstance().reset_pid_jobid_map();
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

/* 
   Enclave generates report, which is then sent back to the service provider
*/
void ecall_generate_report(uint8_t **report_msg_data,
                           size_t* report_msg_data_size) {

  uint8_t public_key[OE_PUBLIC_KEY_SIZE] = {};
  size_t public_key_size = sizeof(public_key);
  uint8_t sha256[OE_SHA256_HASH_SIZE];
  uint8_t* report = NULL;
  size_t report_size = 0;
  oe_report_msg_t* report_msg = NULL;

  if (report_msg_data == NULL || report_msg_data_size == NULL)
  {
    ocall_throw("Invalid parameter");
  }

  *report_msg_data = NULL;
  *report_msg_data_size = 0;

  g_crypto.retrieve_public_key(public_key);

  if (g_crypto.sha256(public_key, public_key_size, sha256) != 0)
  {
    ocall_throw("sha256 failed");
  }

#ifndef SIMULATE
  // Get OE report
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
    
  *report_msg_data_size = sizeof(oe_report_msg_t) + report_size;
  *report_msg_data = (uint8_t*)oe_host_malloc(*report_msg_data_size);
  if (*report_msg_data == NULL) {
    ocall_throw("Out of memory");
  }
  report_msg = (oe_report_msg_t*)(*report_msg_data);

  // Fill oe_report_msg_t
  memcpy_s(report_msg->public_key, sizeof(((oe_report_msg_t*)0)->public_key), 
      public_key, public_key_size);
  report_msg->report_size = report_size;
  if (report_size > 0) {
    memcpy_s(report_msg->report, report_size, report, report_size);
  }
  oe_free_report(report);

}
