#include "Enclave_t.h"
#include <iostream>

#include <cassert>
#include <cstdint>

#include "Random.h"

#include "Aggregate.h"
#include "Attestation.h"
#include "BroadcastNestedLoopJoin.h"
#include "Crypto.h"
#include "Filter.h"
#include "Limit.h"
#include "NonObliviousSortMergeJoin.h"
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

#include <openenclave/attestation/sgx/evidence.h>

// This file contains definitions of the ecalls declared in Enclave.edl. Errors
// originating within these ecalls are signaled by throwing a
// std::runtime_error, which is caught at the top level of the ecall (i.e.,
// within these definitions), and are then rethrown as Java exceptions using
// ocall_throw.

void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext,
                   uint32_t cipher_length) {
  // Guard against encrypting or overwriting enclave memory
  assert(oe_is_outside_enclave(plaintext, plaintext_length) == 1);
  assert(oe_is_outside_enclave(ciphertext, cipher_length) == 1);
  __builtin_ia32_lfence();

  try {
    // IV (12 bytes) + ciphertext + mac (16 bytes)
    assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
    (void)cipher_length;
    (void)plaintext_length;
    encrypt(plaintext, plaintext_length, ciphertext);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_decrypt(uint8_t *ciphertext, uint32_t cipher_length, uint8_t *plaintext,
                   uint32_t plaintext_length) {

  // Guard against decrypting or overwriting enclave memory
  assert(oe_is_outside_enclave(plaintext, plaintext_length) == 1);
  assert(oe_is_outside_enclave(ciphertext, cipher_length) == 1);
  __builtin_ia32_lfence();

  try {
    // IV (12 bytes) + ciphertext + mac (16 bytes)
    assert(cipher_length >= plaintext_length + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
    (void)cipher_length;
    (void)plaintext_length;
    decrypt(ciphertext, cipher_length, plaintext);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }

}

void ecall_project(uint8_t *condition, size_t condition_length, uint8_t *input_rows,
                   size_t input_rows_length, uint8_t **output_rows, size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    project(condition, condition_length, input_rows, input_rows_length, output_rows,
            output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_filter(uint8_t *condition, size_t condition_length, uint8_t *input_rows,
                  size_t input_rows_length, uint8_t **output_rows, size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    filter(condition, condition_length, input_rows, input_rows_length, output_rows,
           output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_sample(uint8_t *input_rows, size_t input_rows_length, uint8_t **output_rows,
                  size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    sample(input_rows, input_rows_length, output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                             uint32_t num_partitions, uint8_t *input_rows,
                             size_t input_rows_length, uint8_t **output_rows,
                             size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    find_range_bounds(sort_order, sort_order_length, num_partitions, input_rows,
                      input_rows_length, output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                              uint32_t num_partitions, uint8_t *input_rows,
                              size_t input_rows_length, uint8_t *boundary_rows,
                              size_t boundary_rows_length, uint8_t **output_partitions,
                              size_t *output_partition_lengths) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  assert(oe_is_outside_enclave(boundary_rows, boundary_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    partition_for_sort(sort_order, sort_order_length, num_partitions, input_rows,
                       input_rows_length, boundary_rows, boundary_rows_length, output_partitions,
                       output_partition_lengths);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_external_sort(uint8_t *sort_order, size_t sort_order_length, uint8_t *input_rows,
                         size_t input_rows_length, uint8_t **output_rows,
                         size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    external_sort(sort_order, sort_order_length, input_rows, input_rows_length, output_rows,
                  output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_non_oblivious_sort_merge_join(uint8_t *join_expr, size_t join_expr_length,
                                         uint8_t *input_rows, size_t input_rows_length,
                                         uint8_t **output_rows, size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    non_oblivious_sort_merge_join(join_expr, join_expr_length, input_rows, input_rows_length,
                                  output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_broadcast_nested_loop_join(uint8_t *join_expr, size_t join_expr_length,
                                      uint8_t *outer_rows, size_t outer_rows_length,
                                      uint8_t *inner_rows, size_t inner_rows_length,
                                      uint8_t **output_rows, size_t *output_rows_length) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(outer_rows, outer_rows_length) == 1);
  assert(oe_is_outside_enclave(inner_rows, inner_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    broadcast_nested_loop_join(join_expr, join_expr_length, outer_rows, outer_rows_length,
                               inner_rows, inner_rows_length, output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_non_oblivious_aggregate(uint8_t *agg_op, size_t agg_op_length, uint8_t *input_rows,
                                   size_t input_rows_length, uint8_t **output_rows,
                                   size_t *output_rows_length, bool is_partial) {
  // Guard against operating on arbitrary enclave memory
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    non_oblivious_aggregate(agg_op, agg_op_length, input_rows, input_rows_length, output_rows,
                            output_rows_length, is_partial);

  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_count_rows_per_partition(uint8_t *input_rows, size_t input_rows_length,
                                    uint8_t **output_rows, size_t *output_rows_length) {
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    count_rows_per_partition(input_rows, input_rows_length, output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_compute_num_rows_per_partition(uint32_t limit, uint8_t *input_rows,
                                          size_t input_rows_length, uint8_t **output_rows,
                                          size_t *output_rows_length) {
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    compute_num_rows_per_partition(limit, input_rows, input_rows_length, output_rows,
                                   output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_local_limit(uint32_t limit, uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length) {
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    limit_return_rows(limit, input_rows, input_rows_length, output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_limit_return_rows(uint64_t partition_id, uint8_t *limits, size_t limits_length,
                             uint8_t *input_rows, size_t input_rows_length, uint8_t **output_rows,
                             size_t *output_rows_length) {
  assert(oe_is_outside_enclave(limits, limits_length) == 1);
  assert(oe_is_outside_enclave(input_rows, input_rows_length) == 1);
  __builtin_ia32_lfence();

  try {
    limit_return_rows(partition_id, limits, limits_length, input_rows, input_rows_length,
                      output_rows, output_rows_length);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

static Crypto g_crypto;

void ecall_finish_attestation(uint8_t *shared_key_msg_input, uint32_t shared_key_msg_size) {
  try {
    (void) shared_key_msg_size;
    oe_shared_key_msg_t *shared_key_msg = (oe_shared_key_msg_t *)shared_key_msg_input;
    uint8_t shared_key_plaintext[SGX_AESGCM_KEY_SIZE];
    size_t shared_key_plaintext_size = sizeof(shared_key_plaintext);
    bool ret = g_crypto.decrypt(shared_key_msg->shared_key_ciphertext, OE_SHARED_KEY_CIPHERTEXT_SIZE,
                                shared_key_plaintext, &shared_key_plaintext_size);
    if (!ret) {
      ocall_throw("shared key decryption failed");
    }

//    set_shared_key(shared_key_plaintext, shared_key_plaintext_size);
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

/*
   Enclave generates report, which is then sent back to the service provider
*/
void ecall_generate_report(uint8_t **report_msg_data, size_t *report_msg_data_size) {

  uint8_t public_key[OE_PUBLIC_KEY_SIZE] = {};
  size_t public_key_size = sizeof(public_key);
  uint8_t sha256[OE_SHA256_HASH_SIZE];
  uint8_t *report = NULL;
  size_t report_size = 0;
  oe_report_msg_t *report_msg = NULL;

  if (report_msg_data == NULL || report_msg_data_size == NULL) {
    ocall_throw("Invalid parameter");
  }

  *report_msg_data = NULL;
  *report_msg_data_size = 0;

  g_crypto.retrieve_public_key(public_key);

  if (g_crypto.sha256(public_key, public_key_size, sha256) != 0) {
    ocall_throw("sha256 failed");
  }

#ifndef SIMULATE
  // Get OE report
  oe_result_t result = oe_get_report(OE_REPORT_FLAGS_REMOTE_ATTESTATION,
                                     sha256, // Store sha256 in report_data field
                                     sizeof(sha256), NULL, 0, &report, &report_size);

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
  *report_msg_data = (uint8_t *)oe_host_malloc(*report_msg_data_size);
  if (*report_msg_data == NULL) {
    ocall_throw("Out of memory");
  }
  report_msg = (oe_report_msg_t *)(*report_msg_data);

  // Fill oe_report_msg_t
  memcpy_s(report_msg->public_key, sizeof(((oe_report_msg_t *)0)->public_key), public_key,
           public_key_size);
  report_msg->report_size = report_size;
  if (report_size > 0) {
    memcpy_s(report_msg->report, report_size, report, report_size);
  }
  oe_free_report(report);
}

//////////////////////////////////// Generate Shared Key Begin //////////////////////////////////////

static Attestation attestation(&g_crypto);

void ecall_get_public_key(uint8_t **report_msg_data,
                           size_t* report_msg_data_size) {

  oe_uuid_t sgx_local_uuid = {OE_FORMAT_UUID_SGX_LOCAL_ATTESTATION};
  oe_uuid_t* format_id = &sgx_local_uuid;

  uint8_t* format_settings = NULL;
  size_t format_settings_size = 0;

  if (!attestation.get_format_settings(
        format_id,
        &format_settings,
        &format_settings_size)) {
    ocall_throw("Unable to get enclave format settings");
  }

  uint8_t pem_public_key[512];
  size_t public_key_size = sizeof(pem_public_key);
  uint8_t* evidence = nullptr;
  size_t evidence_size = 0;

  g_crypto.retrieve_public_key(pem_public_key);

  if (attestation.generate_attestation_evidence(
            format_id,
            format_settings,
            format_settings_size,
            pem_public_key,
            public_key_size,
            &evidence,
            &evidence_size) == false) {
    ocall_throw("Unable to retrieve enclave evidence");
  }

  if (!attestation.attest_attestation_evidence(format_id, evidence, evidence_size, pem_public_key, public_key_size)) {
    ocall_throw("Unable to verify FRESH attestation!");
  }

  // The report msg includes the public key, the size of the evidence, and the evidence itself
  *report_msg_data_size = public_key_size + sizeof(evidence_size) + evidence_size;
  *report_msg_data = (uint8_t*)oe_host_malloc(*report_msg_data_size);

  memcpy_s(*report_msg_data, public_key_size, pem_public_key, public_key_size);
  memcpy_s(*report_msg_data + public_key_size, sizeof(size_t), &evidence_size, sizeof(evidence_size)); 

  memcpy_s(*report_msg_data + public_key_size + sizeof(size_t), evidence_size, evidence, evidence_size);

}

void ecall_get_list_encrypted(uint8_t * pk_list,
                              uint32_t pk_list_size, 
                              uint8_t * sk_list,
                              uint32_t sk_list_size) {

  // Guard against encrypting or overwriting enclave memory
  assert(oe_is_outside_enclave(pk_list, pk_list_size) == 1);
  assert(oe_is_outside_enclave(sk_list, sk_list_size) == 1);
  __builtin_ia32_lfence();

  (void) sk_list_size;

  try {
    // Generate a random value used for key
    // Size of shared key is 16 from ServiceProvider - LC_AESGCM_KEY_SIZE
    // For now SGX_AESGCM_KEY_SIZE is also 16, so will just use that for now

    unsigned char secret_key[SGX_AESGCM_KEY_SIZE] = {0};
    mbedtls_read_rand(secret_key, SGX_AESGCM_KEY_SIZE);

    uint8_t public_key[OE_PUBLIC_KEY_SIZE] = {};
    uint8_t *pk_pointer = pk_list;

    size_t evidence_size[1] = {};

    unsigned char encrypted_sharedkey[OE_SHARED_KEY_CIPHERTEXT_SIZE];
    size_t encrypted_sharedkey_size = sizeof(encrypted_sharedkey);

    uint8_t *sk_pointer = sk_list;

    oe_uuid_t sgx_local_uuid = {OE_FORMAT_UUID_SGX_LOCAL_ATTESTATION};
    oe_uuid_t* format_id = &sgx_local_uuid;

    uint8_t* format_settings = NULL;
    size_t format_settings_size = 0;

    while (pk_pointer < pk_list + pk_list_size) {

      if (!attestation.get_format_settings(
            format_id,
            &format_settings,
            &format_settings_size)) {
        ocall_throw("Unable to get enclave format settings");
      }

      // Read public key, size of evidence, and evidence
      memcpy_s(public_key, OE_PUBLIC_KEY_SIZE, pk_pointer, OE_PUBLIC_KEY_SIZE);
      memcpy_s(evidence_size, sizeof(evidence_size), pk_pointer + OE_PUBLIC_KEY_SIZE, sizeof(size_t));
      uint8_t evidence[evidence_size[0]] = {};
      memcpy_s(evidence, evidence_size[0], pk_pointer + OE_PUBLIC_KEY_SIZE + sizeof(size_t), evidence_size[0]);

      // Verify the provided public key is valid
      if (!attestation.attest_attestation_evidence(format_id, evidence, evidence_size[0], public_key, sizeof(public_key))) {
        std::cout << "get_list_encrypted - unable to verify attestation evidence" << std::endl;
        ocall_throw("Unable to verify attestation evidence");
      }

      g_crypto.encrypt(public_key,
                       secret_key,
                       SGX_AESGCM_KEY_SIZE,
                       encrypted_sharedkey,
                       &encrypted_sharedkey_size);
      memcpy_s(sk_pointer, OE_SHARED_KEY_CIPHERTEXT_SIZE, encrypted_sharedkey, OE_SHARED_KEY_CIPHERTEXT_SIZE);

      pk_pointer += OE_PUBLIC_KEY_SIZE + sizeof(size_t) + evidence_size[0];
      sk_pointer += OE_SHARED_KEY_CIPHERTEXT_SIZE;
    }
  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }

}

void ecall_finish_shared_key(uint8_t *sk_list,
                             uint32_t sk_list_size,
                             uint8_t *sk,
                             uint32_t sk_size) {

  (void) sk;
  (void) sk_size;

  uint8_t *sk_pointer = sk_list;

  uint8_t secret_key[SGX_AESGCM_KEY_SIZE] = {0};
  size_t sk_length = sizeof(secret_key);
  assert(sk_length == sk_size);

  while (sk_pointer < sk_list + sk_list_size) {
    uint8_t encrypted_sharedkey[OE_SHARED_KEY_CIPHERTEXT_SIZE];
    size_t encrypted_sharedkey_size = sizeof(encrypted_sharedkey);

    memcpy_s(encrypted_sharedkey, encrypted_sharedkey_size, sk_pointer, OE_SHARED_KEY_CIPHERTEXT_SIZE);

    try {
      bool ret = g_crypto.decrypt(encrypted_sharedkey, encrypted_sharedkey_size, secret_key, &sk_length);
      if (ret) {break;} // Decryption was successful to obtain secret key
    } catch (const std::runtime_error &e) {
      ocall_throw(e.what());
    }

    sk_pointer += OE_SHARED_KEY_CIPHERTEXT_SIZE;
  }

//  set_shared_key(secret_key, sk_size);
  initKeySchedule();

  // Print out shared_key in two's complement
  for (int i = 0; i < SGX_AESGCM_KEY_SIZE; i++) {
    uint8_t byte = secret_key[i];

    // In 2c form
    if ((byte >> (7)) & 1) {
      uint8_t two_comp = ((byte ^ (1 << 7)) ^ 127) + 1;
      std::cout << "-" << +two_comp << " ";
    } else {
      std::cout << +byte << " ";
    }
  }

  std::cout << std::endl;

  memcpy(sk, secret_key, SGX_AESGCM_KEY_SIZE);
}

//////////////////////////////////// Generate Shared Key End //////////////////////////////////////
