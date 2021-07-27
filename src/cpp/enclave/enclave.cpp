#include "enclave_t.h"

#include <cassert>
#include <cstdint>

#include "common.h"
#include "crypto/crypto_context.h"
#include "crypto/ks_crypto.h"
#include "physical_operators/aggregate.h"
#include "physical_operators/broadcast_nested_loop_join.h"
#include "physical_operators/filter.h"
#include "physical_operators/limit.h"
#include "physical_operators/non_oblivious_sort_merge_join.h"
#include "physical_operators/project.h"
#include "physical_operators/sort.h"
#include "util.h"

#include "attestation.h"
#include "serialization.h"
#include <openenclave/attestation/sgx/evidence.h>

#include <mbedtls/config.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>

// This file contains definitions of the ecalls declared in enclave.edl. Errors
// originating within these ecalls are signaled by throwing a
// std::runtime_error, which is caught at the top level of the ecall (i.e.,
// within these definitions), and are then rethrown as Java exceptions using
// ocall_throw.

Crypto *g_crypto = CryptoContext::getInstance().crypto;

void ecall_set_debugging_level() {
  spdlog::set_pattern("[%l] [%T] | %s:%# in %!(): %v");
  spdlog::set_level(spdlog::level::debug);
}

void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext,
                   uint32_t cipher_length) {
  // Guard against encrypting or overwriting enclave memory
  assert(oe_is_outside_enclave(plaintext, plaintext_length) == 1);
  assert(oe_is_outside_enclave(ciphertext, cipher_length) == 1);
  __builtin_ia32_lfence();

  try {
    // IV (12 bytes) + ciphertext + mac (16 bytes)
    assert(cipher_length >= plaintext_length + CIPHER_IV_SIZE + CIPHER_TAG_SIZE);
    (void)cipher_length;
    (void)plaintext_length;
    g_crypto->SymEnc(shared_key, plaintext, NULL, ciphertext, plaintext_length, 0);
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

typedef struct oe_evidence_msg_t {
  uint8_t enc_public_key[CIPHER_PK_SIZE];
  uint8_t nonce[CIPHER_IV_SIZE];
  size_t evidence_size;
  uint8_t evidence[];
} oe_evidence_msg_t;


void ecall_finish_attestation(uint8_t *enc_signed_shared_key,
                              uint32_t enc_signed_shared_key_size) {
  spdlog::info("Ecall: finish_attestation()");
  try {
    // Asymmetrically decrypt the SignedKey
    size_t serialized_signed_shared_key_size = g_crypto->AsymDecSize(enc_signed_shared_key_size);
    std::unique_ptr<uint8_t[]> serialized_signed_shared_key(
        new uint8_t[serialized_signed_shared_key_size]);

    int err = g_crypto->AsymDec(enc_signed_shared_key, serialized_signed_shared_key.get(),
                                enc_signed_shared_key_size, &serialized_signed_shared_key_size);

    if (err) {
      ocall_throw("Shared key decryption failed");
    }

    // Deserialize to retrieve the shared key and the signature over the shared key
    const uint8_t *shared_key;
    size_t shared_key_size;
    const uint8_t *shared_key_sig;
    size_t shared_key_sig_size;

    deserializeSignedKey(serialized_signed_shared_key.get(), &shared_key, &shared_key_size,
                         &shared_key_sig, &shared_key_sig_size);

    // TODO: check shared key signature
    // To do so, we'll need to somehow obtain the client public key

    // Once we've decrypted the shared key, set it
    set_shared_key(shared_key, shared_key_size);

  } catch (const std::runtime_error &e) {
    ocall_throw(e.what());
  }
}

void ecall_generate_evidence(uint8_t **evidence_msg_data, size_t *evidence_msg_data_size) {
  spdlog::info("Ecall: generate_evidence()");
  // Determine the proper format settings for verification
  // TODO: eventually, the client should pass to the Opaque SQL enclaves the proper format
  // For now, we assume that the format settings for the client and Opaque SQL enclaves are
  // the same

  std::unique_ptr<Attestation> attestation(new Attestation(g_crypto));
  oe_uuid_t format_id = {OE_FORMAT_UUID_SGX_ECDSA};
  uint8_t *format_settings;
  size_t format_settings_size;

  attestation->GetFormatSettings(&format_id, &format_settings, &format_settings_size);

  // Get public key
  uint8_t public_key[CIPHER_PK_SIZE];
  g_crypto->WritePublicKey(public_key);

  // Generate nonce
  // TODO: nonce currently unused
  uint8_t nonce[CIPHER_IV_SIZE] = {0};

  // Buffer to hold generated evidence
  uint8_t *evidence = NULL;
  size_t evidence_size = 0;

#ifndef SIMULATE
  spdlog::info("Generating evidence for attestation");
  attestation->GenerateEvidence(&format_id, format_settings, &evidence, public_key, nonce,
                                format_settings_size, &evidence_size, CIPHER_PK_SIZE);
#endif

  // Allocate memory on host for attestation evidence and public key contained in struct
  // `oe_evidence_msg_t`
  oe_evidence_msg_t *evidence_msg = NULL;
  *evidence_msg_data_size = sizeof(oe_evidence_msg_t) + evidence_size;
  *evidence_msg_data = (uint8_t *)oe_host_malloc(*evidence_msg_data_size);
  if (*evidence_msg_data == NULL) {
    ocall_throw("Out of memory");
  }
  evidence_msg = (oe_evidence_msg_t *)(*evidence_msg_data);

  // Fill oe_evidence_msg_t with public key and generated evidence
  memcpy_s(evidence_msg->enc_public_key, sizeof(evidence_msg->enc_public_key), public_key,
           CIPHER_PK_SIZE);
  memcpy_s(evidence_msg->nonce, sizeof(evidence_msg->nonce), nonce, CIPHER_IV_SIZE);
  evidence_msg->evidence_size = evidence_size;
  if (evidence_size > 0) {
    memcpy_s(evidence_msg->evidence, evidence_size, evidence, evidence_size);
  }
}
