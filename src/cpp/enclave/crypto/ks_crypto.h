#include "crypto.h"
#include "sgxaes.h"

#ifndef CRYPTO_H
#define CRYPTO_H

#define SGX_ECP256_KEY_SIZE 32
typedef struct _sgx_ec256_public_t {
  uint8_t gx[SGX_ECP256_KEY_SIZE];
  uint8_t gy[SGX_ECP256_KEY_SIZE];
} sgx_ec256_public_t;

extern unsigned char shared_key[CIPHER_KEY_SIZE];

/**
 * The public key of the Service Provider, used in remote attestation. This is
 * automatically hardcoded into the enclave during the build process.
 */
extern const sgx_ec256_public_t g_sp_pub_key;

/**
 * Set the symmetric key used to encrypt row data using message 4 of the remote
 * attestation process.
 */
void set_shared_key(uint8_t *msg4, uint32_t msg4_size);

#endif
