#include <sgxaes.h>

#ifndef CRYPTO_H
#define CRYPTO_H

#define SGX_ECP256_KEY_SIZE             32
typedef struct _sgx_ec256_public_t
{
    uint8_t gx[SGX_ECP256_KEY_SIZE];
    uint8_t gy[SGX_ECP256_KEY_SIZE];
} sgx_ec256_public_t;

/**
 * The public key of the Service Provider, used in remote attestation. This is automatically
 * hardcoded into the enclave during the build process.
 */
extern const sgx_ec256_public_t g_sp_pub_key;

/**
 * Set the symmetric key used to encrypt row data using message 4 of the remote attestation process.
 */
void set_shared_key(uint8_t *msg4, uint32_t msg4_size);

/**
 * Encrypt the given plaintext using AES-GCM with a 128-bit key and write the result to
 * `ciphertext`. The encrypted data will be formatted as follows, where || denotes concatenation:
 *
 * IV || ciphertext || MAC
 *
 * The IV is 12 bytes (96 bits). The key is 16 bytes (128 bits).  The MAC is 16 bytes (128 bits).
 *
 * A random IV will be used.
 */
void encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext);

/**
 * Decrypt the given ciphertext using AES-GCM with a 128-bit key and write the result to
 * `plaintext`. The encrypted data must be formatted as described in the documentation for
 * `encrypt`.
 */
void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext);

/** Calculate how many bytes `encrypt` will write if invoked on plaintext of the given length. */
uint32_t enc_size(uint32_t plaintext_size);

/** Calculate how many bytes `decrypt` will write if invoked on ciphertext of the given length. */
uint32_t dec_size(uint32_t ciphertext_size);

#endif
