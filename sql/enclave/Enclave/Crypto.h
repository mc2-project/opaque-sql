#include "sgx_tcrypto.h"
#include "sgx_trts.h"
#include "util.h"
#include "sgxaes.h"
#include <string.h>

#ifndef CRYPTO_H
#define CRYPTO_H

extern const char *key_str;
extern const sgx_aes_gcm_128bit_key_t *key;
extern const KeySchedule ks;

// encrypt() and decrypt() should be called from enclave code only

// encrypt using a global key
// TODO: fix this; should use key obtained from client
void encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext);

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext);

uint32_t enc_size(uint32_t plaintext_size);
uint32_t dec_size(uint32_t ciphertext_size);

void test_big_encrypt();
void test_small_encrypts();

#endif
