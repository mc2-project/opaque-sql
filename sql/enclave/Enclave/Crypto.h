#include "sgx_tcrypto.h"
#include "sgx_trts.h"
#include "util.h"

#ifndef CRYPTO_H
#define CRYPTO_H

extern const char *key_str;
extern const sgx_aes_gcm_128bit_key_t *key;

// encrypt() and decrypt() should be called from enclave code only

// encrypt using a global key
// TODO: fix this; should use key obtained from client 
void encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext);

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext);

uint32_t enc_size(uint32_t plaintext_size);

#endif
