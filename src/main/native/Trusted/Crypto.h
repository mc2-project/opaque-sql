#include <sgx_tcrypto.h>
#include "sgxaes.h"

#ifndef CRYPTO_H
#define CRYPTO_H

//extern const char *key_str;
extern sgx_aes_gcm_128bit_key_t key_data;
extern sgx_aes_gcm_128bit_key_t *key;

extern KeySchedule *ks;

// encrypt using a global key
// TODO: fix this; should use key obtained from client
void encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext);

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext);

uint32_t enc_size(uint32_t plaintext_size);
uint32_t dec_size(uint32_t ciphertext_size);

#endif
