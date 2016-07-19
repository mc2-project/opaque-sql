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


// this class provides support for stream encrypting
// [ciphertext length][ciphertext IV][ciphertext MAC][ciphertext]
class StreamCipher {

 public:
  StreamCipher(uint8_t *ciphertext_ptr);

  ~StreamCipher();

  void encrypt(uint8_t *plaintext, uint32_t size);

  void reset(uint8_t *new_ciphertext_ptr);

  void finish();

  uint32_t bytes_written();

  uint32_t ciphertext_size;

  uint8_t *iv_ptr;
  uint8_t *mac_ptr;
  
  uint8_t *cipher_ptr;
  uint8_t *current_cipher_ptr;
  uint8_t leftover_plaintext[AES_BLOCK_SIZE];
  uint32_t leftover_plaintext_size;

  AesGcm *cipher;
};


// Given a ciphertext, stream decipher into different plaintext
// [ciphertext length][ciphertext IV][ciphertext MAC][ciphertext]
// no bounds checking
class StreamDecipher {

 public:
  StreamDecipher(uint8_t *ciphertext_ptr, uint32_t enc_size);

  ~StreamDecipher();

  void decrypt(uint8_t *plaintext_ptr, uint32_t size);

  void reset(uint8_t *new_ciphertext_ptr, uint32_t enc_size);

  uint8_t *iv_ptr;
  uint8_t *mac_ptr;
  
  uint8_t *cipher_ptr;
  uint8_t *current_cipher_ptr;
  uint8_t leftover_plaintext[AES_BLOCK_SIZE];
  uint8_t *leftover_plaintext_ptr;
  uint32_t leftover_plaintext_size;

  AesGcm *cipher;
  uint32_t total_cipher_size;
};


#endif
