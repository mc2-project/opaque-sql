#include "Crypto.h"

#include <sgx_trts.h>

#include "common.h"

sgx_aes_gcm_128bit_key_t key_data = {0};
sgx_aes_gcm_128bit_key_t *key = &key_data;
const KeySchedule ks_backup = KeySchedule((unsigned char *) key_data, SGX_AESGCM_KEY_SIZE);
KeySchedule *ks = (KeySchedule *) &ks_backup;

void initKeySchedule() {
  if (ks == NULL) {
    ks = new KeySchedule((unsigned char *) key_data, SGX_AESGCM_KEY_SIZE);
  }
}

// encrypt() and decrypt() should be called from enclave code only
// TODO: encrypt() and decrypt() should return status

// encrypt using a global key
// TODO: fix this; should use key obtained from client
void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
             uint8_t *ciphertext) {

  initKeySchedule();

  // key size is 16 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint8_t *iv_ptr = ciphertext;
  uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE;
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
    (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);

  // generate random IV
  sgx_read_rand(iv_ptr, SGX_AESGCM_IV_SIZE);

  AesGcm cipher(ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
  memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);
}


void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length,
             uint8_t *plaintext) {

  initKeySchedule();

  // decrypt using a global key
  // TODO: fix this; should use key obtained from client

  // key size is 16 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint32_t plaintext_length = ciphertext_length - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;

  uint8_t *iv_ptr = (uint8_t *) ciphertext;
  uint8_t *ciphertext_ptr = (uint8_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
    (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);

  AesGcm decipher(ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
  if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) != 0) {
    printf("Decrypt: invalid mac\n");
  }
}

uint32_t enc_size(uint32_t plaintext_size) {
  return plaintext_size + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
}

uint32_t dec_size(uint32_t ciphertext_size) {
  return ciphertext_size - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
}
