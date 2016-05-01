#include "Crypto.h"

const char *key_str = "helloworld123123";
const sgx_aes_gcm_128bit_key_t *key = (const sgx_aes_gcm_128bit_key_t *) key_str;
const KeySchedule ks = KeySchedule((unsigned char *) key_str, SGX_AESGCM_KEY_SIZE);

// encrypt() and decrypt() should be called from enclave code only
// TODO: encrypt() and decrypt() should return status

// encrypt using a global key
// TODO: fix this; should use key obtained from client 
void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
			 uint8_t *ciphertext) {
  
  // key size is 12 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint8_t *iv_ptr = ciphertext;
  // generate random IV
  sgx_read_rand(iv_ptr, SGX_AESGCM_IV_SIZE);
  sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;


  AesGcm cipher(&ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
  memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);

  // //sgx_status_t rand_status = sgx_read_rand(iv, SGX_AESGCM_IV_SIZE);
  // sgx_status_t status = sgx_rijndael128GCM_encrypt(key,
  // 						   plaintext, plaintext_length,
  // 						   ciphertext_ptr,
  // 						   iv_ptr, SGX_AESGCM_IV_SIZE,
  // 						   NULL, 0,
  // 						   mac_ptr);
 
  // switch(status) {
  // case SGX_ERROR_INVALID_PARAMETER:
  //   break;
  // case SGX_ERROR_OUT_OF_MEMORY:
  //   break;
  // case SGX_ERROR_UNEXPECTED:
  //   break;
  // }

}


void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, 
			 uint8_t *plaintext) {

  // decrypt using a global key
  // TODO: fix this; should use key obtained from client 
  
  // key size is 12 bytes/128 bits
  // IV size is 12 bytes/96 bits
  // MAC size is 16 bytes/128 bits

  // one buffer to store IV (12 bytes) + ciphertext + mac (16 bytes)

  uint32_t plaintext_length = ciphertext_length - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;

  uint8_t *iv_ptr = (uint8_t *) ciphertext;
  sgx_aes_gcm_128bit_tag_t *mac_ptr = (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  uint8_t *ciphertext_ptr = (uint8_t *) (ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);

  AesGcm decipher(&ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
  if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) != 0) {
    printf("Decrypt: invalid mac\n");
  }

  //printf("Decrypt: ciphertext_length is %u\n", ciphertext_length);
  // sgx_status_t status = sgx_rijndael128GCM_decrypt(key,
  // 						   ciphertext_ptr, plaintext_length,
  // 						   plaintext,
  // 						   iv_ptr, SGX_AESGCM_IV_SIZE,
  // 						   NULL, 0,
  // 						   mac_ptr);
  
  // if (status != SGX_SUCCESS) {
  //   switch(status) {
  //   case SGX_ERROR_INVALID_PARAMETER:
  //     printf("Decrypt: invalid parameter\n");
  //     break;

  //   case SGX_ERROR_OUT_OF_MEMORY:
  //     printf("Decrypt: out of enclave memory\n");
  //     break;

  //   case SGX_ERROR_UNEXPECTED:
  //     printf("Decrypt: unexpected error\n");
  //     break;

  //   case SGX_ERROR_MAC_MISMATCH:
  //     printf("Decrypt: MAC mismatch\n");
  //     break;

  //   default:
  //     printf("Decrypt: other error %#08x\n", status);
  //   }
  // }
}


uint32_t enc_size(uint32_t plaintext_size) {
  return plaintext_size + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
}

uint32_t dec_size(uint32_t ciphertext_size) {
  return ciphertext_size - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
}
