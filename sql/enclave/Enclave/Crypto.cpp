#include "Crypto.h"

const char *key_str = "helloworld123123";
const sgx_aes_gcm_128bit_key_t *key = (const sgx_aes_gcm_128bit_key_t *) key_str;
const KeySchedule ks = KeySchedule((unsigned char *) key_str, SGX_AESGCM_KEY_SIZE);

// encrypt() and decrypt() should be called from enclave code only
// TODO: encrypt() and decrypt() should return status

// Encrypt and decrypt are no-ops for performance measurement
void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
             uint8_t *ciphertext) {
  memcpy(ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE, plaintext, plaintext_length);
}

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length,
             uint8_t *plaintext) {
  memcpy(plaintext, ciphertext + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE,
         ciphertext_length - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE));
}


uint32_t enc_size(uint32_t plaintext_size) {
  return plaintext_size + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
}

uint32_t dec_size(uint32_t ciphertext_size) {
  return ciphertext_size - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
}


StreamCipher::StreamCipher(uint8_t *ciphertext_ptr) {
  
  iv_ptr = ciphertext_ptr + 4;
  mac_ptr = ciphertext_ptr + 4 + SGX_AESGCM_IV_SIZE;
  cipher_ptr = ciphertext_ptr + 4 + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
  current_cipher_ptr = cipher_ptr;
  
  sgx_read_rand(iv_ptr, SGX_AESGCM_IV_SIZE);
  
  cipher = new AesGcm(&ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  leftover_plaintext_size = 0;
}


StreamCipher::~StreamCipher() {
  delete cipher;
}


void StreamCipher::encrypt(uint8_t *plaintext, uint32_t size, bool if_final) {

  uint32_t merge_bytes = 0;
  uint32_t copy_bytes = 0;

  if (leftover_plaintext_size > 0) {
	copy_bytes = (leftover_plaintext_size + size <= AES_BLOCK_SIZE) ? size : AES_BLOCK_SIZE - leftover_plaintext_size;
	cpy(leftover_plaintext + leftover_plaintext_size, plaintext, copy_bytes);
	merge_bytes = leftover_plaintext_size + copy_bytes;
  }

  // if necessary, encrypt leftover bytes first
  if (merge_bytes > 0) {
	cipher->encrypt(leftover_plaintext, merge_bytes, current_cipher_ptr, merge_bytes);
	current_cipher_ptr += merge_bytes;
  }
	  
  // otherwise, encrypt in blocks
  uint32_t new_leftover_size = (size - copy_bytes) % AES_BLOCK_SIZE;
  uint32_t stream_enc_size = (size - copy_bytes) / AES_BLOCK_SIZE * AES_BLOCK_SIZE;

  if (stream_enc_size > 0) {
	cipher->encrypt(plaintext + copy_bytes, stream_enc_size, current_cipher_ptr, stream_enc_size);
	current_cipher_ptr += stream_enc_size;
  }
  
  // copy leftover size to leftover_plaintext
  if (new_leftover_size > 0) {
	cpy(leftover_plaintext, plaintext + copy_bytes + stream_enc_size, new_leftover_size);
	leftover_plaintext_size = new_leftover_size;
  }

  if (if_final) {
	// if there are leftover bytes, encrypt those as well
	cipher->encrypt(leftover_plaintext, new_leftover_size, current_cipher_ptr, new_leftover_size);
	current_cipher_ptr += new_leftover_size;
	
	*( (uint32_t *) (iv_ptr - 4)) = (current_cipher_ptr - cipher_ptr + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
  }
}



StreamDecipher::StreamDecipher(uint8_t *ciphertext_ptr) {

  total_cipher_size = *( (uint32_t *) ciphertext_ptr) - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;

  iv_ptr = ciphertext_ptr + 4;
  mac_ptr = ciphertext_ptr + 4 + SGX_AESGCM_IV_SIZE;
  cipher_ptr = ciphertext_ptr + 4 + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
  current_cipher_ptr = cipher_ptr;

  cipher = new AesGcm(&ks, iv_ptr, SGX_AESGCM_IV_SIZE);
  leftover_plaintext_size = 0;
  leftover_plaintext_ptr = leftover_plaintext;

}

StreamDecipher::~StreamDecipher() {
  delete cipher;
}

void StreamDecipher::decrypt(uint8_t *plaintext_ptr, uint32_t size) {

  uint32_t copied_bytes = 0;

  if (leftover_plaintext_size >= size) {
	cpy(plaintext_ptr, leftover_plaintext_ptr, size);
	leftover_plaintext_ptr += size;
	leftover_plaintext_size -= size;
	return;
  }
  
  // if there are bytes left over from leftover_plaintext, copy that first
  if (leftover_plaintext_size > 0) {
	cpy(plaintext_ptr, leftover_plaintext_ptr, leftover_plaintext_size);
	copied_bytes = leftover_plaintext_size;
  }

  leftover_plaintext_ptr = leftover_plaintext;
  leftover_plaintext_size = 0;

  // decrypt (size - copied_bytes), up to AES_BLOCK_SIZE
  uint32_t decrypt_bytes = (size - copied_bytes) / AES_BLOCK_SIZE * AES_BLOCK_SIZE;
  if (decrypt_bytes > 0) {
	cipher->decrypt(current_cipher_ptr, decrypt_bytes, plaintext_ptr + copied_bytes, decrypt_bytes);
	current_cipher_ptr += decrypt_bytes;
  }

  uint32_t final_size = (size - copied_bytes) % AES_BLOCK_SIZE;
  total_cipher_size = total_cipher_size - copied_bytes - decrypt_bytes;

  if (total_cipher_size > AES_BLOCK_SIZE) {
	// decrypt AES_BLOCK_SIZE into leftover_plaintext
	cipher->decrypt(current_cipher_ptr, AES_BLOCK_SIZE, leftover_plaintext, AES_BLOCK_SIZE);
	leftover_plaintext_size = AES_BLOCK_SIZE;
	current_cipher_ptr += AES_BLOCK_SIZE;
  } else {
	// decrypt all the rest of the bytes into leftover_plaintext
	cipher->decrypt(current_cipher_ptr, total_cipher_size, leftover_plaintext, total_cipher_size);
	leftover_plaintext_size = total_cipher_size;
	current_cipher_ptr += total_cipher_size;
  }

  // copy final_size 
  cpy(plaintext_ptr + copied_bytes + decrypt_bytes, leftover_plaintext_ptr, final_size);
  leftover_plaintext_ptr += final_size;
  leftover_plaintext_size -= final_size;
}
