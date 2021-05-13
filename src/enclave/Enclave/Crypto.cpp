#include "Crypto.h"
#include "Random.h"

#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <iostream>

#include "Crypto.h"
#include "Random.h"
#include "common.h"
#include "util.h"
#include <unordered_map>
#include <vector>

// Set this number before creating the enclave
uint8_t num_clients = 1;

/**
 * Symmetric key used to encrypt row data. This key is shared among the driver
 * and all enclaves.
 *
 * The key is initially set on the driver, as the Scala byte array
 * edu.berkeley.cs.rise.opaque.Utils.sharedKey. It is securely sent to the
 * enclaves if attestation succeeds.
 */
unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};

std::unique_ptr<KeySchedule> ks;

// may user name to public key
std::unordered_map<std::string, std::unique_ptr<KeySchedule>> client_key_schedules;
std::unordered_map<std::string, std::vector<uint8_t>> client_keys;

void initKeySchedule() {
  // Use shared key to init key schedule
  ks.reset(new KeySchedule(reinterpret_cast<unsigned char *>(shared_key), SGX_AESGCM_KEY_SIZE));
}

void initKeySchedule(char* username) {
  std::string user(username);
  std::unique_ptr<KeySchedule> user_ks; 
  unsigned char client_key[SGX_AESGCM_KEY_SIZE];

  auto iter = client_keys.find(user);
  // if (iter == client_keys.end()) {
  //   ocall_throw("No client key for user: %s", username);
  // } else {
  memcpy(client_key, (uint8_t*) iter->second.data(), SGX_AESGCM_KEY_SIZE);
  // }

  user_ks.reset(new KeySchedule(reinterpret_cast<unsigned char *>(client_key), SGX_AESGCM_KEY_SIZE));
  client_key_schedules[user] = std::move(user_ks);
}

void add_client_key(uint8_t *client_key_bytes, uint32_t client_key_size, char* username) {
  if (client_key_size <= 0) {
    throw std::runtime_error("Add client key failed: Invalid client key size");
  }

  std::vector<uint8_t> user_private_key(client_key_bytes, client_key_bytes + client_key_size);
  std::string user(username);
  client_keys[user] = user_private_key;

  initKeySchedule(username);
}

void set_shared_key(uint8_t *shared_key_bytes, uint32_t shared_key_size) {
  if (shared_key_size <= 0) {
    throw std::runtime_error("Attempting to set a shared key with invalid key size.");
  }
  memcpy_s(shared_key, sizeof(shared_key), shared_key_bytes, shared_key_size);
  initKeySchedule();
}

void encrypt(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext) {

  if (!ks) {
    throw std::runtime_error("Cannot encrypt without a shared key. Ensure all "
                             "enclaves have completed attestation.");
  }

  uint8_t *iv_ptr = ciphertext;
  uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE;
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
      (sgx_aes_gcm_128bit_tag_t *)(ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);
  mbedtls_read_rand(reinterpret_cast<unsigned char *>(iv_ptr), SGX_AESGCM_IV_SIZE);

  AesGcm cipher(ks.get(), reinterpret_cast<uint8_t *>(iv_ptr), SGX_AESGCM_IV_SIZE);
  cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
  memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);

}

void encrypt_user(uint8_t *plaintext, uint32_t plaintext_length, uint8_t *ciphertext, const char * user_name) {

  for (auto& keypair : client_key_schedules) {

    if (strcmp(keypair.first.c_str(), user_name) == 0) {
      uint8_t *iv_ptr = ciphertext;
      uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE;
      sgx_aes_gcm_128bit_tag_t *mac_ptr =
          (sgx_aes_gcm_128bit_tag_t *)(ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);
      mbedtls_read_rand(reinterpret_cast<unsigned char *>(iv_ptr), SGX_AESGCM_IV_SIZE);

      AesGcm cipher(keypair.second.get(), reinterpret_cast<uint8_t *>(iv_ptr), SGX_AESGCM_IV_SIZE);
      cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
      memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);
      return;
    }
  }
  throw std::runtime_error("Couldn't find user and key");
}

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext) {

  if (!ks) {
    throw std::runtime_error("Cannot encrypt without a shared key. Ensure all "
                             "enclaves have completed attestation.");
  }

  uint32_t plaintext_length = dec_size(ciphertext_length);

  uint8_t *iv_ptr = (uint8_t *)ciphertext;
  uint8_t *ciphertext_ptr = (uint8_t *)(ciphertext + SGX_AESGCM_IV_SIZE);
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
      (sgx_aes_gcm_128bit_tag_t *)(ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);

  AesGcm decipher(ks.get(), iv_ptr, SGX_AESGCM_IV_SIZE);
  decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
  if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) != 0) {
    // Shared key doesn't work
    // Perhaps we need to use a client key instead
    int success = -1;
    for (auto& keypair : client_key_schedules) {
      std::vector<uint8_t> print_key = client_keys[keypair.first];
      for(size_t i=0; i < print_key.size(); i++)
        std::cout << print_key.at(i) << ' ';
      std::cout << std::endl;

      AesGcm decipher(keypair.second.get(), iv_ptr, SGX_AESGCM_IV_SIZE);
      decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
      if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) == 0) {
          std::cout << "We found the proper key, of user " << keypair.first << std::endl;
          success = 0;
          break;
      }
    }

    if (success == -1) {
        throw std::runtime_error("Couldn't decrypt -- proper key unknown\n");
    }
  }
}

uint32_t enc_size(uint32_t plaintext_size) {
  return plaintext_size + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
}

uint32_t dec_size(uint32_t ciphertext_size) {
  return ciphertext_size - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
}
