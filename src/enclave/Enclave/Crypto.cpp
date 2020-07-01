#include "Crypto.h"
#include "Random.h"

#include <stdexcept>
// #include <sgx_trts.h>
// #include <sgx_tkey_exchange.h>

#include "common.h"
#include "util.h"
#include <unordered_map>
#include <vector>
#include <iostream>
//#include "rdrand.h"

// Set this number before creating the enclave
uint8_t num_clients = 1;

/**
 * Symmetric key used to encrypt row data. This key is shared among the driver and all enclaves.
 *
 * The key is initially set on the driver, as the Scala byte array
 * edu.berkeley.cs.rise.opaque.Utils.sharedKey. It is securely sent to the enclaves if attestation
 * succeeds.
 */
unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};

// map username to client key schedule
std::unordered_map<std::string, std::unique_ptr<KeySchedule>> client_key_schedules;
std::unordered_map<std::string, std::vector<uint8_t>> client_keys;
// std::unordered_map<std::string, unsigned char shared_key[SGX_AESGCM_KEY_SIZE]> client_keys;

// map user name to public key
// std::unordered_map<std::string, std::vector<uint8_t>> client_public_keys;

// TODO: properly set this KeySchedule
std::unique_ptr<KeySchedule> ks;

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

void initKeySchedule() {
  ks.reset(new KeySchedule(reinterpret_cast<unsigned char *>(shared_key), SGX_AESGCM_KEY_SIZE));
}

void add_client_key(uint8_t *client_key_bytes, uint32_t client_key_size, char* username) {
  if (client_key_size <= 0) {
    throw std::runtime_error("Remote attestation step 2: Invalid client key size");
  }

  std::vector<uint8_t> user_private_key(client_key_bytes, client_key_bytes + client_key_size);
  std::string user(username);
  client_keys[user] = user_private_key;

  initKeySchedule(username);

}

void xor_shared_key(uint8_t *key_share_bytes, uint32_t key_share_size) {
    if (key_share_size <= 0 || key_share_size != SGX_AESGCM_KEY_SIZE) {
      throw std::runtime_error("Remote attestation step 2: Invalid key share size.");
    }

    // XOR key shares
    unsigned char xor_key[SGX_AESGCM_KEY_SIZE];
    int i;
    for (i = 0; i < SGX_AESGCM_KEY_SIZE; i++) {
        xor_key[i] = shared_key[i] ^ key_share_bytes[i];
    }
    memcpy(shared_key, xor_key, SGX_AESGCM_KEY_SIZE);

    // initKeySchedule the shared key if this is the last client
    if (client_keys.size() == num_clients) {
        initKeySchedule();
    }
}


// void get_client_key(uint8_t* key, char *username) {
//     LOG(DEBUG) << "Getting client key for user: " << username;
//     std::string str(username);
//     auto iter = client_keys.find(str);
//     if (iter == client_keys.end()) {
//         LOG(FATAL) << "No client key for user: " << username;
//     } else {
//         memcpy(key, (uint8_t*) iter->second.data(), CIPHER_KEY_SIZE);
//     }
// }
// 
// char* get_client_cert(char *username) {
//     LOG(DEBUG) << "Getting username " << username;
//     std::string str(username);
//     auto iter = client_public_keys.find(str);
//     if (iter == client_public_keys.end()) {
//         LOG(FATAL) << "No certificate for user: " << username;
//     } else {
//         return (char*) iter->second.data();
//     }
// }

void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
             uint8_t *ciphertext, char* username) {

  std::cout << "C++ encrypting inside enclave\n";
  
  if (!client_key_schedules[std::string(username)]) {
    throw std::runtime_error(
      "Cannot encrypt without a shared key. Ensure all enclaves have completed attestation.");
  }

  uint8_t *iv_ptr = ciphertext;
  uint8_t *ciphertext_ptr = ciphertext + SGX_AESGCM_IV_SIZE;
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
    (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);
  // oe_get_entropy(iv_ptr, SGX_AESGCM_IV_SIZE);
  // sgx_read_rand(iv_ptr, SGX_AESGCM_IV_SIZE);
  mbedtls_read_rand(reinterpret_cast<unsigned char*>(iv_ptr), SGX_AESGCM_IV_SIZE);

  if (username == NULL) {
    AesGcm cipher(ks.get(), reinterpret_cast<uint8_t*>(iv_ptr), SGX_AESGCM_IV_SIZE);
    cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
    memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);
    std::cout << "Encrypting with xor shared key\n";
  } else {
    AesGcm cipher(client_key_schedules[std::string(username)].get(), reinterpret_cast<uint8_t*>(iv_ptr), SGX_AESGCM_IV_SIZE);
    cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
    memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);
    std::cout << "Encrypting with client key\n";
  }
}

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext, char* username) {
  std::cout << "C++ decrypting inside enclave\n";
  // if (!ks) {
    // throw std::runtime_error(
      // "Cannot encrypt without a shared key. Ensure all enclaves have completed attestation.");
  // }
  uint32_t plaintext_length = dec_size(ciphertext_length);

  uint8_t *iv_ptr = (uint8_t *) ciphertext;
  uint8_t *ciphertext_ptr = (uint8_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
    (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);

  if (username == NULL) {
    AesGcm decipher(ks.get(), iv_ptr, SGX_AESGCM_IV_SIZE);
    decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
    if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) != 0) {
      printf("User name is null, Decrypt: invalid mac\n");
    }
    std::cout << "Decrypting with xored shared key\n";
  } else {
    AesGcm decipher(client_key_schedules[std::string(username)].get(), iv_ptr, SGX_AESGCM_IV_SIZE);
    decipher.decrypt(ciphertext_ptr, plaintext_length, plaintext, plaintext_length);
    if (memcmp(mac_ptr, decipher.tag().t, SGX_AESGCM_MAC_SIZE) != 0) {
      printf("User name not null, Decrypt: invalid mac\n");
    }
    std::cout << "Decrypting with client key\n";
  }
}

uint32_t enc_size(uint32_t plaintext_size) {
  return plaintext_size + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
}

uint32_t dec_size(uint32_t ciphertext_size) {
  return ciphertext_size - (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE);
}
