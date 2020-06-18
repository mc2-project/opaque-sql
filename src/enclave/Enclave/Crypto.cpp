#include "Crypto.h"
#include "Random.h"

#include <stdexcept>
// #include <sgx_trts.h>
// #include <sgx_tkey_exchange.h>

#include "common.h"
#include "util.h"
//#include "rdrand.h"

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
std::unordered_map<std::string, unsigned char shared_key[SGX_AESGCM_KEY_SIZE]> client_keys;

// map user name to public key
// std::unordered_map<std::string, std::vector<uint8_t>> client_public_keys;

// std::unique_ptr<KeySchedule> ks;

void initKeySchedule(char* username) {
  std::unique_ptr<KeySchedule> ks = client_key_schedule[username];
  ks.reset(new KeySchedule(reinterpret_cast<unsigned char *>(shared_key), SGX_AESGCM_KEY_SIZE));
}

void set_shared_key(uint8_t *shared_key_bytes, uint32_t shared_key_size, char* username) {
  if (shared_key_size <= 0) {
    throw std::runtime_error("Remote attestation step 4: Invalid message size.");
  }
  memcpy_s(client_keys[username], sizeof(client_keys[username]), shared_key_bytes, shared_key_size);

  initKeySchedule(username);
}

void get_client_key(uint8_t* key, char *username) {
    LOG(DEBUG) << "Getting client key for user: " << username;
    std::string str(username);
    auto iter = client_keys.find(str);
    if (iter == client_keys.end()) {
        LOG(FATAL) << "No client key for user: " << username;
    } else {
        memcpy(key, (uint8_t*) iter->second.data(), CIPHER_KEY_SIZE);
    }
}

char* get_client_cert(char *username) {
    LOG(DEBUG) << "Getting username " << username;
    std::string str(username);
    auto iter = client_public_keys.find(str);
    if (iter == client_public_keys.end()) {
        LOG(FATAL) << "No certificate for user: " << username;
    } else {
        return (char*) iter->second.data();
    }
}

void encrypt(uint8_t *plaintext, uint32_t plaintext_length,
             uint8_t *ciphertext) {

  if (!ks) {
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

  // TODO: should we replace this encryption with the mbedtls encryption
  AesGcm cipher(ks.get(), reinterpret_cast<uint8_t*>(iv_ptr), SGX_AESGCM_IV_SIZE);
  cipher.encrypt(plaintext, plaintext_length, ciphertext_ptr, plaintext_length);
  memcpy(mac_ptr, cipher.tag().t, SGX_AESGCM_MAC_SIZE);
}

void decrypt(const uint8_t *ciphertext, uint32_t ciphertext_length, uint8_t *plaintext) {
  if (!ks) {
    throw std::runtime_error(
      "Cannot encrypt without a shared key. Ensure all enclaves have completed attestation.");
  }
  uint32_t plaintext_length = dec_size(ciphertext_length);

  uint8_t *iv_ptr = (uint8_t *) ciphertext;
  uint8_t *ciphertext_ptr = (uint8_t *) (ciphertext + SGX_AESGCM_IV_SIZE);
  sgx_aes_gcm_128bit_tag_t *mac_ptr =
    (sgx_aes_gcm_128bit_tag_t *) (ciphertext + SGX_AESGCM_IV_SIZE + plaintext_length);

  AesGcm decipher(ks.get(), iv_ptr, SGX_AESGCM_IV_SIZE);
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
