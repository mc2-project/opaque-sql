#include <stdexcept>

#include "common.h"
#include "ks_crypto.h"
#include "random.h"
#include "util.h"

/**
 * Symmetric key used to encrypt row data. This key is shared among the driver
 * and all enclaves.
 *
 * The key is initially set on the driver, as the Scala byte array
 * edu.berkeley.cs.rise.opaque.Utils.sharedKey. It is securely sent to the
 * enclaves if attestation succeeds.
 */
unsigned char shared_key[CIPHER_KEY_SIZE] = {0};

std::unique_ptr<KeySchedule> ks;

void initKeySchedule() {
  ks.reset(new KeySchedule(reinterpret_cast<unsigned char *>(shared_key), CIPHER_KEY_SIZE));
}

void set_shared_key(uint8_t *shared_key_bytes, uint32_t shared_key_size) {
  if (shared_key_size <= 0) {
    throw std::runtime_error("Attempting to set a shared key with invalid key size.");
  }
  memcpy_s(shared_key, sizeof(shared_key), shared_key_bytes, shared_key_size);

  initKeySchedule();
}
