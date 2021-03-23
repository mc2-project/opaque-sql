#ifndef SERVICE_PROVIDER_H
#define SERVICE_PROVIDER_H

#include <memory>
#include <string>

#include "iasrequest.h"
#include "sp_crypto.h"

class ServiceProvider {
public:
  ServiceProvider(const std::string &spid, bool is_production, bool linkable_signature)
      : spid(spid), is_production(is_production), linkable_signature(linkable_signature),
        ias_api_version(3), require_attestation(std::getenv("OPAQUE_REQUIRE_ATTESTATION")) {}

  /** Load an OpenSSL private key from the specified file. */
  void load_private_key(const std::string &filename);

  /**
   * Set the symmetric key to send to the enclave. This key is securely sent to
   * the enclaves if attestation succeeds.
   */
  void set_shared_key(const uint8_t *shared_key);
  
  // FOR TESTING PURPOSES
  // FIXME: remove this function
  // void set_test_key(const uint8_t *shared_key);

  void set_user_cert(std::string user_cert);

  void set_key_share(const uint8_t *key_share);

  /**
   * After calling load_private_key, write the corresponding public key as a C++
   * header file. This file should be compiled into the enclave.
   */
  void export_public_key_code(const std::string &filename);

  /**
   * Process attestation report from an enclave, verify the report, and send the
   * shared key to the enclave
   */
  std::unique_ptr<oe_shared_key_msg_t> process_enclave_report(oe_report_msg_t *report_msg,
                                                              uint32_t *shared_key_msg_size);

  /**
   * Functions to help enclaves generate shared key (public key verification specifically)
   */
  EVP_PKEY* buffer_to_public_key_wrapper(char* public_key);

  int public_encrypt_wrapper(EVP_PKEY* key, unsigned char * data, int data_len, unsigned char* encrypted, size_t* encrypted_len);

private:
  void connect_to_ias_helper(const std::string &ias_report_signing_ca_file);

  lc_ec256_public_t sp_pub_key;
  lc_ec256_private_t sp_priv_key;

  uint8_t shared_key[LC_AESGCM_KEY_SIZE];

  // FOR TESTING PURPOSES
  // FIXME: remove this test key
  // uint8_t test_key[LC_AESGCM_KEY_SIZE];

  // FIXME: make this not a set length
  char user_cert[2000];

  // Key share; xor'ed among all parties to produce one shared key for spark cluster
  uint8_t key_share[LC_AESGCM_KEY_SIZE];
  std::string spid;

  std::unique_ptr<IAS_Connection> ias;
  bool is_production;
  bool linkable_signature;
  uint16_t ias_api_version;

  bool require_attestation;
};

extern ServiceProvider service_provider;

#endif
