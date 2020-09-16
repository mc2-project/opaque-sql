#ifndef SERVICE_PROVIDER_H
#define SERVICE_PROVIDER_H

#include <string>
#include <memory>

#include "sp_crypto.h"
#include "iasrequest.h"

class ServiceProvider {
public:
  ServiceProvider(const std::string &spid, bool is_production, bool linkable_signature)
    : spid(spid), is_production(is_production), linkable_signature(linkable_signature),
      ias_api_version(3), require_attestation(std::getenv("OPAQUE_REQUIRE_ATTESTATION")) {}

  /** Load an OpenSSL private key from the specified file. */
  void load_private_key(const std::string &filename);

  /**
   * Set the symmetric key to send to the enclave. This key is securely sent to the enclaves if
   * attestation succeeds.
   */
  void set_shared_key(const uint8_t *shared_key);

  /**
   * After calling load_private_key, write the corresponding public key as a C++ header file. This
   * file should be compiled into the enclave.
   */
  void export_public_key_code(const std::string &filename);

  /**
   * Process attestation report from an enclave, verify the report, and send the shared key to the enclave
   */
  std::unique_ptr<oe_shared_key_msg_t> process_enclave_report(oe_report_msg_t *report_msg, uint32_t *shared_key_msg_size);

private:
  void connect_to_ias_helper(const std::string &ias_report_signing_ca_file);

  lc_ec256_public_t sp_pub_key;
  lc_ec256_private_t sp_priv_key;
  
  uint8_t shared_key[LC_AESGCM_KEY_SIZE];
  std::string spid;

  std::unique_ptr<IAS_Connection> ias;
  bool is_production;
  bool linkable_signature;
  uint16_t ias_api_version;

  bool require_attestation;
};

extern ServiceProvider service_provider;

#endif
