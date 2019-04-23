#ifndef SERVICE_PROVIDER_H
#define SERVICE_PROVIDER_H

#include <string>
#include <memory>
#include <sgx_key_exchange.h>

#include "sp_crypto.h"
#include "iasrequest.h"

typedef struct _sample_ps_sec_prop_desc_t {
  uint8_t  sample_ps_sec_prop_desc[256];
} sample_ps_sec_prop_desc_t;

typedef struct _sp_db_item_t {
  sgx_epid_group_id_t           gid;
  lc_ec256_public_t             g_a;
  lc_ec256_public_t             g_b;
  lc_ec256_private_t            b;
  lc_aes_gcm_128bit_key_t       vk_key;   // Shared secret key for the REPORT_DATA
  lc_aes_gcm_128bit_key_t       mk_key;   // Shared secret key for generating MAC's
  lc_aes_gcm_128bit_key_t       sk_key;   // Shared secret key for encryption
  lc_aes_gcm_128bit_key_t       smk_key;  // Used only for SIGMA protocol
  sample_ps_sec_prop_desc_t     ps_sec_prop;
} sp_db_item_t;

class ServiceProvider {
public:
  ServiceProvider(const std::string &spid, bool is_production)
    : spid(spid), is_production(is_production), ias_api_version(3) {
    ensure_ias_connection();
  }

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

  void process_msg0(uint32_t extended_epid_group_id);

  std::unique_ptr<sgx_ra_msg2_t> process_msg1(sgx_ra_msg1_t *msg1, uint32_t *msg2_size);

  /**
   * Process attestation message 3 from an enclave and generate message 4 for that enclave. Message
   * 4 contains the shared secret required for the enclave to decrypt data.
   *
   * If force_accept is true, then the attestation result will be ignored and the shared secret will
   * be sent unconditionally. This is useful for development when running on simulated enclaves,
   * which are expected to fail attestation.
   */
  std::unique_ptr<ra_msg4_t> process_msg3(
    sgx_ra_msg3_t *msg3, uint32_t msg3_size, bool force_accept, uint32_t *msg4_size);

private:

  void ensure_ias_connection();

  sgx_ec256_public_t sp_pub_key;
  sgx_ec256_private_t sp_priv_key;
  uint8_t shared_key[LC_AESGCM_KEY_SIZE];
  sp_db_item_t sp_db;
  std::string spid;

  std::unique_ptr<IAS_Connection> ias;
  bool is_production;
  uint16_t ias_api_version;
};

extern ServiceProvider service_provider;

#endif
