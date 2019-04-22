#include <openssl/pem.h>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include <iomanip>
#include <sgx_tcrypto.h>

#include "ecp.h"

#include "ServiceProvider.h"

void ServiceProvider::load_private_key(const std::string &filename) {
  FILE *private_key_file = fopen(filename.c_str(), "r");
  if (private_key_file == nullptr) {
    throw std::runtime_error(
      std::string("Error: Private key file '")
      + filename
      + std::string("' does not exist. Set environment variable $PRIVATE_KEY_PATH."));
  }

  EVP_PKEY *pkey = PEM_read_PrivateKey(private_key_file, NULL, NULL, NULL);
  if (pkey == nullptr) {
    throw std::runtime_error(
      "Unable to read private key from '"
      + filename
      + std::string("'."));
  }

  EC_KEY *ec_key = EVP_PKEY_get1_EC_KEY(pkey);
  assert(ec_key != nullptr);

  BIGNUM *x_ec = BN_new();
  BIGNUM *y_ec = BN_new();
  assert(EC_POINT_get_affine_coordinates_GFp(group, point, x_ec, y_ec, nullptr) == 0);

  const BIGNUM *priv_bn = EC_KEY_get0_private_key(ec_key);

  // Store the public and private keys in binary format
  std::unique_ptr<uint8_t> x_(new uint8_t[LC_ECP256_KEY_SIZE]);
  std::unique_ptr<uint8_t> y_(new uint8_t[LC_ECP256_KEY_SIZE]);
  std::unique_ptr<uint8_t> r_(new uint8_t[LC_ECP256_KEY_SIZE]);

  std::unique_ptr<uint8_t> x(new uint8_t[LC_ECP256_KEY_SIZE]);
  std::unique_ptr<uint8_t> y(new uint8_t[LC_ECP256_KEY_SIZE]);
  std::unique_ptr<uint8_t> r(new uint8_t[LC_ECP256_KEY_SIZE]);

  BN_bn2bin(x_ec, x_.get());
  BN_bn2bin(y_ec, y_.get());
  BN_bn2bin(priv_bn, r_.get());

  // reverse x_, y_, r_
  for (uint32_t i = 0; i < LC_ECP256_KEY_SIZE; i++) {
    x.get()[i] = x_.get()[LC_ECP256_KEY_SIZE-i-1];
    y.get()[i] = y_.get()[LC_ECP256_KEY_SIZE-i-1];
    r.get()[i] = r_.get()[LC_ECP256_KEY_SIZE-i-1];
  }

  // Store public and private keys
  memcpy(sp_pub_key.gx, x.get(), LC_ECP256_KEY_SIZE);
  memcpy(sp_pub_key.gy, y.get(), LC_ECP256_KEY_SIZE);
  memcpy(sp_priv_key.r, r.get(), LC_ECP256_KEY_SIZE);

  // Clean up
  BN_free(x_ec);
  BN_free(y_ec);
  EC_KEY_free(ec_key);
  EVP_PKEY_free(pkey);
}

void ServiceProvider::export_public_key_code(const std::string &filename) {
  umask(0600);
  std::ofstream file(filename.c_str());

  file << "#include \"key.h\"\n";
  file << "const sgx_ec256_public_t g_sp_pub_key = {\n";

  file << "{";
  for (uint32_t i = 0; i < LC_ECP256_KEY_SIZE; ++i) {
    file << "0x" << std::hex << std::setfill('0') << std::setw(4) << sp_pub_key.gx[i];
    if (i < LC_ECP256_KEY_SIZE - 1) {
      file << ", ";
    }
  }
  file << "},\n";

  file << "{";
  for (uint32_t i = 0; i < LC_ECP256_KEY_SIZE; ++i) {
    file << "0x" << std::hex << std::setfill('0') << std::setw(4) << sp_pub_key.gy[i];
    if (i < LC_ECP256_KEY_SIZE - 1) {
      file << ", ";
    }
  }
  file << "}\n";

  file << "};\n";
  file.close();
}

std::unique_ptr<sgx_ra_msg2_t> ServiceProvider::process_msg1(
  sgx_ra_msg1_t *msg1, uint32_t *msg2_size) {
  // The following procedure follows Intel's guide:
  // https://software.intel.com/en-us/articles/code-sample-intel-software-guard-extensions-remote-attestation-end-to-end-example
  // The quotes below are from this guide.

  // "Generate a random EC key using the P-256 curve. This key will become Gb."
  lc_ec256_private_t priv_key;
  lc_ec256_public_t pub_key;
  lc_ecc256_create_key_pair(&priv_key, &pub_key);

  // "Derive the key derivation key (KDK) from Ga and Gb"
  lc_ec256_dh_shared_t dh_key;
  lc_ecc256_compute_shared_dhkey(&priv_key, &msg1->g_a, &dh_key);

  // "Derive the SMK from the KDK by performing an AES-128 CMAC on the byte sequence:
  // 0x01 || SMK || 0x00 || 0x80 || 0x00
  // using the KDK as the key. Note that || denotes concatenation and “SMK” is a literal string
  // (without quotes)."
  derive_key(&dh_key, SAMPLE_DERIVE_KEY_SMK, &sp_db.smk_key);
  // We also precompute the same result with different strings for future messages.
  derive_key(&dh_key, SAMPLE_DERIVE_KEY_MK, &sp_db.mk_key);
  derive_key(&dh_key, SAMPLE_DERIVE_KEY_SK, &sp_db.sk_key);
  derive_key(&dh_key, SAMPLE_DERIVE_KEY_VK, &sp_db.vk_key);

  // TODO: "Query IAS to obtain the SigRL for the client's Intel EPID GID."
  // For now we assume no signatures have been revoked.
  uint32_t sig_rl_size = 0;

  *msg2_size = sizeof(sgx_ra_msg2_t) + sig_rl_size;
  std::unique_ptr<sgx_ra_msg2_t> msg2(reinterpret_cast<sgx_ra_msg2_t *>(new uint8_t[*msg2_size]));
  msg2->g_b = sp_db.g_b;
  memcpy_s(&msg2->spid, sizeof(sgx_spid_t), spid.c_str(), spid.size());
  // "Determine the quote type that should be requested from the client (0x0 for unlinkable, and 0x1
  // for linkable). Note that this is a service provider policy decision, and the SPID must be
  // associated with the correct quote type."
  msg2->quote_type = 0x0;
  // "Set the KDF_ID. Normally this is 0x1."
  msg2->kdf_id = 0x1;

  // "Calculate the ECDSA signature of:
  // Gbx || Gby || Gax || Gay
  // (traditionally written as r || s) with the service provider's EC private key."
  lc_ec256_public_t gb_ga[2];
  gb_ga[0] = sp_db.g_b;
  gb_ga[1] = sp_db.g_a;
  lc_ecdsa_sign(reinterpret_cast<uint8_t *>(&gb_ga), sizeof(gb_ga),
                &sp_priv_key,
                &msg2->sign_gb_ga);

  // "Calculate the AES-128 CMAC of:
  // Gb || SPID || Quote_Type || KDF_ID || SigSP
  // using the SMK as the key."
  uint8_t mac[SGX_CMAC_MAC_SIZE] = {0};
  uint32_t cmac_size = offsetof(sgx_ra_msg2_t, mac);
  lc_rijndael128_cmac_msg(&sp_db.smk_key,
                          reinterpret_cast<uint8_t *>(&msg2->g_b),
                          cmac_size,
                          &mac);
  memcpy(&msg2->mac, &mac, sizeof(sgx_mac_t));

  msg2->sig_rl_size = sig_rl_size;

  return msg2;
}
