/*
 * Copyright (C) 2011-2016 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */



#include "service_provider.h"
#include "ecp.h"
#include "ias_ra.h"

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <time.h>
#include <stdexcept>

#ifndef SAFE_FREE
#define SAFE_FREE(ptr) {if (NULL != (ptr)) {free(ptr); (ptr) = NULL;}}
#endif

const char *key_str = "helloworld123123";

// This is supported extended epid group of SP. SP can support more than one
// extended epid group with different extended epid group id and credentials.
static const sample_extended_epid_group g_extended_epid_groups[] = {
    {
        0,
        ias_enroll,
        ias_get_sigrl,
        ias_verify_attestation_evidence
    }
};

// This is the public EC key of SP, this key is hard coded in isv_enclave.
// It is based on NIST P-256 curve. Not used in the SP code.
sgx_ec256_public_t g_sp_pub_key = {{0}, {0}};

// This is the private EC key of SP, the corresponding public EC key is
// hard coded in isv_enclave. It is based on NIST P-256 curve.
sgx_ec256_private_t g_sp_priv_key = {{0}};


static sp_db_item_t g_sp_db;

static const sample_extended_epid_group* g_sp_extended_epid_group_id= NULL;
static bool g_is_sp_registered = false;
static int g_sp_credentials = 0;
static int g_authentication_token = 0;

uint8_t g_secret[8] = {0,1,2,3,4,5,6,7};

sample_spid_t g_spid;

void write_pubkey(const char *filename,
                  unsigned char *pub_key_x,
                  unsigned char *pub_key_y,
                  uint32_t key_len) {

  int fd = open(filename, O_WRONLY | O_CREAT, 0600);
  uint32_t output_len = 1024 + 6 * key_len * 2;
  char *pub_key_output = (char *) malloc(output_len);
  for (uint32_t i = 0; i < output_len; i++) {
    *(pub_key_output+i) = ' ';
  }

  uint32_t offset = 0;
  snprintf(pub_key_output+offset, output_len, "#include \"key.h\"\nconst sgx_ec256_public_t g_sp_pub_key = {{");
  offset += strlen("#include \"key.h\"\nconst sgx_ec256_public_t g_sp_pub_key = {{");
  for (uint32_t i = 0; i < key_len; i++) {
    if (i == key_len - 1) {
      snprintf(pub_key_output+offset, output_len, "%#04x", *(pub_key_x+i));
      offset += 4;
    } else {
      snprintf(pub_key_output+offset, output_len, "%#04x, ", *(pub_key_x+i));
      offset += 6;
    }
  }
  snprintf(pub_key_output+offset, output_len, "},\n{");
  offset += 4;
  for (uint32_t i = 0; i < key_len; i++) {
    if (i == key_len - 1) {
      snprintf(pub_key_output+offset, output_len, "%#04x", *(pub_key_y+i));
      offset += 4;
    } else {
      snprintf(pub_key_output+offset, output_len, "%#04x, ", *(pub_key_y+i));
      offset += 6;
    }
  }
  snprintf(pub_key_output+offset, output_len, "}\n};\n");
  offset += strlen("}\n};\n");

  ssize_t write_bytes = write(fd, pub_key_output, offset);
  assert(write_bytes <= output_len && write_bytes > 0);
  (void)write_bytes;
  close(fd);

  free(pub_key_output);
}

int read_secret_key(const char *filename,
                    const char *cpp_output) {

  int ret = 0;
  FILE *secret_key_file = fopen(filename, "r");
  if (secret_key_file == nullptr) {
      throw std::runtime_error(
          std::string("Error: Private key file '")
          + std::string(filename)
          + std::string("' does not exist. Set environment variable $PRIVATE_KEY_PATH."));
  }
  EVP_PKEY *pkey = PEM_read_PrivateKey(secret_key_file, NULL, NULL, NULL);
  if (pkey == NULL) {
    printf("[read_secret_key] returned private key is null\n");
  }

  BIO *o = BIO_new_fp(stdout, BIO_NOCLOSE);
  EC_KEY *ec_key = (EC_KEY *) EVP_PKEY_get1_EC_KEY(pkey);
  assert(ec_key != NULL);

  EC_GROUP *group = (EC_GROUP *) EC_KEY_get0_group(ec_key);
  EC_POINT *point = (EC_POINT *) EC_KEY_get0_public_key(ec_key);

  BIGNUM *x_ec = BN_new();
  BIGNUM *y_ec = BN_new();
  ret = EC_POINT_get_affine_coordinates_GFp(group, point, x_ec, y_ec, NULL);

  if (ret == 0) {
    printf("[read_pub_key] EC_POINT_set_affine_coordinates_GFp did not get the correct points\n");
    return 1;
  } else {
#ifdef TEST_EC
    BN_print(o, x_ec);
    printf("\n");
    BN_print(o, y_ec);
    printf("\n");
#endif
  }

  const BIGNUM *priv_bn = EC_KEY_get0_private_key(ec_key);

  // Store the public and private keys in binary format
  unsigned char *x_ = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);
  unsigned char *y_ = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);
  unsigned char *r_ = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);

  unsigned char *x = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);
  unsigned char *y = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);
  unsigned char *r = (unsigned char *) malloc(LC_ECP256_KEY_SIZE);

  BN_bn2bin(x_ec, x_);
  BN_bn2bin(y_ec, y_);
  BN_bn2bin(priv_bn, r_);

  // reverse x_, y_, r_

  for (uint32_t i = 0; i < LC_ECP256_KEY_SIZE; i++) {
    *(x+i) = *(x_+LC_ECP256_KEY_SIZE-i-1);
    *(y+i) = *(y_+LC_ECP256_KEY_SIZE-i-1);
    *(r+i) = *(r_+LC_ECP256_KEY_SIZE-i-1);
  }

  memcpy_s(g_sp_pub_key.gx, LC_ECP256_KEY_SIZE, x, LC_ECP256_KEY_SIZE);
  memcpy_s(g_sp_pub_key.gy, LC_ECP256_KEY_SIZE, y, LC_ECP256_KEY_SIZE);
  memcpy_s(g_sp_priv_key.r, LC_ECP256_KEY_SIZE, r, LC_ECP256_KEY_SIZE);

  if (cpp_output != NULL) {
    write_pubkey(cpp_output, x, y, LC_ECP256_KEY_SIZE);
  }

  // free malloc'ed buffers
  free(x);
  free(y);
  free(r);
  free(x_);
  free(y_);
  free(r_);

  // free BIO
  BIO_free_all(o);
  // free BN
  BN_free(x_ec);
  BN_free(y_ec);

  // free EC stuff
  EC_KEY_free(ec_key);

  // free EVP
  EVP_PKEY_free(pkey);
  return 0;
}


// Verify message 0 then configure extended epid group.
int sp_ra_proc_msg0_req(uint32_t extended_epid_group_id) {
  int ret = 0;
  // Check to see if we have registered with the attestation server yet?
  if (!g_is_sp_registered ||
      (g_sp_extended_epid_group_id != NULL && g_sp_extended_epid_group_id->extended_epid_group_id != extended_epid_group_id))
    {
      // Check to see if the extended_epid_group_id is supported?
      ret = SP_UNSUPPORTED_EXTENDED_EPID_GROUP;
      for (size_t i = 0; i < sizeof(g_extended_epid_groups) / sizeof(sample_extended_epid_group); i++)
        {
          if (g_extended_epid_groups[i].extended_epid_group_id == extended_epid_group_id)
            {
              g_sp_extended_epid_group_id = &(g_extended_epid_groups[i]);
              // In the product, the SP will establish a mutually
              // authenticated SSL channel. During the enrollment process, the ISV
              // registers it exchanges TLS certs with attestation server and obtains an SPID and
              // Report Key from the attestation server.
              // For a product attestation server, enrollment is an offline process.  See the 'on-boarding'
              // documentation to get the information required.  The enrollment process is
              // simulated by a call in this sample.
              ret = g_sp_extended_epid_group_id->enroll(g_sp_credentials, &g_spid,
                                                        &g_authentication_token);
              if (0 != ret)
                {
                  ret = SP_IAS_FAILED;
                  break;
                }

              g_is_sp_registered = true;
              ret = SP_OK;
              break;
            }
        }
    }
  else
    {
      ret = SP_OK;
    }

  return ret;
}

// Verify message 1 then generate and return message 2 to isv.
int sp_ra_proc_msg1_req(sgx_ra_msg1_t *p_msg1,
						uint32_t msg1_size,
                        ra_samp_response_header_t **pp_msg2) {
  int ret = 0;
  ra_samp_response_header_t* p_msg2_full = NULL;
  sgx_ra_msg2_t *p_msg2 = NULL;
  //sgx_status_t ret = SGX_SUCCESS;
  bool derive_ret = false;

  if (!p_msg1 || !pp_msg2 || (msg1_size != sizeof(sgx_ra_msg1_t))) {
    printf("[%s] Unexpected error 1: %p, %p, size %u\n", __FUNCTION__, p_msg1, pp_msg2, msg1_size);
    std::exit(1);
  }

  // Check to see if we have registered?
  if (!g_is_sp_registered) {
    return SP_UNSUPPORTED_EXTENDED_EPID_GROUP;
  }

  do {
    // Get the sig_rl from attestation server using GID.
    // GID is Base-16 encoded of EPID GID in little-endian format.
    // In the product, the SP and attesation server uses an established channel for
    // communication.
    uint8_t* sig_rl;
    uint32_t sig_rl_size = 0;

    // The product interface uses a REST based message to get the SigRL.
        
    ret = g_sp_extended_epid_group_id->get_sigrl(p_msg1->gid, &sig_rl_size, &sig_rl);
    if (ret != 0) {
      fprintf(stderr, "[%s] Error, ias_get_sigrl\n", __FUNCTION__);
      ret = SP_IAS_FAILED;
      break;
    }

    // Need to save the client's public ECCDH key to local storage
    if (memcpy_s(&g_sp_db.g_a, sizeof(g_sp_db.g_a), &p_msg1->g_a,
                 sizeof(p_msg1->g_a))) {
      fprintf(stderr, "[%s] Error, could not memcpy msg1's g_a to local storage\n.", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // Generate the Service providers ECCDH key pair.
    lc_ec256_public_t pub_key = {{0},{0}};
    lc_ec256_private_t priv_key = {{0}};
    ret = lc_ecc256_create_key_pair((lc_ec256_private_t *) &priv_key,
                                    (lc_ec256_public_t *) &pub_key);
    if (ret != LC_SUCCESS) {
      fprintf(stderr, "[%s] Error, cannot generate key pair.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // Need to save the SP ECCDH key pair to local storage.
    if (memcpy_s(&g_sp_db.b, sizeof(g_sp_db.b), &priv_key,sizeof(priv_key))
        || memcpy_s(&g_sp_db.g_b, sizeof(g_sp_db.g_b), &pub_key,sizeof(pub_key))) {
      fprintf(stderr, "[%s] Error, cannot memcpy priv_key or pub_key to local storage.", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // Generate the client/SP shared secret
    lc_ec256_dh_shared_t dh_key = {{0}};
    ret = lc_ecc256_compute_shared_dhkey((lc_ec256_private_t *) &priv_key,
                                   (lc_ec256_public_t *) &p_msg1->g_a,
                                   (lc_ec256_dh_shared_t *) &dh_key);

    if (ret != LC_SUCCESS) {
      fprintf(stderr, "[%s] Error, compute share key fail.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

#ifdef SUPPLIED_KEY_DERIVATION

    // smk is only needed for msg2 generation.
    derive_ret = derive_key(&dh_key, SAMPLE_DERIVE_KEY_SMK_SK,
                            &g_sp_db.smk_key, &g_sp_db.sk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key fail.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // The rest of the keys are the shared secrets for future communication.
    derive_ret = derive_key(&dh_key, SAMPLE_DERIVE_KEY_MK_VK,
                            &g_sp_db.mk_key, &g_sp_db.vk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key failure.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }
#else
    // smk is only needed for msg2 generation.
    derive_ret = derive_key((lc_ec256_dh_shared_t *) &dh_key, SAMPLE_DERIVE_KEY_SMK,
                            &g_sp_db.smk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key failure.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // The rest of the keys are the shared secrets for future communication.
    derive_ret = derive_key((lc_ec256_dh_shared_t *) &dh_key, SAMPLE_DERIVE_KEY_MK,
                            &g_sp_db.mk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key failure.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    derive_ret = derive_key((lc_ec256_dh_shared_t *) &dh_key, SAMPLE_DERIVE_KEY_SK,
                            &g_sp_db.sk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key failure.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    derive_ret = derive_key((lc_ec256_dh_shared_t *) &dh_key, SAMPLE_DERIVE_KEY_VK,
                            &g_sp_db.vk_key);
    if (derive_ret != true) {
      fprintf(stderr, "[%s] Error, derive key failure.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }
#endif

    uint32_t msg2_size = sizeof(sgx_ra_msg2_t) + sig_rl_size;
    p_msg2_full = (ra_samp_response_header_t*) malloc(msg2_size + sizeof(ra_samp_response_header_t));
    if(!p_msg2_full) {
      fprintf(stderr, " [%s] Error, could not allocate p_msg2_full\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    memset(p_msg2_full, 0, msg2_size + sizeof(ra_samp_response_header_t));
    p_msg2_full->type = TYPE_RA_MSG2;
    p_msg2_full->size = msg2_size;
    // The simulated message2 always passes.  This would need to be set
    // accordingly in a real service provider implementation.
    p_msg2_full->status[0] = 0;
    p_msg2_full->status[1] = 0;
    p_msg2 = (sgx_ra_msg2_t *) p_msg2_full->body;

    // Assemble MSG2
    if(memcpy_s(&p_msg2->g_b, sizeof(p_msg2->g_b), &g_sp_db.g_b, sizeof(g_sp_db.g_b)) ||
       memcpy_s(&p_msg2->spid, sizeof(sample_spid_t), &g_spid, sizeof(g_spid))) {
      fprintf(stderr,"[%s] Error, memcpy failed\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // The service provider is responsible for selecting the proper EPID
    // signature type and to understand the implications of the choice!
    p_msg2->quote_type = SAMPLE_QUOTE_LINKABLE_SIGNATURE;

#ifdef SUPPLIED_KEY_DERIVATION
    //isv defined key derivation function id
#define ISV_KDF_ID 2
    p_msg2->kdf_id = ISV_KDF_ID;
#else
    p_msg2->kdf_id = SAMPLE_AES_CMAC_KDF_ID;
#endif
    // Create gb_ga
    lc_ec256_public_t gb_ga[2];
    if(memcpy_s(&gb_ga[0], sizeof(gb_ga[0]), &g_sp_db.g_b, sizeof(g_sp_db.g_b))
       || memcpy_s(&gb_ga[1], sizeof(gb_ga[1]), &g_sp_db.g_a, sizeof(g_sp_db.g_a))) {
      fprintf(stderr,"[%s] Error, memcpy failed.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // Sign gb_ga
    ret = lc_ecdsa_sign((uint8_t *)&gb_ga, sizeof(gb_ga),
                        (lc_ec256_private_t *)&g_sp_priv_key,
                        (lc_ec256_signature_t *)&p_msg2->sign_gb_ga);

    // printf("[%s] lc_ecdsa_sign   ", __FUNCTION__);
    // print_hex((uint8_t *) p_msg2->sign_gb_ga.x, 32);
    // printf("\n");
    // print_hex((uint8_t *) p_msg2->sign_gb_ga.y, 32);
    // printf("\n");

    if (ret != LC_SUCCESS) {
      fprintf(stderr, "[%s] Error, sign ga_gb fail.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    // Generate the CMACsmk for gb||SPID||TYPE||KDF_ID||Sigsp(gb,ga)
    uint8_t mac[SGX_CMAC_MAC_SIZE] = {0};
    uint32_t cmac_size = offsetof(sgx_ra_msg2_t, mac);
    ret = lc_rijndael128_cmac_msg(&g_sp_db.smk_key,
                                  (uint8_t *)&p_msg2->g_b,
                                  cmac_size,
                                  &mac);
    if (ret != LC_SUCCESS) {
      fprintf(stderr, "[%s] Error, cmac fail.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    if(memcpy_s(&p_msg2->mac, sizeof(p_msg2->mac), mac, sizeof(mac))) {
      fprintf(stderr,"[%s] Error, memcpy failed in.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

    if (memcpy_s(&p_msg2->sig_rl[0], sig_rl_size, sig_rl, sig_rl_size)) {
      fprintf(stderr,"[%s] Error, memcpy failed.\n", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }
    p_msg2->sig_rl_size = sig_rl_size;

  } while(0);

  if (ret) {
    *pp_msg2 = NULL;
    SAFE_FREE(p_msg2_full);
  } else {
    // Freed by the network simulator in ra_free_network_response_buffer
    *pp_msg2 = p_msg2_full;
  }

#ifdef DEBUG
  PRINT_BYTE_ARRAY(NULL, p_msg2_full->body, p_msg2_full->size);
#endif

  return ret;
}

// Process remote attestation message 3
int sp_ra_proc_msg3_req(sgx_ra_msg3_t *p_msg3,
                        uint32_t msg3_size,
                        ra_samp_response_header_t **pp_att_result_msg)
{
  int err_ret = 0;
  lc_status_t ret = LC_SUCCESS;
  const uint8_t *p_msg3_cmaced = NULL;
  sample_quote_t *p_quote = NULL;
  lc_sha_state_handle_t sha_handle = NULL;
  sample_report_data_t report_data = {0};
  sample_ra_att_result_msg_t *p_att_result_msg = NULL;
  ra_samp_response_header_t* p_att_result_msg_full = NULL;
  uint32_t i;

  if((!p_msg3) ||
     (msg3_size < sizeof(sample_ra_msg3_t)) ||
     (!pp_att_result_msg))
    {
      return SP_INTERNAL_ERROR;
    }

  // Check to see if we have registered?
  if (!g_is_sp_registered)
    {
      return SP_UNSUPPORTED_EXTENDED_EPID_GROUP;
    }
  do
    {
      // Compare g_a in message 3 with local g_a.
      err_ret = memcmp(&g_sp_db.g_a, &p_msg3->g_a, sizeof(lc_ec256_public_t));
      if (err_ret) {
        fprintf(stderr, "\nError, g_a is not same [%s].", __FUNCTION__);
        err_ret = SP_PROTOCOL_ERROR;
        break;
      }

      //Make sure that msg3_size is bigger than sample_mac_t.
      uint32_t mac_size = msg3_size - sizeof(sample_mac_t);
      p_msg3_cmaced = reinterpret_cast<const uint8_t*>(p_msg3);
      p_msg3_cmaced += sizeof(sample_mac_t);

      // Verify the message mac using SMK
      lc_cmac_128bit_tag_t mac = {0};
      ret = lc_rijndael128_cmac_msg(&g_sp_db.smk_key, p_msg3_cmaced,
                                    mac_size, &mac);
      if (ret != LC_SUCCESS) {
        fprintf(stderr, "\nError, cmac fail in [%s].", __FUNCTION__);
        err_ret = SP_INTERNAL_ERROR;
        break;
      }

      // In real implementation, should use a time safe version of memcmp here,
      // in order to avoid side channel attack.
      err_ret = memcmp(&p_msg3->mac, mac, sizeof(mac));
      if (err_ret) {
        fprintf(stderr, "\nError, verify cmac fail [%s].", __FUNCTION__);
        err_ret = SP_INTEGRITY_FAILED;
        break;
      }

      if (memcpy_s(&g_sp_db.ps_sec_prop, sizeof(g_sp_db.ps_sec_prop),
                  &p_msg3->ps_sec_prop, sizeof(p_msg3->ps_sec_prop))) {
        fprintf(stderr,"\nError, memcpy failed in [%s].", __FUNCTION__);
        err_ret = SP_INTERNAL_ERROR;
        break;
      }

      p_quote = (sample_quote_t *)p_msg3->quote;

      // Check the quote version if needed. Only check the Quote.version field if the enclave
      // identity fields have changed or the size of the quote has changed.  The version may
      // change without affecting the legacy fields or size of the quote structure.
      //if(p_quote->version < ACCEPTED_QUOTE_VERSION)
      //{
      //    fprintf(stderr,"\nError, quote version is too old.", __FUNCTION__);
      //    ret = SP_QUOTE_VERSION_ERROR;
      //    break;
      //}

      // Verify the report_data in the Quote matches the expected value.
      // The first 32 bytes of report_data are SHA256 HASH of {ga|gb|vk}.
      // The second 32 bytes of report_data are set to zero.
      lc_sha256_init(&sha_handle);
      lc_sha256_update((uint8_t *)&(g_sp_db.g_a), sizeof(g_sp_db.g_a), sha_handle);
      lc_sha256_update((uint8_t *)&(g_sp_db.g_b), sizeof(g_sp_db.g_b), sha_handle);
      lc_sha256_update((uint8_t *)&(g_sp_db.vk_key), sizeof(g_sp_db.vk_key), sha_handle);
      lc_sha256_get_hash(sha_handle, (lc_sha256_hash_t *)&report_data);
      err_ret = memcmp((uint8_t *)&report_data,
                       (uint8_t *)&(p_quote->report_body.report_data),
                       sizeof(report_data));
      if (err_ret) {
        fprintf(stderr, "\nError, verify hash fail [%s].", __FUNCTION__);
        err_ret = SP_INTEGRITY_FAILED;
        break;
      }

      // Verify Enclave policy (an attestation server may provide an API for this if we
      // registered an Enclave policy)

      // Verify quote with attestation server.
      // In the product, an attestation server could use a REST message and JSON formatting to request
      // attestation Quote verification.  The sample only simulates this interface.
      ias_att_report_t attestation_report;
      err_ret = g_sp_extended_epid_group_id->verify_attestation_evidence(p_quote,
                                                                         NULL,
                                                                         &attestation_report);
      if (err_ret != 0) {
        err_ret = SP_IAS_FAILED;
        break;
      }

      FILE* OUTPUT = stdout;
      fprintf(OUTPUT, "\n\n\tAtestation Report:");
      fprintf(OUTPUT, "\n\tid: 0x%0x.", attestation_report.id);
      fprintf(OUTPUT, "\n\tstatus: %d.", attestation_report.status);
      fprintf(OUTPUT, "\n\trevocation_reason: %u.",
              attestation_report.revocation_reason);
      // attestation_report.info_blob;
      fprintf(OUTPUT, "\n\tpse_status: %d.",  attestation_report.pse_status);
      // Note: This sample always assumes the PIB is sent by attestation server.  In the product
      // implementation, the attestation server could only send the PIB for certain attestation
      // report statuses.  A product SP implementation needs to handle cases
      // where the PIB is zero length.

      // Respond the client with the results of the attestation.
      uint32_t att_result_msg_size = sizeof(sample_ra_att_result_msg_t);
      p_att_result_msg_full = (ra_samp_response_header_t*) malloc(att_result_msg_size
                                                                  + sizeof(ra_samp_response_header_t) + LC_AESGCM_KEY_SIZE);
      if (!p_att_result_msg_full) {
        fprintf(stderr, "\nError, out of memory in [%s].", __FUNCTION__);
        err_ret = SP_INTERNAL_ERROR;
        break;
      }
      memset(p_att_result_msg_full, 0, att_result_msg_size
             + sizeof(ra_samp_response_header_t) + LC_AESGCM_KEY_SIZE);
      p_att_result_msg_full->type = TYPE_RA_ATT_RESULT;
      p_att_result_msg_full->size = att_result_msg_size;
      if (attestation_report.status != IAS_QUOTE_OK) {
        p_att_result_msg_full->status[0] = 0xFF;
      }
      if (attestation_report.pse_status != IAS_PSE_OK) {
        p_att_result_msg_full->status[1] = 0xFF;
      }

      p_att_result_msg =
        (sample_ra_att_result_msg_t *)p_att_result_msg_full->body;

      // In a product implementation of attestation server, the HTTP response header itself could have
      // an RK based signature that the service provider needs to check here.

      // The platform_info_blob signature will be verified by the client
      // when sent. No need to have the Service Provider to check it.  The SP
      // should pass it down to the application for further analysis.

      fprintf(OUTPUT, "\n\n\tEnclave Report:");
      fprintf(OUTPUT, "\n\tSignature Type: 0x%x", p_quote->sign_type);
      fprintf(OUTPUT, "\n\tSignature Basename: ");
      for(i=0; i<sizeof(p_quote->basename.name) && p_quote->basename.name[i];
          i++)
        {
          fprintf(OUTPUT, "%c", p_quote->basename.name[i]);
        }
#ifdef __x86_64__
      fprintf(OUTPUT, "\n\tattributes.flags: 0x%0lx",
              p_quote->report_body.attributes.flags);
      fprintf(OUTPUT, "\n\tattributes.xfrm: 0x%0lx",
              p_quote->report_body.attributes.xfrm);
#else
      fprintf(OUTPUT, "\n\tattributes.flags: 0x%0llx",
              p_quote->report_body.attributes.flags);
      fprintf(OUTPUT, "\n\tattributes.xfrm: 0x%0llx",
              p_quote->report_body.attributes.xfrm);
#endif
      fprintf(OUTPUT, "\n\tmr_enclave: ");
      for(i=0;i<sizeof(sample_measurement_t);i++)
        {

          fprintf(OUTPUT, "%02x",p_quote->report_body.mr_enclave[i]);

          //fprintf(stderr, "%02x",p_quote->report_body.mr_enclave.m[i]);

        }
      fprintf(OUTPUT, "\n\tmr_signer: ");
      for(i=0;i<sizeof(sample_measurement_t);i++)
        {

          fprintf(OUTPUT, "%02x",p_quote->report_body.mr_signer[i]);

          //fprintf(stderr, "%02x",p_quote->report_body.mr_signer.m[i]);

        }
      fprintf(OUTPUT, "\n\tisv_prod_id: 0x%0x",
              p_quote->report_body.isv_prod_id);
      fprintf(OUTPUT, "\n\tisv_svn: 0x%0x",p_quote->report_body.isv_svn);
      fprintf(OUTPUT, "\n");

      // A product service provider needs to verify that its enclave properties
      // match what is expected.  The SP needs to check these values before
      // trusting the enclave.  For the sample, we always pass the policy check.
      // Attestation server only verifies the quote structure and signature.  It does not
      // check the identity of the enclave.
      bool isv_policy_passed = true;

      // Assemble Attestation Result Message
      // Note, this is a structure copy.  We don't copy the policy reports
      // right now.
      p_att_result_msg->platform_info_blob = attestation_report.info_blob;

      // Generate mac based on the mk key.
      mac_size = sizeof(ias_platform_info_blob_t);
      ret = lc_rijndael128_cmac_msg(&g_sp_db.mk_key,
                                    (const uint8_t*)&p_att_result_msg->platform_info_blob,
                                    mac_size,
                                    &p_att_result_msg->mac);

      if (ret != LC_SUCCESS) {
        fprintf(stderr, "\nError, cmac fail in [%s].", __FUNCTION__);
        err_ret = SP_INTERNAL_ERROR;
        break;
      }

      // Generate shared secret and encrypt it with SK, if attestation passed.
      // TODO: question -- is a random IV needed here, if the session keys are different?
      uint8_t aes_gcm_iv[LC_AESGCM_IV_SIZE] = {0};
      // // read from /dev/random
      // int fd = open("/dev/random", O_RDONLY);
      // ssize_t bytes_read = read(fd, aes_gcm_iv, LC_AESGCM_IV_SIZE);
      // assert(bytes_read >= LC_AESGCM_IV_SIZE);
      // close(fd);

      p_att_result_msg->secret.payload_size = LC_AESGCM_KEY_SIZE;

      if((IAS_QUOTE_OK == attestation_report.status) &&
         (IAS_PSE_OK == attestation_report.pse_status) &&
         (isv_policy_passed == true)) {

        ret = (lc_status_t ) lc_rijndael128GCM_encrypt(&g_sp_db.sk_key,
                                                       (uint8_t *) key_str,
                                                       p_att_result_msg->secret.payload_size,
                                                       p_att_result_msg->secret.payload,
                                                       &aes_gcm_iv[0],
                                                       LC_AESGCM_IV_SIZE,
                                                       NULL,
                                                       0,
                                                       &p_att_result_msg->secret.payload_tag);
#ifdef PERF
        printf("[%s] Sent key is \t", __FUNCTION__);
        print_hex((uint8_t *) key_str, p_att_result_msg->secret.payload_size);
        printf("ret is %u\n", ret);
#endif

      }
    } while(0);

  if (err_ret) {
    *pp_att_result_msg = NULL;
    SAFE_FREE(p_att_result_msg_full);
  } else {
    // Freed by the network simulator in ra_free_network_response_buffer
    *pp_att_result_msg = p_att_result_msg_full;
  }

  return err_ret;
}
