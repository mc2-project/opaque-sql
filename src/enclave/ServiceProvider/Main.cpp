/**
 *   Copyright(C) 2011-2015 Intel Corporation All Rights Reserved.
 *
 *   The source code, information  and  material ("Material") contained herein is
 *   owned  by Intel Corporation or its suppliers or licensors, and title to such
 *   Material remains  with Intel Corporation  or its suppliers or licensors. The
 *   Material  contains proprietary information  of  Intel or  its  suppliers and
 *   licensors. The  Material is protected by worldwide copyright laws and treaty
 *   provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
 *   modified, published, uploaded, posted, transmitted, distributed or disclosed
 *   in any way  without Intel's  prior  express written  permission. No  license
 *   under  any patent, copyright  or  other intellectual property rights  in the
 *   Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
 *   implication, inducement,  estoppel or  otherwise.  Any  license  under  such
 *   intellectual  property  rights must  be express  and  approved  by  Intel in
 *   writing.
 *
 *   *Third Party trademarks are the property of their respective owners.
 *
 *   Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
 *   this  notice or  any other notice embedded  in Materials by Intel or Intel's
 *   suppliers or licensors in any way.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdint.h>

//#include "Main.h"
//#include "sgx_tcrypto.h"
//#include "sgx_ukey_exchange.h"
//#include "define.h"
//#include "common.h"

#include "SP.h"
//#include "sample_messages.h"    // this is for debugging remote attestation only
#include "service_provider.h"

class scoped_timer {

public:
  scoped_timer(uint64_t *total_time) {
    this->total_time = total_time;
    struct timeval start;
    gettimeofday(&start, NULL);
    time_start = start.tv_sec * 1000000 + start.tv_usec;
  }

  ~scoped_timer() {
    struct timeval end;
    gettimeofday(&end, NULL);
    time_end = end.tv_sec * 1000000 + end.tv_usec;
    *total_time += time_end - time_start;
  }

  uint64_t * total_time;
  uint64_t time_start, time_end;
};

// uint8_t* msg1_samples[] = { msg1_sample1, msg1_sample2 };
// uint8_t* msg2_samples[] = { msg2_sample1, msg2_sample2 };
// uint8_t* msg3_samples[] = { msg3_sample1, msg3_sample2 };
// uint8_t* attestation_msg_samples[] = { attestation_msg_sample1, attestation_msg_sample2};



// These SP (service provider) calls are supposed to be made in a trusted environment
// For now we assume that the trusted master executes these calls
JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg0(JNIEnv *env, jobject obj, jbyteArray msg0_input) {
  // Master receives EPID information from the enclave

  (void)env;
  (void)obj;

  //uint32_t msg0_size = (uint32_t) env->GetArrayLength(msg0_input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(msg0_input, &if_copy);
  uint32_t *extended_epid_group_id = (uint32_t *) ptr;

  sp_ra_proc_msg0_req(*extended_epid_group_id);
}

// Returns msg2 to the enclave
JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg1(JNIEnv *env, jobject obj, jbyteArray msg1_input) {

  (void)env;
  (void)obj;

  uint32_t msg1_size = (uint32_t) env->GetArrayLength(msg1_input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(msg1_input, &if_copy);
  sgx_ra_msg1_t *msg1 = (sgx_ra_msg1_t *) ptr;

  ra_samp_response_header_t *pp_msg2 = NULL;
  sp_ra_proc_msg1_req(msg1, msg1_size, &pp_msg2);

  jbyteArray array_ret = env->NewByteArray(pp_msg2->size);
  env->SetByteArrayRegion(array_ret, 0, pp_msg2->size, (jbyte *) pp_msg2->body);
  assert(pp_msg2->size == sizeof(sgx_ra_msg2_t));

  free(pp_msg2);

  return array_ret;
}

// Returns the attestation result to the enclave
JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg3(JNIEnv *env, jobject obj, jbyteArray msg3_input) {

  (void)env;
  (void)obj;

  uint32_t msg3_size = (uint32_t) env->GetArrayLength(msg3_input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(msg3_input, &if_copy);
  sgx_ra_msg3_t *msg3 = (sgx_ra_msg3_t *) ptr;


  ra_samp_response_header_t *pp_att_full = NULL;
  sp_ra_proc_msg3_req(msg3, msg3_size, &pp_att_full);

  sample_ra_att_result_msg_t *result = (sample_ra_att_result_msg_t *) pp_att_full->body;
  printf("[SPProcMsg3] pp_att_full->size is %u, payload's size is %u\n", pp_att_full->size, result->secret.payload_size);

  jbyteArray array_ret = env->NewByteArray(pp_att_full->size + sizeof(ra_samp_response_header_t));
  env->SetByteArrayRegion(array_ret, 0, pp_att_full->size + sizeof(ra_samp_response_header_t), (jbyte *) pp_att_full);
  free(pp_att_full);

  return array_ret;
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_LoadKeys(
    JNIEnv *env, jobject obj) {
  (void)env;
  (void)obj;

  const char *private_key_filename = std::getenv("PRIVATE_KEY_PATH");
  read_secret_key(private_key_filename, NULL);
}

void test_encrypt() {
  uint8_t p_key[16] = {0xff, 0xff, 0xff, 0xff,
                      0xff, 0xff, 0xff, 0xff,
                      0xff, 0xff, 0xff, 0xff,
                      0xff, 0xff, 0xff, 0xff};

  uint8_t p_src_[11] = "helloworld";
  uint8_t *p_src = p_src_;
  uint32_t src_len = 10;
  uint8_t p_iv[SAMPLE_SP_IV_SIZE] = {
    0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff
  };
  lc_aes_gcm_128bit_tag_t mac;
  uint8_t p_dst_[100];
  uint8_t *p_dst = p_dst_;

  lc_rijndael128GCM_encrypt((lc_aes_gcm_128bit_key_t *) &p_key,
                            p_src, src_len,
                            p_dst,
                            p_iv, SAMPLE_SP_IV_SIZE,
                            NULL, 0,
                            &mac);

  print_hex(p_src, src_len);
  printf("\n");

  print_hex(p_dst, src_len);
  printf("\n");


  uint8_t plaintext[100];
  lc_rijndael128GCM_decrypt(p_dst, src_len,
                            NULL, 0,
                            (unsigned char *) mac,
                            (unsigned char *) &p_key,
                            (unsigned char *)  p_iv,
                            (unsigned char *) plaintext);

  print_hex(plaintext, src_len);
  printf("\n");
}

void test_sign() {
  uint8_t data[100] = "helloworldhelloworldhelloworldhelloworld";
  uint32_t data_len = 15;

  sample_ecc_state_handle_t ecc_state = NULL;
  sample_ec256_private_t priv_key;
  sample_ec256_public_t pub_key;
  sample_ecc256_open_context(&ecc_state);
  sample_ecc256_create_key_pair((sample_ec256_private_t *) &priv_key,
                                (sample_ec256_public_t *) &pub_key,
                                ecc_state);

  printf("priv_key: ");
  print_hex((uint8_t *) priv_key.r, 32);
  printf("\n");

  printf("pub_key x: ");
  print_hex((uint8_t *) pub_key.gx, 32);
  printf("\n");

  printf("pub_key y: ");
  print_hex((uint8_t *) pub_key.gy, 32);
  printf("\n");


  // lc_ec256_private_t priv_key_2;
  // lc_ec256_public_t pub_key_2;
  // lc_ecc256_create_key_pair(&priv_key_2, &pub_key_2, NULL);

  lc_ec256_signature_t sig;
  lc_ecdsa_sign(data, data_len, (lc_ec256_private_t *) &priv_key,
                &sig, NULL);
  printf("lc_ecdsa sign\n");
  print_hex((uint8_t *) sig.x, 32);
  printf("\n");
  print_hex((uint8_t *) sig.y, 32);
  printf("\n");

  sample_ec256_signature_t sample_sig;
  sample_ecdsa_sign(data, data_len,
                    (sample_ec256_private_t *) &priv_key,
                    (sample_ec256_signature_t *) &sample_sig,
                    ecc_state);

  printf("sample_ecdsa sign\n");
  print_hex((uint8_t *) sample_sig.x, 32);
  printf("\n");
  print_hex((uint8_t *) sample_sig.y, 32);
  printf("\n");


  ECDSA_SIG *openssl_sig = ECDSA_SIG_new();
  uint8_t *r_ = (uint8_t *) malloc(32);
  uint8_t *s_ = (uint8_t *) malloc(32);
  reverse_endian((uint8_t *) sample_sig.x, r_, 32);
  reverse_endian((uint8_t *) sample_sig.y, s_, 32);

  printf("r_ is \t ");
  print_hex(r_, 32);
  printf("\n");
  printf("s_ is \t ");
  print_hex(s_, 32);
  printf("\n");

  openssl_sig->r = BN_new();
  openssl_sig->s = BN_new();
  BN_bin2bn((uint8_t *) r_, 32, openssl_sig->r);
  BN_bin2bn((uint8_t *) s_, 32, openssl_sig->s);

  printf("Verifying sample sig\n");
  // first calculate the  SHA256 hash
  lc_sha_state_handle_t p_sha_handle;
  lc_sha256_hash_t p_hash;
  lc_sha256_init(&p_sha_handle);
  lc_sha256_update(data, data_len, p_sha_handle);
  lc_sha256_get_hash(p_sha_handle, &p_hash);
  lc_sha256_close(p_sha_handle);

  // verify the digest
  EC_KEY *ec_key = get_priv_key((lc_ec256_private_t *) &priv_key);
  int verify = ECDSA_do_verify((uint8_t *) p_hash, sizeof(lc_sha256_hash_t), openssl_sig, ec_key);
  printf("verify is %u\n", verify);

  free(r_);
  free(s_);
  EC_KEY_free(ec_key);
}

void test_ecdh() {
  sample_ecc_state_handle_t ecc_state = NULL;
  sample_ec256_private_t priv_key;
  sample_ec256_public_t pub_key;
  sample_ecc256_open_context(&ecc_state);
  sample_ecc256_create_key_pair((sample_ec256_private_t *) &priv_key,
                                (sample_ec256_public_t *) &pub_key,
                                ecc_state);

  lc_ec256_private_t priv_key_2;
  lc_ec256_public_t pub_key_2;
  lc_ecc256_create_key_pair(&priv_key_2, &pub_key_2, NULL);

  printf("priv_key_2: ");
  print_hex((uint8_t *) priv_key_2.r, 32);
  printf("\n");

  printf("pub_key_2 x: ");
  print_hex((uint8_t *) pub_key_2.gx, 32);
  printf("\n");

  printf("pub_key_2 y: ");
  print_hex((uint8_t *) pub_key_2.gy, 32);
  printf("\n");

  sample_ec256_dh_shared_t dh_key;

  sample_ecc256_compute_shared_dhkey((sample_ec256_private_t *) &priv_key,
                                     (sample_ec256_public_t *) &pub_key,
                                     (sample_ec256_dh_shared_t *) &dh_key,
                                     ecc_state);

  printf("\n");
  printf("dh_key: ");
  print_hex((uint8_t *) dh_key.s, 32);
  printf("\n");

  lc_ec256_dh_shared_t dh_key_lc;
  lc_ecc256_compute_shared_dhkey((lc_ec256_private_t *) &priv_key,
                                 (lc_ec256_public_t *) &pub_key,
                                 &dh_key_lc, NULL);
  printf("Computed shared key\n");

  printf("\n");
  printf("dh_key_lc: ");
  print_hex((uint8_t *) dh_key_lc.s, 32);
  printf("\n");

  if (ecc_state) {
    sample_ecc256_close_context(&ecc_state);
  }

}

int main(int argc, char **argv) {
  (void)(argc);
  (void)(argv);


#ifndef TEST_EC
  if (argc < 2) {
    printf("Please input the public key's cpp file location\n");
    return 1;
  }

  const char *private_key_filename = std::getenv("PRIVATE_KEY_PATH");
  read_secret_key(private_key_filename, argv[1]);
#else

  printf("TEST_EC\n");

  uint8_t data[11] = "helloworld";

  lc_ec256_private_t p_private;
  lc_ec256_public_t p_public;
  lc_ec256_dh_shared_t p_shared_key;
  lc_ecc_state_handle_t ecc_handle = NULL;
  lc_ecc256_create_key_pair(&p_private, &p_public, ecc_handle);
  printf("Created key pair\n");

  print_pub_key(p_public);
  print_priv_key(p_private);

  // test compute shared key
  lc_ecc256_compute_shared_dhkey(&p_private, &p_public, &p_shared_key, ecc_handle);
  printf("Computed shared key\n");

  EC_POINT *pub_key = get_ec_point(&p_public);
  EC_POINT_free(pub_key);

  // test signature
  lc_ec256_signature_t sig;
  lc_ecdsa_sign(data, 10, &p_private, &sig, ecc_handle);
  printf("Signed data using ECDSA\n");

  lc_sha_state_handle_t sha_handle;
  lc_sha256_hash_t hash;

  // test sha
  lc_sha256_init(&sha_handle);
  lc_sha256_update(data, 10, sha_handle);
  lc_sha256_get_hash(sha_handle, &hash);
  lc_sha256_close(sha_handle);

  print_hex((uint8_t *) hash, sizeof(lc_sha256_hash_t));
  printf("\n");

  uint8_t p_key_data[33] = "12345678123456781234567812345678";
  lc_cmac_128bit_key_t *p_key = (lc_cmac_128bit_key_t *) &p_key_data;
  lc_cmac_128bit_tag_t mac;
  lc_rijndael128_cmac_msg(p_key, data, 10, &mac);
  printf("LC mac: ");
  print_hex((uint8_t *) mac, sizeof(lc_cmac_128bit_tag_t));
  printf("\n");

  sample_cmac_128bit_key_t *key = (sample_cmac_128bit_key_t *) &p_key_data;
  sample_cmac_128bit_tag_t sample_mac;
  sample_rijndael128_cmac_msg(key, data, 10, &sample_mac);

  printf("Sample mac: ");
  print_hex((uint8_t *) sample_mac, sizeof(sample_cmac_128bit_tag_t));
  printf("\n");

  //test_ecdh();
  test_sign();

  test_encrypt();

#endif

  return 0;
}
