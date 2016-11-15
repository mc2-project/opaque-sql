#include "RemoteAttestation.h"

bool verify_cmac128(
    sample_ec_key_128bit_t mac_key,
    const uint8_t *p_data_buf,
    uint32_t buf_size,
    const uint8_t *p_mac_buf)
{
    uint8_t data_mac[SAMPLE_EC_MAC_SIZE];
    sample_status_t sample_ret;

    sample_ret = sample_rijndael128_cmac_msg((sample_cmac_128bit_key_t*)mac_key,
        p_data_buf,
        buf_size,
        (sample_cmac_128bit_tag_t *)data_mac);
    if(sample_ret != SAMPLE_SUCCESS)
        return false;
    // In real implementation, should use a time safe version of memcmp here,
    // in order to avoid side channel attack.
    if(!memcmp(p_mac_buf, data_mac, SAMPLE_EC_MAC_SIZE))
        return true;
    return false;
}

#define EC_DERIVATION_BUFFER_SIZE(label_length) ((label_length) +4)

const char str_SMK[] = "SMK";
const char str_SK[] = "SK";
const char str_MK[] = "MK";
const char str_VK[] = "VK";

// Derive key from shared key and key id.
// key id should be sample_derive_key_type_t.
bool derive_key(
    const sgx_ec_dh_shared_t *p_shared_key,
    uint8_t key_id,
    sgx_ec_key_128bit_t* derived_key)
{
    sgx_status_t ret = SGX_SUCCESS;
    uint8_t cmac_key[MAC_KEY_SIZE];
    sgx_ec_key_128bit_t key_derive_key;

    memset(&cmac_key, 0, MAC_KEY_SIZE);

    ret = sgx_rijndael128_cmac_msg(
        (sgx_cmac_128bit_key_t *)&cmac_key,
        (uint8_t*)p_shared_key,
        sizeof(sgx_ec_dh_shared_t),
        (sgx_cmac_128bit_tag_t *)&key_derive_key);
    if (ret != SGX_SUCCESS)
    {
        // memset here can be optimized away by compiler, so please use memset_s on
        // windows for production code and similar functions on other OSes.
        memset(&key_derive_key, 0, sizeof(key_derive_key));
        return false;
    }

    const char *label = NULL;
    uint32_t label_length = 0;
    switch (key_id)
    {
    case DERIVE_KEY_SMK:
        label = str_SMK;
        label_length = sizeof(str_SMK) -1;
        break;
    case DERIVE_KEY_SK:
        label = str_SK;
        label_length = sizeof(str_SK) -1;
        break;
    case DERIVE_KEY_MK:
        label = str_MK;
        label_length = sizeof(str_MK) -1;
        break;
    case DERIVE_KEY_VK:
        label = str_VK;
        label_length = sizeof(str_VK) -1;
        break;
    default:
        // memset here can be optimized away by compiler, so please use memset_s on
        // windows for production code and similar functions on other OSes.
        memset(&key_derive_key, 0, sizeof(key_derive_key));
        return false;
        break;
    }
    /* derivation_buffer = counter(0x01) || label || 0x00 || output_key_len(0x0080) */
    uint32_t derivation_buffer_length = EC_DERIVATION_BUFFER_SIZE(label_length);
    uint8_t *p_derivation_buffer = (uint8_t *)malloc(derivation_buffer_length);
    if (p_derivation_buffer == NULL)
    {
        // memset here can be optimized away by compiler, so please use memset_s on
        // windows for production code and similar functions on other OSes.
        memset(&key_derive_key, 0, sizeof(key_derive_key));
        return false;
    }
    memset(p_derivation_buffer, 0, derivation_buffer_length);

    /*counter = 0x01 */
    p_derivation_buffer[0] = 0x01;
    /*label*/
    memcpy(&p_derivation_buffer[1], label, label_length);
    /*output_key_len=0x0080*/
    uint16_t *key_len = (uint16_t *)(&(p_derivation_buffer[derivation_buffer_length - 2]));
    *key_len = 0x0080;


    sample_ret = sample_rijndael128_cmac_msg(
        (sample_cmac_128bit_key_t *)&key_derive_key,
        p_derivation_buffer,
        derivation_buffer_length,
        (sample_cmac_128bit_tag_t *)derived_key);
    free(p_derivation_buffer);
    // memset here can be optimized away by compiler, so please use memset_s on
    // windows for production code and similar functions on other OSes.
    memset(&key_derive_key, 0, sizeof(key_derive_key));
    if (sample_ret != SAMPLE_SUCCESS)
    {
        return false;
    }
    return true;
}


// First step of the remote attestation protocol
// 1. Generate the client-side public key ga
// 2. Sends ga || GID (GID should be the Enhanced Privacy ID (EPID) )
void enclave_init_ra(int b_pse, sgx_ra_context_t *p_context) {

  // isv enclave call to trusted key exchange library.
  sgx_status_t ret;
  if(b_pse)
    {
      int busy_retry_times = 2;
      do{
        ret = sgx_create_pse_session();
      }while (ret == SGX_ERROR_BUSY && busy_retry_times--);
      if (ret != SGX_SUCCESS)
        return ret;
    }
  ret = sgx_ra_init(&g_sp_pub_key, b_pse, p_context);
  if(b_pse)
    {
      sgx_close_pse_session();
      return ret;
    }
  return ret;

}

// This is a context data structure used on SP side
typedef struct _sp_db_item_t
{
    sample_ec_pub_t             g_a;
    sample_ec_pub_t             g_b;
    sample_ec_key_128bit_t      vk_key;// Shared secret key for the REPORT_DATA
    sample_ec_key_128bit_t      mk_key;// Shared secret key for generating MAC's
    sample_ec_key_128bit_t      sk_key;// Shared secret key for encryption
    sample_ec_key_128bit_t      smk_key;// Used only for SIGMA protocol
    sample_ec_priv_t            b;
    sample_ps_sec_prop_desc_t   ps_sec_prop;
} sp_db_item_t;
static sp_db_item_t g_sp_db;

static bool g_is_sp_registered = false;
static int g_sp_credentials = 0;
static int g_authentication_token = 0;

// The client needs to process message 1 (ga || GID)
void process_message1(const sgx_ra_msg1_t *p_msg1,
                      uint32_t msg1_size,
                      ra_samp_response_header_t **pp_msg2) {
  int ret = 0;
  ra_samp_response_header_t* p_msg2_full = NULL;
  sample_ra_msg2_t *p_msg2 = NULL;
  sgx_ecc_state_handle_t ecc_state = NULL;
  sgx_status_t sample_ret = SGX_SUCCESS;
  bool derive_ret = false;

  if(!p_msg1 ||
     !pp_msg2 ||
     (msg1_size != sizeof(sgx_ra_msg1_t))) {
    return -1;
  }

  // Check to see if we have registered with the IAS yet?
  if(!g_is_sp_registered) {
    do
      {
        // @IAS_Q: What are the sp credentials?
        // @IAS_Q: What is in the authentication token
        // In the product, the SP will establish a mutually
        // authenticated SSL channel. The authentication token is
        // based on this channel.
        // @TODO: Convert this call to a 'network' send/receive
        // once the IAS server is a vaialable.
        ret = ias_enroll(g_sp_credentials, &g_spid,
                         &g_authentication_token);
        if(0 != ret)
          {
            ret = SP_IAS_FAILED;
            break;
          }

        // IAS may support registering the Enclave Trust Policy.
        // Just leave a place holder here
        // @IAS_Q: What needs to be sent to the IAS with the policy
        // that identifies the SP?
        // ret = ias_register_enclave_policy(g_enclave_policy,
        // g_authentication_token);
        // if(0 != ret)
        // {
        //     break;
        // }

        g_is_sp_registered = true;
        break;
      } while(0);
  }

  // Get the sig_rl from IAS using GID.
  // GID is Base-16 encoded of EPID GID in little-endian format.
  // @IAS_Q: Does the SP need to supply any authentication info to the
  // IAS?  SPID?
  // In the product, the SP and IAS will use an established channel for
  // communication.
  uint8_t* sig_rl;
  uint32_t sig_rl_size = 0;

  // @TODO: Convert this call to a 'network' send/receive
  // once the IAS server is a vaialable.
  ret = ias_get_sigrl(p_msg1->gid, &sig_rl_size, &sig_rl);
  if(0 != ret) {
    fprintf(stderr, "\nError, ias_get_sigrl [%s].", __FUNCTION__);
    ret = SP_IAS_FAILED;
    break;
  }

  // Need to save the client's public ECCDH key to local storage
  if (memcpy_s(&g_sp_db.g_a, sizeof(g_sp_db.g_a), &p_msg1->g_a,
               sizeof(p_msg1->g_a))) {
    fprintf(stderr, "\nError, cannot do memcpy in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  // Generate the Service providers ECCDH key pair.
  ret = sgx_ecc256_open_context(&ecc_state);
  if(SGX_SUCCESS != ret) {
    fprintf(stderr, "\nError, cannot get ECC cotext in [%s].",
            __FUNCTION__);
    ret = -1;
    break;
  }

  sgx_ec256_public_t pub_key = {{0},{0}};
  sgx_ec256_private_t priv_key = {{0}};
  ret = sgx_ecc256_create_key_pair(&priv_key, &pub_key, ecc_state);

  if(SGX_SUCCESS != sample_ret) {
    fprintf(stderr, "\nError, cannot generate key pair in [%s].",
            __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  // Need to save the SP ECCDH key pair to local storage.
  if(memcpy_s(&g_sp_db.b, sizeof(g_sp_db.b), &priv_key,sizeof(priv_key))
     || memcpy_s(&g_sp_db.g_b, sizeof(g_sp_db.g_b),
                 &pub_key,sizeof(pub_key))) {
    fprintf(stderr, "\nError, cannot do memcpy in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  // Generate the client/SP shared secret using received ga as well as the newly generated priv_key
  // Basically DH key exchange for elliptic curve keys
  sgx_ec_dh_shared_t dh_key = {{0}};
  ret = sgx_ecc256_compute_shared_dhkey(&priv_key,
                                        (sgx_ec256_public_t *)&p_msg1->g_a,
                                        (sgx_ec256_dh_shared_t *)&dh_key,
                                        ecc_state);
  if(SGX_SUCCESS != ret) {
    fprintf(stderr, "\nError, compute share key fail in [%s].",
            __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  // smk is only needed for msg2 generation.
  derive_ret = derive_key(&dh_key, DERIVE_KEY_SMK,
                          &g_sp_db.smk_key);
  if (derive_ret != true) {
    fprintf(stderr, "\nError, derive key fail in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  // The rest of the keys are the shared secrets for future communication.
  derive_ret = derive_key(&dh_key, DERIVE_KEY_MK,
                          &g_sp_db.mk_key);
  if (derive_ret != true) {
    fprintf(stderr, "\nError, derive key fail in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  derive_ret = derive_key(&dh_key, DERIVE_KEY_SK,
                          &g_sp_db.sk_key);
  if (derive_ret != true) {
    fprintf(stderr, "\nError, derive key fail in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  derive_ret = derive_key(&dh_key, DERIVE_KEY_VK,
                          &g_sp_db.vk_key);
  if (derive_ret != true) {
    fprintf(stderr, "\nError, derive key fail in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }

  uint32_t msg2_size = sizeof(sample_ra_msg2_t) + sig_rl_size;
  p_msg2_full = (ra_samp_response_header_t*)malloc(msg2_size
                                                   + sizeof(ra_samp_response_header_t));
  if (!p_msg2_full) {
    fprintf(stderr, "\nError, out of memory in [%s].", __FUNCTION__);
    ret = SP_INTERNAL_ERROR;
    break;
  }
  memset(p_msg2_full, 0, msg2_size + sizeof(ra_samp_response_header_t));
  p_msg2_full->type = TYPE_RA_MSG2;
  p_msg2_full->size = msg2_size;

  // @TODO: Set the status properly based on real protocol communication.
  p_msg2_full->status[0] = 0;
  p_msg2_full->status[1] = 0;
  p_msg2 = (sample_ra_msg2_t *)p_msg2_full->body;

  // Assemble MSG2
  if(memcpy_s(&p_msg2->g_b, sizeof(p_msg2->g_b), &g_sp_db.g_b,
              sizeof(g_sp_db.g_b)) ||
     memcpy_s(&p_msg2->spid, sizeof(sample_spid_t),
              &g_spid, sizeof(g_spid)))
    {
      fprintf(stderr,"\nError, memcpy failed in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

  // The service provider is responsible for selecting the proper EPID
  // signature type and to understand the implications of the choice!
  p_msg2->quote_type = SAMPLE_QUOTE_LINKABLE_SIGNATURE;
  p_msg2->kdf_id = SAMPLE_AES_CMAC_KDF_ID;

  // Create gb_ga
  sgx_ec_pub_t gb_ga[2];
  if(memcpy_s(&gb_ga[0], sizeof(gb_ga[0]), &g_sp_db.g_b,
              sizeof(g_sp_db.g_b))
     || memcpy_s(&gb_ga[1], sizeof(gb_ga[1]), &g_sp_db.g_a,
                 sizeof(g_sp_db.g_a)))
    {
      fprintf(stderr,"\nError, memcpy failed in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

  // Sign gb_ga
  ret = sgx_ecdsa_sign((uint8_t *)&gb_ga, sizeof(gb_ga),
                       (sgx_ec256_private_t *)&g_sp_priv_key,
                       (sgx_ec256_signature_t *)&p_msg2->sign_gb_ga,
                       ecc_state);
  if(SGX_SUCCESS != sample_ret)
    {
      fprintf(stderr, "\nError, sign ga_gb fail in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

  // Generate the CMACsmk for gb||SPID||TYPE||KDF_ID||Sigsp(gb,ga)
  uint8_t mac[SAMPLE_EC_MAC_SIZE] = {0};
  uint32_t cmac_size = offsetof(sample_ra_msg2_t, mac);
  ret = sgx_rijndael128_cmac_msg(&g_sp_db.smk_key,
                                 (uint8_t *)&p_msg2->g_b, cmac_size, &mac);
  if(SGX_SUCCESS != ret)
    {
      fprintf(stderr, "\nError, cmac fail in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }
  if(memcpy_s(&p_msg2->mac, sizeof(p_msg2->mac), mac, sizeof(mac)))
    {
      fprintf(stderr,"\nError, memcpy failed in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }

  if(memcpy_s(&p_msg2->sig_rl[0], sig_rl_size, sig_rl, sig_rl_size))
    {
      fprintf(stderr,"\nError, memcpy failed in [%s].", __FUNCTION__);
      ret = SP_INTERNAL_ERROR;
      break;
    }
  p_msg2->sig_rl_size = sig_rl_size;

  if(ret)
    {
      *pp_msg2 = NULL;
      SAFE_FREE(p_msg2_full);
    }
  else
    {
      // Freed by the network simulator in ra_free_network_response_buffer
      *pp_msg2 = p_msg2_full;
    }

  if(ecc_state)
    {
      sgx_ecc256_close_context(ecc_state);
    }

  return ret;
}


// The application must process the received message 2 from the client
