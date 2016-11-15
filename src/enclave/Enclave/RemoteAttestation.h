#include <stdint.h>
#include <stdlib.h>

#include "Crypto.h"

#ifdef  __cplusplus
extern "C" {
#endif

#define SAMPLE_MAC_SIZE             16  // Message Authentication Code
                                        // - 16 bytes
typedef uint8_t                     sample_mac_t[SAMPLE_MAC_SIZE];

#ifndef SAMPLE_FEBITSIZE
    #define SAMPLE_FEBITSIZE        256
#endif

#define SAMPLE_NISTP256_KEY_SIZE    (SAMPLE_FEBITSIZE/ 8 /sizeof(uint32_t))

typedef struct sample_ec_sign256_t
{
    uint32_t x[SAMPLE_NISTP256_KEY_SIZE];
    uint32_t y[SAMPLE_NISTP256_KEY_SIZE];
} sample_ec_sign256_t;

#pragma pack(push,1)

#define SAMPLE_SP_TAG_SIZE          16

typedef struct sp_aes_gcm_data_t {
    uint32_t        payload_size;       //  0: Size of the payload which is
                                        //     encrypted
    uint8_t         reserved[12];       //  4: Reserved bits
    uint8_t	        payload_tag[SAMPLE_SP_TAG_SIZE];
                                        // 16: AES-GMAC of the plain text,
                                        //     payload, and the sizes
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning ( disable:4200 )
#endif
    uint8_t         payload[];          // 32: Ciphertext of the payload
                                        //     followed by the plain text
#ifdef _MSC_VER
#pragma warning(pop)
#endif
} sp_aes_gcm_data_t;


#define ISVSVN_SIZE 2
#define PSDA_SVN_SIZE 4
#define GID_SIZE 4
#define PSVN_SIZE 18

// @TODO: Modify at production to use the values specified by the Production
// IAS API
typedef struct ias_platform_info_blob_t
{
     uint8_t sample_epid_group_status;
     uint16_t sample_tcb_evaluation_status;
     uint16_t pse_evaluation_status;
     uint8_t latest_equivalent_tcb_psvn[PSVN_SIZE];
     uint8_t latest_pse_isvsvn[ISVSVN_SIZE];
     uint8_t latest_psda_svn[PSDA_SVN_SIZE];
     uint8_t performance_rekey_gid[GID_SIZE];
     sample_ec_sign256_t signature;
} ias_platform_info_blob_t;


typedef struct sample_ra_att_result_msg_t {
    ias_platform_info_blob_t    platform_info_blob;
    sample_mac_t                mac;    // mac_smk(attestation_status)
    sp_aes_gcm_data_t           secret;
} sample_ra_att_result_msg_t;

#pragma pack(pop)


#ifndef SAMPLE_FEBITSIZE
    #define SAMPLE_FEBITSIZE                    256
#endif

#define SAMPLE_ECP_KEY_SIZE                     (SAMPLE_FEBITSIZE/8)

typedef struct sample_ec_priv_t
{
    uint8_t r[SAMPLE_ECP_KEY_SIZE];
} sample_ec_priv_t;

typedef struct sample_ec_dh_shared_t
{
    uint8_t s[SAMPLE_ECP_KEY_SIZE];
}sample_ec_dh_shared_t;

typedef uint8_t sample_ec_key_128bit_t[16];

#define SAMPLE_EC_MAC_SIZE 16

#ifdef  __cplusplus
extern "C" {
#endif

#ifndef _MSC_VER

#ifndef _ERRNO_T_DEFINED
#define _ERRNO_T_DEFINED
typedef int errno_t;
#endif
errno_t memcpy_s(void *dest, size_t numberOfElements, const void *src,
                 size_t count);
#endif


typedef enum _sample_derive_key_type_t
{
    SAMPLE_DERIVE_KEY_SMK = 0,
    SAMPLE_DERIVE_KEY_SK,
    SAMPLE_DERIVE_KEY_MK,
    SAMPLE_DERIVE_KEY_VK,
} sample_derive_key_type_t;

bool derive_key(
    const sample_ec_dh_shared_t *p_shared_key,
    uint8_t key_id,
    sample_ec_key_128bit_t *derived_key);

bool verify_cmac128(
    sample_ec_key_128bit_t mac_key,
    const uint8_t *p_data_buf,
    uint32_t buf_size,
    const uint8_t *p_mac_buf);
#ifdef  __cplusplus
}
#endif


typedef struct sgx_ra_msg1_t
{
    sample_ec_pub_t             g_a;        // the Endian-ness of Ga is
                                            // Little-Endian
    sample_epid_group_id_t      gid;        // the Endian-ness of GID is
                                            // Little-Endian
} sample_ra_msg1_t;

//Key Derivation Function ID : 0x0001  AES-CMAC Entropy Extraction and Key Expansion
const uint16_t SAMPLE_AES_CMAC_KDF_ID = 0x0001;

typedef struct sample_ra_msg2_t
{
    sample_ec_pub_t             g_b;        // the Endian-ness of Gb is
                                            // Little-Endian
    sample_spid_t               spid;
    uint16_t                    quote_type;  /* unlinkable Quote(0) or linkable Quote(0) in little endian*/
    uint16_t                    kdf_id;      /* key derivation function id in little endian.
                                             0x0001 for AES-CMAC Entropy Extraction and Key Derivation */
    sample_ec_sign256_t         sign_gb_ga; // In little endian
    sample_mac_t                mac;        // mac_smk(g_b||spid||quote_type||
                                            //         sign_gb_ga)
    uint32_t                    sig_rl_size;
#ifdef _MSC_VER
#pragma warning(push)
// Disable warning that array payload has size 0
#ifdef __INTEL_COMPILER
#pragma warning ( disable:94 )
#else
#pragma warning ( disable: 4200 )
#endif
#endif
    uint8_t                  sig_rl[];
#ifdef _MSC_VER
#pragma warning(pop)
#endif
} sample_ra_msg2_t;

typedef struct sample_ra_msg3_t
{
    sample_mac_t                mac;        // mac_smk(g_a||ps_sec_prop||quote)
    sample_ec_pub_t             g_a;        // the Endian-ness of Ga is
                                            // Little-Endian
    sample_ps_sec_prop_desc_t   ps_sec_prop;
#ifdef _MSC_VER
#pragma warning(push)
    // Disable warning that array payload has size 0
#ifdef __INTEL_COMPILER
#pragma warning ( disable:94 )
#else
#pragma warning ( disable: 4200 )
#endif
#endif
    uint8_t                  quote[];
#ifdef _MSC_VER
#pragma warning(pop)
#endif
} sample_ra_msg3_t;
