/**
 *
 * INTEL CONFIDENTIAL
 * Copyright(c) 2011-2016 Intel Corporation All Rights Reserved.
 *
 * The source code contained or described herein and all documents related to
 * the source code ("Material") are owned by Intel Corporation or its suppliers
 * or licensors. Title to the Material remains with Intel Corporation or its
 * suppliers and licensors. The Material contains trade secrets and proprietary
 * and confidential information of Intel or its suppliers and licensors. The
 * Material is protected by worldwide copyright and trade secret laws and treaty
 * provisions. No part of the Material may be used, copied, reproduced, modified,
 * published, uploaded, posted, transmitted, distributed, or disclosed in any
 * way without Intel's prior express written permission.
 *
 * No license under any patent, copyright, trade secret or other intellectual
 * property right is granted to or conferred upon you by disclosure or delivery
 * of the Materials, either expressly, by implication, inducement, estoppel or
 * otherwise. Any license under such intellectual property rights must be
 * express and approved by Intel(R) in writing.
 *
 */



/**
* File: lc_libcrypto.h
* Description:
*     Interface for generic crypto library APIs. This library does NOT have
*     production quality crypto implementations.
*     Some methods have been constructed to facilitate reproduceable results to
*     aid in the debugging of the lc application.
*/
#pragma once

#include <stdint.h>
#include <cstring>
#include "common.h"
#include "openssl/evp.h"
#include "openssl/pem.h"
#include "openssl/cmac.h"

//#define TEST_EC

// lc = libcrypto

typedef enum lc_status_t {
    LC_SUCCESS                  = 0,
    LC_ERROR_UNEXPECTED         ,      // Unexpected error
    LC_ERROR_INVALID_PARAMETER  ,      // The parameter is incorrect
    LC_ERROR_OUT_OF_MEMORY      ,      // Not enough memory is available to complete this operation

} lc_status_t;

// SHA
#define LC_SHA256_HASH_SIZE            32
// ECC
#define LC_ECP256_KEY_SIZE             32
#define LC_NISTP_ECP256_KEY_SIZE       (LC_ECP256_KEY_SIZE/sizeof(uint32_t))
// AES-GCM
#define LC_AESGCM_IV_SIZE              12
#define LC_AESGCM_KEY_SIZE             16
#define LC_AESGCM_MAC_SIZE             16
#define LC_CMAC_KEY_SIZE               16
#define LC_CMAC_MAC_SIZE               16
#define LC_AESCTR_KEY_SIZE             16

typedef struct lc_ec256_dh_shared_t
{
  uint8_t s[LC_ECP256_KEY_SIZE];
} lc_ec256_dh_shared_t;

typedef struct lc_ec256_private_t
{
    uint8_t r[LC_ECP256_KEY_SIZE];
} lc_ec256_private_t;

typedef struct lc_ec256_public_t
{
    uint8_t gx[LC_ECP256_KEY_SIZE];
    uint8_t gy[LC_ECP256_KEY_SIZE];
} lc_ec256_public_t;

typedef struct lc_ec256_signature_t
{
    uint32_t x[LC_NISTP_ECP256_KEY_SIZE];
    uint32_t y[LC_NISTP_ECP256_KEY_SIZE];
} lc_ec256_signature_t;

typedef SHA256_CTX* lc_sha_state_handle_t;
typedef void* lc_cmac_state_handle_t;
typedef void* lc_ecc_state_handle_t;

typedef uint8_t lc_sha256_hash_t[LC_SHA256_HASH_SIZE];

typedef uint8_t lc_aes_gcm_128bit_key_t[LC_AESGCM_KEY_SIZE];
typedef uint8_t lc_aes_gcm_128bit_tag_t[LC_AESGCM_MAC_SIZE];
typedef uint8_t lc_cmac_128bit_key_t[LC_CMAC_KEY_SIZE];
typedef uint8_t lc_cmac_128bit_tag_t[LC_CMAC_MAC_SIZE];
typedef uint8_t lc_aes_ctr_128bit_key_t[LC_AESCTR_KEY_SIZE];

#ifdef __cplusplus
    #define EXTERN_C extern "C"
#else
    #define EXTERN_C
#endif

#ifdef _MSC_VER
    #ifdef LC_LIBCRYPTO_EXPORTS
        #define LC_LIBCRYPTO_API EXTERN_C __declspec( dllexport )
    #else
        #define LC_LIBCRYPTO_API EXTERN_C __declspec( dllimport )
    #endif
#else
    #define LC_LIBCRYPTO_API EXTERN_C
#endif


// helper function
lc_status_t lc_ssl2sgx(EC_KEY *ssl_key, lc_ec256_private_t *p_private, lc_ec256_public_t *p_public);

/* Rijndael AES-GCM
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Inputs: lc_aes_gcm_128bit_key_t *p_key - Pointer to key used in encryption/decryption operation
*			uint8_t *p_src - Pointer to input stream to be encrypted/decrypted
*			uint32_t src_len - Length of input stream to be encrypted/decrypted
*			uint8_t *p_iv - Pointer to initialization vector to use
*			uint32_t iv_len - Length of initialization vector
*			uint8_t *p_aad - Pointer to input stream of additional authentication data
*			uint32_t aad_len - Length of additional authentication data stream
*			lc_aes_gcm_128bit_tag_t *p_in_mac - Pointer to expected MAC in decryption process
*	Output: uint8_t *p_dst - Pointer to cipher text. Size of buffer should be >= src_len.
*			lc_aes_gcm_128bit_tag_t *p_out_mac - Pointer to MAC generated from encryption process
* NOTE: Wrapper is responsible for confirming decryption tag matches encryption tag */
LC_LIBCRYPTO_API lc_status_t lc_rijndael128GCM_encrypt(const lc_aes_gcm_128bit_key_t *p_key,
                                                       const uint8_t *p_src, uint32_t src_len,
                                                       uint8_t *p_dst,
                                                       const uint8_t *p_iv, uint32_t iv_len,
                                                       const uint8_t *p_aad, uint32_t aad_len,
                                                       lc_aes_gcm_128bit_tag_t *p_out_mac);

/* Message Authentication - Rijndael 128 CMAC
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Inputs: lc_cmac_128bit_key_t *p_key - Pointer to key used in encryption/decryption operation
*			uint8_t *p_src - Pointer to input stream to be MACd
*			uint32_t src_len - Length of input stream to be MACd
*	Output: lc_cmac_gcm_128bit_tag_t *p_mac - Pointer to resultant MAC */
LC_LIBCRYPTO_API lc_status_t lc_rijndael128_cmac_msg(const lc_cmac_128bit_key_t *p_key,
                                                     const uint8_t *p_src, uint32_t src_len,
                                                     lc_cmac_128bit_tag_t *p_mac);


/*
* Elliptic Curve Crytpography - Based on GF(p), 256 bit
*/
/* Allocates and initializes ecc context
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS or failure as defined LC_Error.h
*	Output: lc_ecc_state_handle_t ecc_handle - Handle to ECC crypto system  */
LC_LIBCRYPTO_API lc_status_t lc_ecc256_open_context(lc_ecc_state_handle_t* ecc_handle);

/* Cleans up ecc context
* Parameters:
* 	Return: lc_status_t  - LC_SUCCESS or failure as defined LC_Error.h
*	Output: lc_ecc_state_handle_t ecc_handle - Handle to ECC crypto system  */
LC_LIBCRYPTO_API lc_status_t lc_ecc256_close_context(lc_ecc_state_handle_t ecc_handle);

/* Populates private/public key pair - caller code allocates memory
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Inputs: lc_ecc_state_handle_t ecc_handle - Handle to ECC crypto system
*	Outputs: lc_ec256_private_t *p_private - Pointer to the private key
*			 lc_ec256_public_t *p_public - Pointer to the public key  */
LC_LIBCRYPTO_API lc_status_t lc_ecc256_create_key_pair(lc_ec256_private_t *p_private,
                                                       lc_ec256_public_t *p_public,
                                                       lc_ecc_state_handle_t ecc_handle);

/* Computes DH shared key based on private B key (local) and remote public Ga Key
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Inputs: lc_ecc_state_handle_t ecc_handle - Handle to ECC crypto system
*			lc_ec256_private_t *p_private_b - Pointer to the local private key - LITTLE ENDIAN
*			lc_ec256_public_t *p_public_ga - Pointer to the remote public key - LITTLE ENDIAN
*	Output: lc_ec256_dh_shared_t *p_shared_key - Pointer to the shared DH key - LITTLE ENDIAN
x-coordinate of (privKeyB - pubKeyA) */
LC_LIBCRYPTO_API lc_status_t lc_ecc256_compute_shared_dhkey(lc_ec256_private_t *p_private_b,
                                                            lc_ec256_public_t *p_public_ga,
                                                            lc_ec256_dh_shared_t *p_shared_key,
                                                            lc_ecc_state_handle_t ecc_handle);


/* Computes signature for data based on private key
*
* A message digest is a fixed size number derived from the original message with
* an applied hash function over the binary code of the message. (SHA256 in this case)
* The signer's private key and the message digest are used to create a signature.
*
* A digital signature over a message consists of a pair of large numbers, 256-bits each,
* which the given function computes.
*
* The scheme used for computing a digital signature is of the ECDSA scheme,
* an elliptic curve of the DSA scheme.
*
* The keys can be generated and set up by the function: sgx_ecc256_create_key_pair.
*
* The elliptic curve domain parameters must be created by function:
*     lc_ecc256_open_context
*
* Return: If context, private key, signature or data pointer is NULL,
*                       LC_ERROR_INVALID_PARAMETER is returned.
*         If the signature creation process fails then LC_ERROR_UNEXPECTED is returned.
*
* Parameters:
*   Return: lc_status_t - LC_SUCCESS, success, error code otherwise.
*   Inputs: lc_ecc_state_handle_t ecc_handle - Handle to the ECC crypto system
*           lc_ec256_private_t *p_private - Pointer to the private key - LITTLE ENDIAN
*           uint8_t *p_data - Pointer to the data to be signed
*           uint32_t data_size - Size of the data to be signed
*   Output: ec256_signature_t *p_signature - Pointer to the signature - LITTLE ENDIAN */
LC_LIBCRYPTO_API lc_status_t lc_ecdsa_sign(const uint8_t *p_data,
                                        uint32_t data_size,
                                        lc_ec256_private_t *p_private,
                                        lc_ec256_signature_t *p_signature,
                                        lc_ecc_state_handle_t ecc_handle);

/* Allocates and initializes sha256 state
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*   Output: lc_sha_state_handle_t sha_handle - Handle to the SHA256 state  */
LC_LIBCRYPTO_API lc_status_t lc_sha256_init(lc_sha_state_handle_t* p_sha_handle);

/* Updates sha256 has calculation based on the input message
* Parameters:
*   Return: lc_status_t  - LC_SUCCESS or failure.
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state
*	        uint8_t *p_src - Pointer to the input stream to be hashed
*          uint32_t src_len - Length of the input stream to be hashed  */
LC_LIBCRYPTO_API lc_status_t lc_sha256_update(const uint8_t *p_src, uint32_t src_len, lc_sha_state_handle_t sha_handle);

/* Returns Hash calculation
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state
*   Output: lc_sha256_hash_t *p_hash - Resultant hash from operation  */
LC_LIBCRYPTO_API lc_status_t lc_sha256_get_hash(lc_sha_state_handle_t sha_handle, lc_sha256_hash_t *p_hash);

/* Cleans up sha state
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state  */
LC_LIBCRYPTO_API lc_status_t lc_sha256_close(lc_sha_state_handle_t sha_handle);


lc_status_t print_priv_key(lc_ec256_private_t p_private);
lc_status_t print_pub_key(lc_ec256_public_t p_public);
lc_status_t print_ec_key(EC_KEY *ec_key);
EC_POINT *get_ec_point(lc_ec256_public_t *p_public);
