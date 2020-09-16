/*
 * Copyright (C) 2011-2017 Intel Corporation. All rights reserved.
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
#include <sys/errno.h>

#include "openssl/evp.h"
#include "openssl/pem.h"
#include "openssl/cmac.h"
#include "openssl/hmac.h"
#include <openssl/err.h>
#include <openssl/obj_mac.h>
#include <openssl/bn.h>
#include <openssl/rand.h>
#include <openssl/ec.h>
#include <openssl/rand.h>

#include <mbedtls/entropy.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/cipher.h>
#include <mbedtls/gcm.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>
#include <mbedtls/x509_crt.h>
#include <mbedtls/error.h>

#include "common.h"

//#define TEST_EC

// lc = libcrypto

typedef enum lc_status_t {
    LC_SUCCESS                  = 0,
    LC_ERROR_UNEXPECTED         ,      // Unexpected error
    LC_ERROR_INVALID_PARAMETER  ,      // The parameter is incorrect
    LC_ERROR_OUT_OF_MEMORY      ,      // Not enough memory is available to complete this operation

} lc_status_t;

#define WARN_UNUSED __attribute__((warn_unused_result))

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
// Currently oesign only supports an rsa public key of size 3072 bits
#define lc_rsa_public_t                (3072/8)


// copied from intel sgx sdk tcrypto.h
#define SGX_ECP256_KEY_SIZE             32
#define SGX_NISTP_ECP256_KEY_SIZE       (SGX_ECP256_KEY_SIZE/sizeof(uint32_t))
#define SGX_AESGCM_KEY_SIZE             16

typedef struct _sgx_ec256_dh_shared_t
{
    uint8_t s[SGX_ECP256_KEY_SIZE];
} sgx_ec256_dh_shared_t;

typedef struct _sgx_ec256_private_t
{
    uint8_t r[SGX_ECP256_KEY_SIZE];
} sgx_ec256_private_t;

typedef struct _sgx_ec256_public_t
{
    uint8_t gx[SGX_ECP256_KEY_SIZE];
    uint8_t gy[SGX_ECP256_KEY_SIZE];
} sgx_ec256_public_t;

typedef struct _sgx_ec256_signature_t
{
    uint32_t x[SGX_NISTP_ECP256_KEY_SIZE];
    uint32_t y[SGX_NISTP_ECP256_KEY_SIZE];
} sgx_ec256_signature_t;

typedef sgx_ec256_dh_shared_t lc_ec256_dh_shared_t;
typedef sgx_ec256_private_t lc_ec256_private_t;
typedef sgx_ec256_public_t lc_ec256_public_t;
typedef sgx_ec256_signature_t lc_ec256_signature_t;

typedef SHA256_CTX* lc_sha_state_handle_t;
typedef void* lc_cmac_state_handle_t;
typedef void* lc_ecc_state_handle_t;

typedef uint8_t lc_sha256_hash_t[LC_SHA256_HASH_SIZE];

typedef uint8_t lc_aes_gcm_128bit_key_t[LC_AESGCM_KEY_SIZE];
typedef uint8_t lc_aes_gcm_128bit_tag_t[LC_AESGCM_MAC_SIZE];
typedef uint8_t lc_cmac_128bit_key_t[LC_CMAC_KEY_SIZE];
typedef uint8_t lc_cmac_128bit_tag_t[LC_CMAC_MAC_SIZE];
typedef uint8_t lc_aes_ctr_128bit_key_t[LC_AESCTR_KEY_SIZE];

typedef uint8_t sgx_aes_gcm_128bit_key_t[SGX_AESGCM_KEY_SIZE];

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
void lc_ssl2sgx(EC_KEY *ssl_key, lc_ec256_private_t *p_private, lc_ec256_public_t *p_public);

void reverse_endian(uint8_t *input, uint8_t *output, uint32_t len);
EC_KEY *get_priv_key(lc_ec256_private_t *p_private);

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
LC_LIBCRYPTO_API lc_status_t WARN_UNUSED lc_rijndael128GCM_encrypt(
    const lc_aes_gcm_128bit_key_t *p_key,
    const uint8_t *p_src, uint32_t src_len,
    uint8_t *p_dst,
    const uint8_t *p_iv, uint32_t iv_len,
    const uint8_t *p_aad, uint32_t aad_len,
    lc_aes_gcm_128bit_tag_t *p_out_mac);

lc_status_t WARN_UNUSED lc_rijndael128GCM_decrypt(
    unsigned char *ciphertext, int ciphertext_len, unsigned char *aad,
    int aad_len, unsigned char *tag, unsigned char *key, unsigned char *iv,
    unsigned char *plaintext) __attribute__((warn_unused_result));

/* Message Authentication - Rijndael 128 CMAC
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Inputs: lc_cmac_128bit_key_t *p_key - Pointer to key used in encryption/decryption operation
*			uint8_t *p_src - Pointer to input stream to be MACd
*			uint32_t src_len - Length of input stream to be MACd
*	Output: lc_cmac_gcm_128bit_tag_t *p_mac - Pointer to resultant MAC */
LC_LIBCRYPTO_API lc_status_t WARN_UNUSED lc_rijndael128_cmac_msg(
    const lc_cmac_128bit_key_t *p_key,
    const uint8_t *p_src, uint32_t src_len,
    lc_cmac_128bit_tag_t *p_mac);


/* Populates private/public key pair - caller code allocates memory
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Outputs: lc_ec256_private_t *p_private - Pointer to the private key
*			 lc_ec256_public_t *p_public - Pointer to the public key  */
LC_LIBCRYPTO_API lc_status_t WARN_UNUSED lc_ecc256_create_key_pair(
    lc_ec256_private_t *p_private,
    lc_ec256_public_t *p_public);

/* Computes DH shared key based on private B key (local) and remote public Ga Key
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*			lc_ec256_private_t *p_private_b - Pointer to the local private key - LITTLE ENDIAN
*			lc_ec256_public_t *p_public_ga - Pointer to the remote public key - LITTLE ENDIAN
*	Output: lc_ec256_dh_shared_t *p_shared_key - Pointer to the shared DH key - LITTLE ENDIAN
x-coordinate of (privKeyB - pubKeyA) */
LC_LIBCRYPTO_API lc_status_t WARN_UNUSED lc_ecc256_compute_shared_dhkey(
    lc_ec256_private_t *p_private_b,
    lc_ec256_public_t *p_public_ga,
    lc_ec256_dh_shared_t *p_shared_key);


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
*   Inputs: lc_ec256_private_t *p_private - Pointer to the private key - LITTLE ENDIAN
*           uint8_t *p_data - Pointer to the data to be signed
*           uint32_t data_size - Size of the data to be signed
*   Output: ec256_signature_t *p_signature - Pointer to the signature - LITTLE ENDIAN */
LC_LIBCRYPTO_API lc_status_t WARN_UNUSED lc_ecdsa_sign(
    const uint8_t *p_data,
    uint32_t data_size,
    lc_ec256_private_t *p_private,
    lc_ec256_signature_t *p_signature);

/* Allocates and initializes sha256 state
* Parameters:
*   Output: lc_sha_state_handle_t sha_handle - Handle to the SHA256 state  */
LC_LIBCRYPTO_API void lc_sha256_init(
    lc_sha_state_handle_t* p_sha_handle);

/* Updates sha256 has calculation based on the input message
* Parameters:
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state
*	        uint8_t *p_src - Pointer to the input stream to be hashed
*          uint32_t src_len - Length of the input stream to be hashed  */
LC_LIBCRYPTO_API void lc_sha256_update(
    const uint8_t *p_src, uint32_t src_len, lc_sha_state_handle_t sha_handle);

/* Returns Hash calculation
* Parameters:
*	Return: lc_status_t  - LC_SUCCESS on success, error code otherwise.
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state
*   Output: lc_sha256_hash_t *p_hash - Resultant hash from operation  */
LC_LIBCRYPTO_API void lc_sha256_get_hash(
    lc_sha_state_handle_t sha_handle, lc_sha256_hash_t *p_hash);

/* Cleans up sha state
* Parameters:
*	Input:  lc_sha_state_handle_t sha_handle - Handle to the SHA256 state  */
LC_LIBCRYPTO_API void lc_sha256_close(
    lc_sha_state_handle_t sha_handle);


lc_status_t WARN_UNUSED print_priv_key(lc_ec256_private_t p_private);
lc_status_t WARN_UNUSED print_pub_key(lc_ec256_public_t p_public);
void print_ec_key(EC_KEY *ec_key);
EC_POINT *get_ec_point(lc_ec256_public_t *p_public);

int lc_compute_sha256(const uint8_t* data, size_t data_size, uint8_t sha256[LC_SHA256_HASH_SIZE]);
