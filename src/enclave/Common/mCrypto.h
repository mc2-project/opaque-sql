// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef OE_SAMPLES_ATTESTATION_ENC_CRYPTO_H
#define OE_SAMPLES_ATTESTATION_ENC_CRYPTO_H

// Includes for mbedtls shipped with oe.
// Also add the following libraries to your linker command line:
// -loeenclave -lmbedcrypto -lmbedtls -lmbedx509
// Sample code reference: https://github.com/openenclave/openenclave/blob/master/samples/remote_attestation/common/crypto.h

#include "common.h"

#include <mbedtls/config.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>

class Crypto
{
  private:
    mbedtls_ctr_drbg_context m_ctr_drbg_contex;
    mbedtls_entropy_context m_entropy_context;
    mbedtls_pk_context m_pk_context;
    uint8_t m_public_key[OE_PUBLIC_KEY_SIZE];
    bool m_initialized;

  public:
    Crypto();
    ~Crypto();

    /** init_mbedtls initializes the crypto module.
     */
    bool init_mbedtls(void);

    /**
     * Get this enclave's own public key
     */
    void retrieve_public_key(uint8_t pem_public_key[OE_PUBLIC_KEY_SIZE]);

    /**
     * Encrypt encrypts the given data using the given public key.
     * Used to encrypt data using the public key of another enclave.
     */
    static bool encrypt(
        const uint8_t* pem_public_key,
        const uint8_t* data,
        size_t size,
        uint8_t* encrypted_data,
        size_t* encrypted_data_size);

    /**
     * decrypt decrypts the given data using current enclave's private key.
     * Used to receive encrypted data from another enclave.
     */
    bool decrypt(
        const uint8_t* encrypted_data,
        size_t encrypted_data_size,
        uint8_t* data,
        size_t* data_size);

    /**
     * Compute the sha256 hash of given data.
     */
    int sha256(const uint8_t* data, size_t data_size, uint8_t sha256[32]);

    /**
     * Compute the hmac of given data using the client's symmetric key
     * given to the enclave during attestation.
     */
    int hmac(const uint8_t* data, size_t data_length, uint8_t hmac[32]);

  private:
    /**
     * Crypto demonstrates use of mbedtls within the enclave to generate keys
     * and perform encryption. In this sample, each enclave instance generates
     * an ephemeral 2048-bit RSA key pair and shares the public key with the
     * other instance. The other enclave instance then replies with data
     * encrypted to the provided public key.
     */

    void cleanup_mbedtls(void);
};

#endif // OE_SAMPLES_ATTESTATION_ENC_CRYPTO_H
