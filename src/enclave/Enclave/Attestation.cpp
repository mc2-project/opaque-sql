// Copyright (c) Open Enclave SDK contributors.
// Licensed under the MIT License.

#include <stdexcept>
#include <string.h>
#include <iostream>
#include <fstream>

#include "Attestation.h"
#include "enclave_pubkey.h"

#include <openenclave/attestation/attester.h>
#include <openenclave/attestation/custom_claims.h>
#include <openenclave/attestation/verifier.h>
#include <openenclave/attestation/sgx/report.h>

Attestation::Attestation(Crypto* crypto)
{
    m_crypto = crypto;
}

/**
 * Get format settings for the given enclave.
 */
bool Attestation::get_format_settings(
    const oe_uuid_t* format_id,
    uint8_t** format_settings,
    size_t* format_settings_size)
{
    bool ret = false;

    // Intialize verifier to get enclave's format settings.
    if (oe_verifier_initialize() != OE_OK)
    {
        throw std::runtime_error("oe_verifier_initialize failed");
        goto exit;
    }

    // Use the plugin.
    if (oe_verifier_get_format_settings(
            format_id, format_settings, format_settings_size) != OE_OK)
    {
        throw std::runtime_error("oe_verifier_get_format_settings failed");
        goto exit;
    }
    ret = true;

exit:
    return ret;
}

/**
 * Generate evidence for the given data.
 */
bool Attestation::generate_attestation_evidence(
    const oe_uuid_t* format_id,
    uint8_t* format_settings,
    size_t format_settings_size,
    const uint8_t* data,
    const size_t data_size,
    uint8_t** evidence,
    size_t* evidence_size)
{
    bool ret = false;
    uint8_t hash[32];
    oe_result_t result = OE_OK;
    uint8_t* custom_claims_buffer = nullptr;
    size_t custom_claims_buffer_size = 0;
    char custom_claim1_name[] = "Event";
    char custom_claim1_value[] = "Attestation sample";
    char custom_claim2_name[] = "Public key hash";

    // The custom_claims[1].value will be filled with hash of public key later
    oe_claim_t custom_claims[2] = {
        {.name = custom_claim1_name,
         .value = (uint8_t*)custom_claim1_value,
         .value_size = sizeof(custom_claim1_value)},
        {.name = custom_claim2_name, .value = nullptr, .value_size = 0}};

    if (m_crypto->sha256(data, data_size, hash) != 0)
    {
        throw std::runtime_error("data hashing failed");
        goto exit;
    }

    // Initialize attester and use the plugin.
    result = oe_attester_initialize();
    if (result != OE_OK)
    {
        throw std::runtime_error("oe_attester_initialize failed.");
        goto exit;
    }

    // serialize the custom claims, store hash of data in custom_claims[1].value
    custom_claims[1].value = hash;
    custom_claims[1].value_size = sizeof(hash);

    if (oe_serialize_custom_claims(
            custom_claims,
            2,
            &custom_claims_buffer,
            &custom_claims_buffer_size) != OE_OK)
    {
        throw std::runtime_error("oe_serialize_custom_claims failed.");
        goto exit;
    }

    // Generate evidence based on the format selected by the attester.
    result = oe_get_evidence(
        format_id,
        0,
        custom_claims_buffer,
        custom_claims_buffer_size,
        format_settings,
        format_settings_size,
        evidence,
        evidence_size,
        nullptr,
        0);
    if (result != OE_OK)
    {
        throw std::runtime_error("oe_get_evidence failed.(%s)");
        goto exit;
    }

    ret = true;
exit:
    return ret;
}

/**
 * Helper function used to make the claim-finding process more convenient. Given
 * the claim name, claim list, and its size, returns the claim with that claim
 * name in the list.
 */
static const oe_claim_t* _find_claim(
    const oe_claim_t* claims,
    size_t claims_size,
    const char* name)
{
    for (size_t i = 0; i < claims_size; i++)
    {
        if (strcmp(claims[i].name, name) == 0)
            return &(claims[i]);
    }
    return nullptr;
}

/**
 * Attest the given evidence and accompanying data. It consists of the
 * following three steps:
 *
 * 1) The evidence is first attested using the oe_verify_evidence API.
 * This ensures the authenticity of the enclave that generated the evidence.
 * 2) Next, to establish trust in the enclave that generated the
 * evidence, the signer_id, product_id, and security version values are
 * checked to see if they are predefined trusted values.
 * 3) Once the enclave's trust has been established,
 * the validity of accompanying data is ensured by comparing its SHA256 digest
 * against the OE_CLAIM_CUSTOM_CLAIMS_BUFFER claim.
 */
bool Attestation::attest_attestation_evidence(
    const oe_uuid_t* format_id,
    const uint8_t* evidence,
    size_t evidence_size,
    const uint8_t* data,
    size_t data_size)
{
    bool ret = false;
    uint8_t hash[32];
    oe_result_t result = OE_OK;
    oe_claim_t* claims = nullptr;
    size_t claims_length = 0;
    const oe_claim_t* claim;
    oe_claim_t* custom_claims = nullptr;
    size_t custom_claims_length = 0;

    // Read in the public key as a string

    uint8_t m_enclave_signer_id[OE_SIGNER_ID_SIZE];
    size_t signer_size = sizeof(m_enclave_signer_id);

//    std::cout << "Attestation.cpp - before read environment variable" << std::endl;
//    std::string public_key_file = std::string(std::getenv("OPAQUE_HOME"));
//    public_key_file.append("/public_key.pub");
//    std::cout << "Attestation.cpp - after read environment variable" << std::endl;
//
//    std::cout << "Attestation.cpp - before create file stream" << std::endl;
//    std::ifstream t(public_key_file.c_str());
//    std::string public_key;
//    std::cout << "Attestation.cpp - after create file stream" << std::endl;
//
//    std::cout << "Attestation.cpp - before read from file" << std::endl;
//    t.seekg(0, std::ios::end);
//    size_t public_key_size = t.tellg();
//    public_key.reserve(public_key_size + 1);
//    t.seekg(0, std::ios::beg);
//    std::cout << "Attestation.cpp - after read from file" << std::endl;
//
//    std::cout << "Attestation.cpp - not sure what this is" << std::endl;
//    public_key.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
//    public_key.replace(public_key_size, 1, "\0");
//    std::cout << "Attestation.cpp - not sure what this is" << std::endl;
//
//    std::cout << "Attestation.cpp - public key: " + public_key << std::endl;

    if (oe_sgx_get_signer_id_from_public_key(
            OTHER_ENCLAVE_PUBLIC_KEY,
            sizeof(OTHER_ENCLAVE_PUBLIC_KEY),
            m_enclave_signer_id,
            &signer_size) != OE_OK)
    {
        throw std::runtime_error("oe_sgx_get_signer_id_from_public_key failed\n");
        return false;
    }

    // While attesting, the evidence being attested must not be tampered
    // with. Ensure that it has been copied over to the enclave.
    if (!oe_is_within_enclave(evidence, evidence_size))
    {
        throw std::runtime_error("Cannot attest evidence in host memory. Unsafe.");
        goto exit;
    }

    // 1) Validate the evidence's trustworthiness
    // Verify the evidence to ensure its authenticity.
    result = oe_verify_evidence(
        format_id,
        evidence,
        evidence_size,
        nullptr,
        0,
        nullptr,
        0,
        &claims,
        &claims_length);
    if (result != OE_OK)
    {
        throw std::runtime_error("oe_verify_evidence failed (%s).\n");
        goto exit;
    }

    // 2) validate the enclave identity's signer_id is the hash of the public
    // signing key that was used to sign an enclave. Check that the enclave was
    // signed by an trusted entity.

    // Validate the signer id.
//    if ((claim = _find_claim(claims, claims_length, OE_CLAIM_SIGNER_ID)) ==
//        nullptr)
//    {
//        throw std::runtime_error("Could not find claim.");
//        goto exit;
//    };
//
//    if (claim->value_size != OE_SIGNER_ID_SIZE)
//    {
//        throw std::runtime_error("signer_id size checking failed");
//        goto exit;
//    }
//
//    if (memcmp(claim->value, m_enclave_signer_id, OE_SIGNER_ID_SIZE) != 0)
//    {
//        throw std::runtime_error("signer_id checking failed");
//        goto exit;
//    }
//
//    // Check the enclave's product id.
    if ((claim = _find_claim(claims, claims_length, OE_CLAIM_PRODUCT_ID)) ==
        nullptr)
    {
        throw std::runtime_error("could not find claim");
        goto exit;
    };

    if (claim->value_size != OE_PRODUCT_ID_SIZE)
    {
        throw std::runtime_error(
            "product_id size checking failed");
        goto exit;
    }

    if (*(claim->value) != 1)
    {
        throw std::runtime_error("product_id checking failed");
        goto exit;
    }

    // Check the enclave's security version.
    if ((claim = _find_claim(
             claims, claims_length, OE_CLAIM_SECURITY_VERSION)) == nullptr)
    {
        throw std::runtime_error("could not find claim");
        goto exit;
    };

    if (claim->value_size != sizeof(uint32_t))
    {
        throw std::runtime_error(
            "security_version size checking failed");
        goto exit;
    }

    if (*(claim->value) < 1)
    {
        throw std::runtime_error("security_version checking failed");
        goto exit;
    }

    // 3) Validate the custom claims buffer
    //    Deserialize the custom claims buffer to custom claims list, then fetch
    //    the hash value of the data held in custom_claims[1].
    if ((claim = _find_claim(
             claims, claims_length, OE_CLAIM_CUSTOM_CLAIMS_BUFFER)) == nullptr)
    {
        throw std::runtime_error("Could not find claim.");
        goto exit;
    }

    if (m_crypto->sha256(data, data_size, hash) != 0)
    {
        goto exit;
    }

    // deserialize the custom claims buffer
    if (oe_deserialize_custom_claims(
            claim->value,
            claim->value_size,
            &custom_claims,
            &custom_claims_length) != OE_OK)
    {
        throw std::runtime_error("oe_deserialize_custom_claims failed.");
        goto exit;
    }

    if (custom_claims[1].value_size != sizeof(hash) ||
        memcmp(custom_claims[1].value, hash, sizeof(hash)) != 0)
    {
        throw std::runtime_error("hash mismatch");
        goto exit;
    }

    ret = true;
exit:
    // Shut down attester/verifier and free claims.
    oe_attester_shutdown();
    oe_verifier_shutdown();
    oe_free_claims(claims, claims_length);
    return ret;
}
