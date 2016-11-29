#ifndef REMOTE_ATTESTATION_H
#define REMOTE_ATTESTATION_H

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include "sgx_edger8r.h"
#include "user_types.h"
#include "stdbool.h"
#include "sgx_key_exchange.h"
#include "sgx_trts.h"
#include "common.h"
#include "key.h"
#include "Crypto.h"

sgx_status_t enclave_init_ra(int b_pse, sgx_ra_context_t *p_context);
sgx_status_t enclave_ra_close(sgx_ra_context_t context);
sgx_status_t verify_att_result_mac(sgx_ra_context_t context, uint8_t* message,
                                   size_t message_size, uint8_t* mac,
                                   size_t mac_size);
sgx_status_t put_secret_data(sgx_ra_context_t context,
                             uint8_t* p_secret,
                             uint32_t secret_size,
                             uint8_t* gcm_mac);

sgx_status_t test_get_key(sgx_ra_context_t context);

#endif
