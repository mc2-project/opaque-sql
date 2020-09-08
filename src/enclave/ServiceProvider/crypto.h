/*

Copyright 2018 Intel Corporation

This software and the related documents are Intel copyrighted materials,
and your use of them is governed by the express license under which they
were provided to you (License). Unless the License provides otherwise,
you may not use, modify, copy, publish, distribute, disclose or transmit
this software or the related documents without Intel's prior written
permission.

This software and the related documents are provided as is, with no
express or implied warranties, other than those that are expressly stated
in the License.

*/

#ifndef _CRYPTO_INIT_H
#define _CRYPTO_INIT_H

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#define KEY_PUBLIC	0
#define KEY_PRIVATE	1

#ifdef __cplusplus
extern "C" {
#endif

/* HMAC */

int sha256_verify(const unsigned char *msg, size_t mlen, unsigned char *sig,
	size_t sigsz);

/* Certs */

int cert_load_file (X509 **cert, const char *filename);
int cert_load_size (X509 **cert, const char *pemdata, size_t sz);
int cert_load (X509 **cert, const char *pemdata);
X509_STORE *cert_init_ca(X509 *cert);
int cert_verify(X509_STORE *store, STACK_OF(X509) *chain);
STACK_OF(X509) *cert_stack_build(X509 **certs);
void cert_stack_free(STACK_OF(X509) *chain);

EVP_PKEY* buffer_to_public_key(char* input_buffer, int input_buf_size);
int public_encrypt(EVP_PKEY* key, unsigned char * data, int data_len, unsigned char* encrypted, size_t* encrypted_len);

#ifdef __cplusplus
};
#endif

#endif
