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

#include <openssl/cmac.h>
#include <openssl/conf.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bn.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <string.h>
#include <stdio.h>
#include "crypto.h"
#include "sp_crypto.h"

static enum _error_type {
	e_none,
	e_crypto,
	e_system,
	e_api
} error_type= e_none;

/*==========================================================================
 * EC key functions
 *========================================================================== */

/*==========================================================================
 * HMAC
 *========================================================================== */

int sha256_verify(const unsigned char *msg, size_t mlen, unsigned char *sig,
    size_t sigsz)
{
    if (sigsz != LC_SHA256_HASH_SIZE) {
        return 0;
    }

    lc_sha_state_handle_t sha_handle;
    lc_sha256_init(&sha_handle);
    lc_sha256_update(msg, mlen, sha_handle);
    lc_sha256_hash_t hash;
    lc_sha256_get_hash(sha_handle, &hash);
    lc_sha256_close(sha_handle);
    return memcmp(reinterpret_cast<const uint8_t *>(&hash), sig, LC_SHA256_HASH_SIZE) == 0;
}


/*==========================================================================
 * Certificate verification
 *========================================================================== */

int cert_load_file (X509 **cert, const char *filename)
{
	FILE *fp;

	error_type= e_none;


#ifdef _WIN32
	if ((fopen_s(&fp, filename, "r")) != 0) {
		error_type = e_system;
		return 0;
	}
#else
	if ((fp = fopen(filename, "r")) == NULL) {
		error_type = e_system;
		return 0;
	}
#endif


	*cert= PEM_read_X509(fp, NULL, NULL, NULL);
	if ( *cert == NULL ) error_type= e_crypto;

	fclose(fp);

	return (error_type == e_none);
}

int cert_load (X509 **cert, const char *pemdata)
{
	return cert_load_size(cert, pemdata, strlen(pemdata));
}

int cert_load_size (X509 **cert, const char *pemdata, size_t sz)
{
	BIO * bmem;
	error_type= e_none;

	bmem= BIO_new(BIO_s_mem());
	if ( bmem == NULL ) {
		error_type= e_crypto;
		goto cleanup;
	}

	if ( BIO_write(bmem, pemdata, (int) sz) != (int) sz ) {
		error_type= e_crypto;
		goto cleanup;
	}

	*cert= PEM_read_bio_X509(bmem, NULL, NULL, NULL);
	if ( *cert == NULL ) error_type= e_crypto;

cleanup:
	if ( bmem != NULL ) BIO_free(bmem);

	return (error_type == e_none);
}

X509_STORE *cert_init_ca(X509 *cert)
{
	X509_STORE *store;

	error_type= e_none;

	store= X509_STORE_new();
	if ( store == NULL ) {
		error_type= e_crypto;
		return NULL;
	}

	if ( X509_STORE_add_cert(store, cert) != 1 ) {
		X509_STORE_free(store);
		error_type= e_crypto;
		return NULL;
	}

	return store;
}

/*
 * Verify cert chain against our CA in store. Assume the first cert in
 * the chain is the one to validate. Note that a store context can only
 * be used for a single verification so we need to do this every time
 * we want to validate a cert.
 */

int cert_verify (X509_STORE *store, STACK_OF(X509) *chain)
{
	X509_STORE_CTX *ctx;
	X509 *cert= sk_X509_value(chain, 0);

	error_type= e_none;

	ctx= X509_STORE_CTX_new();
	if ( ctx == NULL ) {
		error_type= e_crypto;
		return 0;
	}

	if ( X509_STORE_CTX_init(ctx, store, cert, chain) != 1 ) {
		error_type= e_crypto;
		goto cleanup;
	}

	if ( X509_verify_cert(ctx) != 1 ) error_type=e_crypto;

cleanup:
	if ( ctx != NULL ) X509_STORE_CTX_free(ctx);

	return (error_type == e_none);
}

/*
 * Take an array of certificate pointers and build a stack.
 */

STACK_OF(X509) *cert_stack_build (X509 **certs)
{
	X509 **pcert;
	STACK_OF(X509) *stack;

	error_type= e_none;

	stack= sk_X509_new_null();
	if ( stack == NULL ) {
		error_type= e_crypto;
		return NULL;
	}

	for ( pcert= certs; *pcert!= NULL; ++pcert ) sk_X509_push(stack, *pcert);

	return stack;
}

void cert_stack_free (STACK_OF(X509) *chain)
{
	sk_X509_free(chain);
}

EVP_PKEY* buffer_to_public_key(char* input_buffer, int input_buf_size)
{
    BIO* bio = BIO_new_mem_buf(input_buffer, input_buf_size);
    EVP_PKEY *key = EVP_PKEY_new();
    key = PEM_read_bio_PUBKEY(bio, &key, NULL, NULL);
    if (key == NULL)
    {
        unsigned long ulErr = ERR_get_error();
        fprintf(stderr, "PEM_read_bio_RSA_PUBKEY() failed with '%s'\n", ERR_reason_error_string(ulErr));
    }

    BIO_free(bio);

    return key;
}

int public_encrypt(EVP_PKEY* key, unsigned char * data, int data_len, unsigned char* encrypted, size_t* encrypted_len)
{
    size_t outlen = 0;
    const int padding = RSA_PKCS1_PADDING;
    ENGINE *eng = NULL;
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new(key, eng);

    if (EVP_PKEY_encrypt_init(ctx) <= 0)
	{
		return -1;
	}
	if (EVP_PKEY_CTX_set_rsa_padding(ctx, padding) <= 0)
	{
		return -1;
	}
    if (EVP_PKEY_encrypt(ctx, NULL, &outlen, data, data_len) <= 0)
    {
        return -1;
    }
    if (*encrypted_len < outlen)
    {
        *encrypted_len = outlen;
        return 0;
    }

    if (EVP_PKEY_encrypt(ctx, encrypted, encrypted_len, data, data_len) <= 0)
    {
        return -1;
    }

    EVP_PKEY_CTX_free(ctx);

    return *encrypted_len;
}
