#include <stdint.h>
#include <stdlib.h>

#ifndef SGX_AES_H
#define SGX_AES_H

enum State {
	New,
	Aad,
	AadFinal,
	Encrypting,
	Decrypting,
	Done,
};

const size_t AES_MAX_EXP_KEY_SIZE = 8*15;
const size_t AES_BLOCK_SIZE = 16;

struct KeySchedule {
	uint32_t nb;
	uint32_t nr;
	const void* freebl_cipher_func;
	unsigned char iv[AES_BLOCK_SIZE*2];
	uint32_t ks[AES_MAX_EXP_KEY_SIZE];

	KeySchedule(const unsigned char* k, size_t k_len);
	KeySchedule(const KeySchedule& other) {
		(void)other;
	};
};

struct GcmContext {
	unsigned char htbl[16*AES_BLOCK_SIZE];
	unsigned char x0[AES_BLOCK_SIZE];
	unsigned char t[AES_BLOCK_SIZE];
	unsigned char ctr[AES_BLOCK_SIZE];
	const KeySchedule* ks;
	
	GcmContext();
	GcmContext(const GcmContext& other) {
		(void)other;
	};
};

struct Tag {
	unsigned char t[16];
};

struct AesGcm {
	GcmContext gctx;
	size_t a_len;
	size_t m_len;
	State state;

	AesGcm(const KeySchedule* ks, const unsigned char* iv, size_t iv_len);
	AesGcm(const AesGcm& other);
	void aad(const unsigned char* data, size_t data_len);
	void encrypt(const unsigned char* plaintext, size_t plaintext_len, unsigned char* ciphertext, size_t ciphertext_len);
	void decrypt(const unsigned char* ciphertext, size_t ciphertext_len, unsigned char* plaintext, size_t plaintext_len);
	Tag tag() const;
};

extern "C" {
	/* Prepares the constants used in the aggregated reduction method */
	void intel_aes_gcmINIT(unsigned char Htbl[16*AES_BLOCK_SIZE],
						   const uint32_t *KS,
						   int NR);

	/* Produces the final GHASH value */
	void intel_aes_gcmTAG(const unsigned char Htbl[16*AES_BLOCK_SIZE], 
						  const unsigned char *Tp, 
						  unsigned long Mlen, 
						  unsigned long Alen, 
						  const unsigned char* X0, 
						  unsigned char* TAG);

    /* Hashes the Additional Authenticated Data, should be used before enc/dec.
	   Operates on whole blocks only. Partial blocks should be padded externally. */
	void intel_aes_gcmAAD(unsigned char Htbl[16*AES_BLOCK_SIZE], 
						  const unsigned char *AAD, 
						  unsigned long Alen, 
						  unsigned char *Tp);

	/* Encrypts and hashes the Plaintext. 
	   Operates on any length of data, however partial block should only be encrypted
	   at the last call, otherwise the result will be incorrect. */
	void intel_aes_gcmENC(const unsigned char* PT, 
						  unsigned char* CT, 
						  GcmContext *Gctx, 
						  unsigned long len);
					  
	/* Similar to ENC, but decrypts the Ciphertext. */
	void intel_aes_gcmDEC(const unsigned char* CT, 
						  unsigned char* PT, 
						  GcmContext *Gctx, 
						  unsigned long len);

	void intel_aes_encrypt_init_128(const unsigned char *key, uint32_t *expanded);
	void intel_aes_encrypt_init_192(const unsigned char *key, uint32_t *expanded);
	void intel_aes_encrypt_init_256(const unsigned char *key, uint32_t *expanded);
}

#endif
