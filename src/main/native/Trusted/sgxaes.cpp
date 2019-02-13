#include <cassert>
#include <cstring>
#include "sgxaes.h"

KeySchedule::KeySchedule(const unsigned char* k, size_t k_len) {
	memset(this,0,sizeof(*this));
	switch (k_len) {
		case 16: intel_aes_encrypt_init_128(k,ks);nr=10;break;
		case 24: intel_aes_encrypt_init_192(k,ks);nr=12;break;
		case 32: intel_aes_encrypt_init_256(k,ks);nr=14;break;
		default: throw "Invalid AES keysize!";
	};
}

GcmContext::GcmContext() {
	memset(this,0,sizeof(*this));
}

AesGcm::AesGcm(const KeySchedule* ks, const unsigned char* iv, size_t iv_len) {
	memset(this,0,sizeof(*this));
	state=New;
	gctx.ks=ks;
	intel_aes_gcmINIT(gctx.htbl,gctx.ks->ks,gctx.ks->nr);
	if (iv_len==12) {
		memcpy(gctx.ctr,iv,12);
		gctx.ctr[15]=1;
	} else {
		throw "Only 96-bit IV supported!";
	}
	unsigned char out[AES_BLOCK_SIZE];
	const unsigned char zero[AES_BLOCK_SIZE]={};
	intel_aes_gcmENC(zero,out,&gctx,AES_BLOCK_SIZE);
	memcpy(gctx.x0,out,16);
	memset(gctx.t,0,16);
}

AesGcm::AesGcm(const AesGcm& other) {
	(void)other;
	if (state!=New) throw "Can't clone in this state";
}

void AesGcm::aad(const unsigned char* data, size_t data_len) {
	if (state!=New && state!=Aad) throw "Can't add AAD in this state";
	a_len+=data_len;

	size_t partial=data_len%AES_BLOCK_SIZE;
	unsigned char data2[AES_BLOCK_SIZE]={};
	if (partial!=0) {
		memcpy(data2,data+data_len-partial,partial);
		data_len-=partial;
		state=AadFinal;
	} else {
		state=Aad;
	}
	intel_aes_gcmAAD(gctx.htbl,data,data_len,gctx.t);
	if (partial!=0) {
		intel_aes_gcmAAD(gctx.htbl,data2,AES_BLOCK_SIZE,gctx.t);
	}
}

void AesGcm::encrypt(const unsigned char* plaintext, size_t plaintext_len, unsigned char* ciphertext, size_t ciphertext_len) {
	assert(plaintext_len==ciphertext_len);
	(void)ciphertext_len;
	if (state==Decrypting || state==Done) throw "Can't encrypt in this state";
	if (plaintext_len%AES_BLOCK_SIZE == 0) {
		state=Encrypting;
	} else {
		state=Done;
	}

	m_len+=plaintext_len;
	intel_aes_gcmENC(plaintext,ciphertext,&gctx,plaintext_len);
}

void AesGcm::decrypt(const unsigned char* ciphertext, size_t ciphertext_len, unsigned char* plaintext, size_t plaintext_len) {
	assert(plaintext_len==ciphertext_len);
	(void)ciphertext_len;
	if (state==Encrypting || state==Done) throw "Can't decrypt in this state";
	if (plaintext_len%AES_BLOCK_SIZE == 0) {
		state=Decrypting;
	} else {
		state=Done;
	}

	m_len+=plaintext_len;
	intel_aes_gcmDEC(ciphertext,plaintext,&gctx,plaintext_len);
}

Tag AesGcm::tag() const {
	Tag ret=Tag();
	intel_aes_gcmTAG(gctx.htbl,gctx.t,m_len,a_len,gctx.x0,ret.t);
	return ret;
}
