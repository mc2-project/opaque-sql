#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "define.h"

// Declarations for C/C++ standard library functions that are not present in the trusted standard
// libraries, but are reimplemented in Enclave/util.cpp. This allows us to use these functions
// uniformly across trusted and untrusted code.
int printf(const char* format, ...);
void exit(int exit_code);
namespace std {
    using ::exit;
}

#ifndef COMMON_H
#define COMMON_H

#ifdef DEBUG
#define debug(...) printf(__VA_ARGS__)
#else
#define debug(...) do {} while (0)
#endif

#ifdef PERF
#define perf(...) printf(__VA_ARGS__)
#else
#define perf(...) do {} while (0)
#endif

inline int memcpy_s(void *dest,
                    size_t numberOfElements,
                    const void *src,
                    size_t count) {

  if (numberOfElements<count)
    return -1;
  memcpy(dest, src, count);
  return 0;
}

inline void print_hex(unsigned char *mem, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
    printf("%#02x, ", *(mem+i));
  }
}

inline void PRINT_BYTE_ARRAY(void *file, void *mem, uint32_t len)
{
  (void) file;

  if(!mem || !len) {
    printf("\n( null )\n");
    return;
  }
  uint8_t *array = (uint8_t *)mem;
  printf("%u bytes:\n{\n", len);
  uint32_t i = 0;
  for(i = 0; i < len - 1; i++) {
    printf("0x%x, ", array[i]);
    if(i % 8 == 7)
      printf("\n");
  }
  printf("0x%x ", array[i]);
  printf("\n}\n");
}

#define SGX_AESGCM_IV_SIZE              12
#define SGX_AESGCM_KEY_SIZE             16
#define SGX_AESGCM_MAC_SIZE             16

typedef uint8_t sgx_aes_gcm_128bit_tag_t[SGX_AESGCM_MAC_SIZE];


#define OE_SHA256_HASH_SIZE 32
#define OE_HMAC_SIZE 32
#define OE_PUBLIC_KEY_SIZE 512
#define OE_SHARED_KEY_CIPHERTEXT_SIZE 256

typedef struct oe_report_msg_t {
  uint8_t public_key[OE_PUBLIC_KEY_SIZE];
  size_t report_size;
  uint8_t report[];
} oe_report_msg_t;

typedef struct oe_shared_key_msg_t {
  uint8_t shared_key_ciphertext[OE_SHARED_KEY_CIPHERTEXT_SIZE];
} oe_shared_key_msg_t;

#endif // COMMON_H
