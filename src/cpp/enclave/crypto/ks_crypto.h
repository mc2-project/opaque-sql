#include "crypto.h"
#include "sgxaes.h"

extern unsigned char shared_key[CIPHER_KEY_SIZE];

void set_shared_key(const uint8_t *shared_key_bytes, uint32_t shared_key_size);
