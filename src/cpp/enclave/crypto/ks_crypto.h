#include "crypto.h"
#include "sgxaes.h"

extern unsigned char shared_key[CIPHER_KEY_SIZE];

void set_shared_key(uint8_t *msg4, uint32_t msg4_size);
