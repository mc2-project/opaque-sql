#include <algorithm>

#include "common.h"
#include "util.h"

#ifndef O_ACCESS
#define O_ACCESS

void dummy_access(uint8_t *ptr1, uint32_t size);
void swap_memory(uint8_t *ptr1, uint8_t *ptr2, uint32_t size, bool if_swap);

void oblivious_get(uint8_t *in,
                   uint32_t in_size,
                   uint32_t in_offset_start,
                   uint32_t in_offset_end,
                   uint8_t *out,
                   uint32_t out_size);

void oblivious_set(uint8_t *out,
                   uint32_t out_size,
                   uint8_t *in,
                   uint32_t in_size,
                   uint32_t in_offset_start,
                   uint32_t in_offset_end);
#endif
