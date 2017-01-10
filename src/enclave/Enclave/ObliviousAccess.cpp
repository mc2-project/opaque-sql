#include "ObliviousAccess.h"

void byte_access(uint8_t *ptr) {
  asm volatile("mov %0, %%r8"
               :
               :"r" (ptr)
               :"%r8");
}

void dummy_access(uint8_t *ptr, uint32_t size) {

  uint32_t register_size = 8;
  uint32_t max_rounds = size / register_size;
  uint32_t leftover_bytes = size % register_size;

  uint8_t *output_ptr = ptr;
  (void)output_ptr;

  for (uint32_t i = 0; i < max_rounds; i++) {
    byte_access(output_ptr);
    output_ptr += register_size;
  }

  if (leftover_bytes > 0) {
    byte_access(output_ptr);
  }

}

void byte_swap(uint8_t *ptr1, uint8_t *ptr2,
               bool if_swap) {

  if (if_swap) {
    asm volatile("mov %0, %%r8\n\t"
                 "mov %1, %%r9\n\t"
                   "mov %%r9, %0\n\t"
                 "mov %%r8, %1\n\t"
                 :"+m" (*((uint64_t *) ptr1)), "+m" (*((uint64_t *) ptr2))
                 :
                 :"%r8", "%r9");
  } else {
    asm volatile("mov %0, %%r8\n\t"
                 "mov %1, %%r9\n\t"
                 "mov %%r8, %0\n\t"
                 "mov %%r9, %1\n\t"
                 :"+m" (*((uint64_t *) ptr1)), "+m" (*((uint64_t *) ptr2))
                 :
                 :"%r8", "%r9");
  }
}

void swap_memory(uint8_t *ptr1, uint8_t *ptr2, uint32_t size,
                 bool if_swap) {

  const uint32_t register_size = 8;
  uint32_t max_rounds = size / register_size;
  uint32_t leftover_bytes = size % register_size;
  (void)leftover_bytes;

  uint8_t *output_ptr1 = ptr1;
  uint8_t *output_ptr2 = ptr2;


  for (uint32_t i = 0; i < max_rounds; i++) {
    byte_swap(output_ptr1, output_ptr2, if_swap);
    output_ptr1 += register_size;
    output_ptr2 += register_size;
  }

  // pad the last few bytes...
  if (leftover_bytes > 0) {
    uint8_t padding1[register_size];
    uint8_t padding2[register_size];
    memset(padding1, '\0', register_size);
    memset(padding2, '\0', register_size);
    memcpy(padding1, output_ptr1, leftover_bytes);
    memcpy(padding2, output_ptr2, leftover_bytes);

    byte_swap(padding1, padding2, if_swap);

    memcpy(output_ptr1, padding1, leftover_bytes);
    memcpy(output_ptr2, padding2, leftover_bytes);
  }

}

// obliviously copies in[in_offset_start:in_offset_end] to out
void oblivious_get(uint8_t *in,
                   uint32_t in_size,
                   uint32_t in_offset_start,
                   uint32_t in_offset_end,
                   uint8_t *out,
                   uint32_t out_size) {
  for (uint32_t i = 0; i < in_size; i++) {
    if (i >= in_offset_start && i < in_offset_end) {
      for (uint32_t j = 0; j < out_size; j++) {
        if (j == i - in_offset_start) {
          *(out + j) = *(in + 1);
        } else {
          dummy_access(in + i, 1);
          dummy_access(out + j, 1);
        }
      }
    } else {
      for (uint32_t j = 0; j < out_size; j++) {
        dummy_access(in + i, 1);
        dummy_access(out + j, i);
      }
    }
  }
}

// obliviously copies out to in[in_offset_start:in_offset_end]
void oblivious_set(uint8_t *out,
                   uint32_t out_size,
                   uint8_t *in,
                   uint32_t in_size,
                   uint32_t in_offset_start,
                   uint32_t in_offset_end) {
  for (uint32_t i = 0; i < in_size; i++) {
    if (i >= in_offset_start && i < in_offset_end) {
      for (uint32_t j = 0; j < out_size; j++) {
        if (j == i - in_offset_start) {
          *(in + 1) = *(out + j);
        } else {
          dummy_access(in + i, 1);
          dummy_access(out + j, 1);
        }
      }
    } else {
      for (uint32_t j = 0; j < out_size; j++) {
        dummy_access(in + i, 1);
        dummy_access(out + j, i);
      }
    }
  }
}
