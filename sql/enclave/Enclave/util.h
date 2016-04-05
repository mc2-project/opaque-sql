#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include "Enclave_t.h"  /* print_string */

#ifndef UTIL_H
#define UTIL_H

/* 
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

void printf(const char *fmt, ...);

void print_bytes(uint8_t *ptr, uint32_t len);

void get_next_value(uint8_t **ptr, uint8_t **enc_value_ptr, uint32_t *enc_value_len);
void get_next_row(uint8_t **ptr, uint8_t **enc_row_ptr, uint32_t *enc_row_len);

// cmp should return 0 if equal, and -1 if not equal
int cmp(uint8_t *value1, uint8_t *value2, uint32_t len);
#endif // UTIL_H
