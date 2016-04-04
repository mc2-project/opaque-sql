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
#endif // UTIL_H
