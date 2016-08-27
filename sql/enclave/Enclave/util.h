#ifndef UTIL_H
#define UTIL_H

#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <time.h>

#include "Enclave_t.h"  /* print_string */
#include "define.h"
#include "Crypto.h"
#include "common.h"

bool is_dummy_type(uint8_t attr_type);
uint8_t get_dummy_type(uint8_t attr_type);
uint32_t attr_upper_bound(uint8_t attr);

/*
 * printf:
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

int printf(const char *fmt, ...);

void print_bytes(uint8_t *ptr, uint32_t len);

// cmp should return 0 if equal, and -1 if not equal
int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len);
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);
void clear(uint8_t *dest, uint32_t len);

void write_dummy(uint8_t *dest, uint32_t len);
int test_dummy(const uint8_t *src, uint32_t len);

// returns the offset for output to advance
void encrypt_attribute(uint8_t **input, uint8_t **output);
void decrypt_attribute(uint8_t **input, uint8_t **output);

int log_2(int value);

int pow_2(int value);

int secs_to_tm(long long t, struct tm *tm);

#endif // UTIL_H
