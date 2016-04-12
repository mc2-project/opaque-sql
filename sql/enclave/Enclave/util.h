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
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);
void clear(uint8_t *dest, uint32_t len);

void write_dummy(uint8_t *dest, uint32_t len);
int test_dummy(uint8_t *src, uint32_t len);

void find_attribute(uint8_t *row, uint32_t length, uint32_t num_cols,
					uint32_t attr_num,
					uint8_t **enc_value_ptr, uint32_t *enc_value_len);


// defines an upper bound on the size of the aggregation value
// only the plaintext size
#define PARTIAL_AGG_UPPER_BOUND (128) // this only includes the partial aggregation
#define ROW_UPPER_BOUND (2048)
// distinct items, offset, sort attribute, aggregation attribute
#define AGG_UPPER_BOUND (4 + 4 + ROW_UPPER_BOUND + PARTIAL_AGG_UPPER_BOUND)

enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2
};

#define TYPE_SIZE (1)
#define LEN_SIZE (4)
#define HEADER_SIZE (TYPE_SIZE + LEN_SIZE)

#define INT_UPPER_BOUND (4)
#define STRING_UPPER_BOUND (1024)


#define TABLE_ID_SIZE (8)
#define JOIN_ROW_UPPER_BOUND (ROW_UPPER_BOUND + TABLE_ID_SIZE)

#endif // UTIL_H
