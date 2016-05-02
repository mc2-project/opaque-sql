#ifndef UTIL_H
#define UTIL_H

#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "Enclave_t.h"  /* print_string */
#include "define.h"
#include "InternalTypes.h"
#include "Crypto.h"

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

void get_table_indicator(uint8_t *primary_table,
						 uint8_t *foreign_table);
int is_table_primary(uint8_t *table);

void print_attribute(const char *attr_name, uint8_t *value_ptr);
void print_row(const char *row_name, uint8_t *row_ptr);
void print_join_row(const char *row_name, uint8_t *row_ptr);

uint32_t get_num_col(uint8_t *row);
uint8_t *get_enc_attr(uint8_t **enc_attr_ptr, uint32_t *enc_attr_len, 
					  uint8_t *row_ptr, uint8_t *row, uint32_t length);
void get_attr(uint8_t *dec_attr_ptr,
			  uint8_t *type, uint32_t *attr_len, uint8_t **attr_ptr);

void find_plaintext_attribute(uint8_t *row, uint32_t num_cols,
							  uint32_t attr_num,
							  uint8_t **value_ptr,
							  uint32_t *len);

template <typename T> void swap_helper(T *v1, T *v2) {
  T temp = *v1;
  *v1 = *v2;
  *v2 = temp;
}


// returns the offset for output to advance
void encrypt_attribute(uint8_t **input, uint8_t **output, uint8_t real_type = DUMMY);
void decrypt_attribute(uint8_t **input, uint8_t **output);

void check(const char* message, bool test);

#endif // UTIL_H
