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
#include "common.h"

bool is_dummy_type(uint8_t attr_type);
uint8_t get_dummy_type(uint8_t attr_type);
uint32_t attr_upper_bound(uint8_t attr);

/*
 * printf:
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

void printf(const char *fmt, ...);

void print_bytes(uint8_t *ptr, uint32_t len);

void get_next_value(uint8_t **ptr, uint8_t **enc_value_ptr, uint32_t *enc_value_len);
void get_next_row(uint8_t **ptr, uint8_t **enc_row_ptr, uint32_t *enc_row_len);
void get_next_plaintext_row(uint8_t **ptr, uint8_t **row_ptr, uint32_t *row_len);

// cmp should return 0 if equal, and -1 if not equal
int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len);
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);
void clear(uint8_t *dest, uint32_t len);

void write_dummy(uint8_t *dest, uint32_t len);
int test_dummy(uint8_t *src, uint32_t len);

void find_attribute(uint8_t *row, uint32_t length, uint32_t num_cols,
                    uint32_t attr_num,
                    uint8_t **enc_value_ptr, uint32_t *enc_value_len);

int is_table_primary(uint8_t *table);

void print_attribute(const char *attr_name, uint8_t *value_ptr);
void print_attributes(const char *attr_name, uint8_t *ptr, uint32_t num_attributes);
void print_row(const char *row_name, uint8_t *row_ptr);
void print_row(const char *row_name, uint8_t *row_ptr, uint32_t num_cols);
void print_join_row(const char *row_name, uint8_t *row_ptr);

uint32_t get_num_col(uint8_t *row);
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
void encrypt_attribute(uint8_t **input, uint8_t **output);
void decrypt_attribute(uint8_t **input, uint8_t **output);

void check(const char* message, bool test);

class BufferReader {
public:
  BufferReader();

  void add_buffer(uint8_t *ptr, uint32_t size);
  void reset();
  void clear();
  uint8_t *get_ptr();
  void inc_ptr(uint8_t *ptr);

  uint8_t *buffer_list[10];
  uint32_t buffer_sizes[10];

  int offset;

  uint32_t current_buf;
  uint8_t *current_pointer;
};

uint32_t get_plaintext_padded_row_size(uint8_t *row);

#endif // UTIL_H
