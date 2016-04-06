#include "util.h"

void printf(const char *fmt, ...)
{
    char buf[BUFSIZ] = {'\0'};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_print_string(buf);
}

void print_bytes(uint8_t *ptr, uint32_t len) {
  
  for (int i = 0; i < len; i++) {
    printf("%u", *(ptr + i));
    printf(" - ");
  }

  printf("\n");
}



void get_next_value(uint8_t **ptr, uint8_t **enc_value_ptr, uint32_t *enc_value_len) {

  uint32_t *enc_value_len_ptr = (uint32_t *) *ptr;
  *enc_value_len = *enc_value_len_ptr;
  *enc_value_ptr = *ptr + 4;

  *ptr = *ptr + 4 + *enc_value_len;
  
}


// advance pointer to the next row
// return pointer to current row, as well as the overall length
void get_next_row(uint8_t **ptr, uint8_t **enc_row_ptr, uint32_t *enc_row_len) {
  // a row should be in the format of [num_col][enc_attr1 len][enc_attr1][enc_attr2 len][enc_attr2]...
  uint32_t num_cols = * ( (uint32_t *) *ptr);
  uint8_t *enc_attr_ptr = *ptr;
  uint32_t enc_attr_len = 0;
  uint32_t len = 0;

  // move past the column number
  enc_attr_ptr += 4;
  len = 4;

  for (uint32_t i = 0; i < num_cols; i++) {
	enc_attr_len = * ((uint32_t *) enc_attr_ptr);
	enc_attr_ptr += 4 + enc_attr_len;
	len += 4 + enc_attr_len;
  }

  *enc_row_ptr = *ptr;
  *ptr = enc_attr_ptr;
  *enc_row_len = len;
}


int cmp(uint8_t *value1, uint8_t *value2, uint32_t len) {

  for (uint32_t i = 0; i < len; i++) {
	if (*(value1+i) != *(value2+i)) {
	  return -1;
	}
  }
  return 0;
}

void cpy(uint8_t *dest, uint8_t *src, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
	*(dest + i) = *(src + i);
  }
}

// basically a memset 0
void clear(uint8_t *dest, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
	*(dest + i) = 0;
  }
}

void write_dummy(uint8_t *dest, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
	*(dest + i) = 0;
  }
}
