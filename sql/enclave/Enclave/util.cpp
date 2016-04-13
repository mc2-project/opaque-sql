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


int test_dummy(uint8_t *src, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
	if (*(src + i) != 0) {
	  return -1;
	}
  }
  return 0;
}


void find_attribute(uint8_t *row, uint32_t length, uint32_t num_cols,
					uint32_t attr_num,
					uint8_t **enc_value_ptr, uint32_t *enc_value_len) {

  uint8_t *enc_value_ptr_ = row;
  uint32_t enc_value_len_ = 0;

  for (uint32_t j = 0; j < num_cols; j++) {
	// [enc len]encrypted{[value type][value len][value]}

	enc_value_len_ = *( (uint32_t *) enc_value_ptr_);
	enc_value_ptr_ += 4;
	  
	// found aggregate attribute
	if (j + 1 == attr_num) {
	  *enc_value_ptr = enc_value_ptr_;
	  *enc_value_len = enc_value_len_;
	  return;
	}
	
	enc_value_ptr_ += enc_value_len_;

  }
}

// return the upper bound size for a certain type
uint32_t get_value_bound(int type) {
  if (type == INT) {
	return INT_UPPER_BOUND;
  } else if (type == STRING) {
	return STRING_UPPER_BOUND;
  } else {
	return 0;
  }
}


void get_table_indicator(uint8_t *primary_table,
						 uint8_t *foreign_table) {
  char primary_table_[TABLE_ID_SIZE+1] = "11111111";
  char foreign_table_[TABLE_ID_SIZE+1] = "22222222";
  
  cpy(primary_table, (uint8_t *) primary_table_, TABLE_ID_SIZE);
  cpy(foreign_table, (uint8_t *) foreign_table_, TABLE_ID_SIZE);
}

// should be decrypted attribute
void print_attribute(const char *attr_name, uint8_t *value_ptr) {
  uint8_t attr_type = *value_ptr;
  uint32_t attr_len = *( (uint32_t *) (value_ptr + 1));
  printf("[%s: type is %u, attr_len is %u; ", attr_name, attr_type, attr_len);
  if (attr_type == 1) {
	printf("Attr: %u]\n", *( (uint32_t *) (value_ptr + 1 + 4)));
  } else if (attr_type == 2) {
	printf("Attr: %.*s]\n", attr_len, (char *) (value_ptr + 1 + 4));
  }
}

// this function prints out a plaintext row
void print_row(const char *row_name, uint8_t *row_ptr) {
  uint32_t num_cols = *( (uint32_t *) row_ptr);
  uint8_t *value_ptr = row_ptr + 4;
  printf("===============\n");
  printf("Row %s\n", row_name);
  for (uint32_t i = 0; i < num_cols; i++) {
	print_attribute("", value_ptr);
	value_ptr += *( (uint32_t *) (value_ptr + TYPE_SIZE)) + HEADER_SIZE;
  }
  printf("===============\n");
}

// this function prints out a plaintext join row
void print_join_row(const char *row_name, uint8_t *row_ptr) {
  uint8_t *table_id = row_ptr;
  printf("===============\n");
  printf("Table id: ");
  print_bytes(table_id, TABLE_ID_SIZE);
  
  uint32_t num_cols = *( (uint32_t *) (row_ptr + TABLE_ID_SIZE));
  uint8_t *value_ptr = row_ptr + TABLE_ID_SIZE + 4;
  
  printf("Row %s\n", row_name);
  for (uint32_t i = 0; i < num_cols; i++) {
	print_attribute("", value_ptr);
	value_ptr += *( (uint32_t *) (value_ptr + TYPE_SIZE)) + HEADER_SIZE;
  }
  printf("===============\n");  
}


// returns the number of attributes for this row
uint32_t get_num_col(uint8_t *row) {
  uint32_t *num_col_ptr = (uint32_t *) row;
  return *num_col_ptr;
}

uint8_t *get_enc_attr(uint8_t **enc_attr_ptr, uint32_t *enc_attr_len, 
					  uint8_t *row_ptr, uint8_t *row, uint32_t length) {
  if (row_ptr >= row + length) {
    return NULL;
  }

  uint8_t *ret_row_ptr = row_ptr;

  *enc_attr_ptr = row_ptr + 4;
  *enc_attr_len = * ((uint32_t *) row_ptr);

  ret_row_ptr += 4 + *enc_attr_len;
  return ret_row_ptr;
}


void get_attr(uint8_t *dec_attr_ptr,
			  uint8_t *type, uint32_t *attr_len, uint8_t **attr_ptr) {

  // given a pointer to the encrypted attribute, return the type, attr len, attr pointer
  assert(dec_attr_ptr != NULL);
  *type = *dec_attr_ptr;

  uint32_t *attr_len_ptr = (uint32_t *) (dec_attr_ptr + 1);
  *attr_len = *(dec_attr_ptr + 1);
  
  *attr_ptr  = (dec_attr_ptr + 1 + *attr_len);
}
