#include "util.h"

bool is_dummy_type(uint8_t attr_type) {
  switch(attr_type) {
  case DUMMY:
  case INT:
  case STRING:
  case FLOAT:
  case DATE:
  case URL_TYPE:
  case C_CODE:
  case L_CODE:
  case LONG:
  case IP_TYPE:
  case USER_AGENT_TYPE:
  case SEARCH_WORD_TYPE:
    return false;

  case DUMMY_INT:
  case DUMMY_FLOAT:
  case DUMMY_STRING:
    return true;

  default:
    printf("is_dummy_type: Unknown type %d\n", attr_type);
    assert(false);
  }
}

uint8_t get_dummy_type(uint8_t attr_type) {
  switch(attr_type) {
  case INT:
    return DUMMY_INT;

  case FLOAT:
    return DUMMY_FLOAT;

  case STRING:
    return DUMMY_STRING;

  default:
    printf("get_dummy_type: Unknown type %d\n", attr_type);
    assert(false);
  }
}

uint32_t attr_upper_bound(uint8_t attr_type) {
  switch(attr_type) {
  case INT:
  case DUMMY_INT:
  case FLOAT:
  case DUMMY_FLOAT:
    return INT_UPPER_BOUND;

  case STRING:
  case DUMMY_STRING:
    return STRING_UPPER_BOUND;

  case DATE:
  case LONG:
    return LONG_UPPER_BOUND;

  case URL_TYPE:
    return URL_UPPER_BOUND;

  case C_CODE:
    return C_CODE_UPPER_BOUND;

  case L_CODE:
    return L_CODE_UPPER_BOUND;

  case IP_TYPE:
    return IP_UPPER_BOUND;

  case USER_AGENT_TYPE:
    return USER_AGENT_UPPER_BOUND;

  case SEARCH_WORD_TYPE:
    return SEARCH_WORD_UPPER_BOUND;

  default:
    printf("attr_upper_bound: Unknown type %d\n", attr_type);
    assert(false);
  }
}

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

// advance pointer to the next row
// return pointer to current row, as well as the overall length
void get_next_plaintext_row(uint8_t **ptr, uint8_t **row_ptr, uint32_t *row_len) {
  // a row should be in the format of [num_col][attr1 type][attr1 len][attr1]...
  uint32_t num_cols = * ( (uint32_t *) *ptr);
  uint8_t *attr_ptr = *ptr;
  uint32_t attr_len = 0;
  uint32_t len = 0;

  // move past the column number
  attr_ptr += 4;
  len = 4;

  for (uint32_t i = 0; i < num_cols; i++) {
	attr_len = * ((uint32_t *) (attr_ptr + TYPE_SIZE));
	attr_ptr += HEADER_SIZE + attr_len;
	len += HEADER_SIZE + attr_len;
  }

  *row_ptr = *ptr;
  *ptr = attr_ptr;
  *row_len = len;
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
  // for (uint32_t i = 0; i < len; i++) {
  // 	*(dest + i) = *(src + i);
  // }

  memcpy(dest, src, len);
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

// assume that the entire row has been decrypted
void find_plaintext_attribute(uint8_t *row, uint32_t num_cols,
							  uint32_t attr_num,
							  uint8_t **value_ptr,
							  uint32_t *len) {
  
  uint8_t *value_ptr_ = row;
  uint32_t value_len_ = 0;

  for (uint32_t j = 0; j < num_cols; j++) {
	// [value type][value len][value]

	value_len_ = *( (uint32_t *) (value_ptr_ + TYPE_SIZE));
	  
	// found aggregate attribute
	if (j + 1 == attr_num) {
	  *value_ptr = value_ptr_;
	  *len = value_len_ + HEADER_SIZE;
	  return;
	}
	
	value_ptr_ += value_len_ + HEADER_SIZE;

  }
  
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

void get_table_indicator(uint8_t *primary_table,
						 uint8_t *foreign_table) {
  char primary_table_[TABLE_ID_SIZE+1] = "11111111";
  char foreign_table_[TABLE_ID_SIZE+1] = "22222222";
  
  cpy(primary_table, (uint8_t *) primary_table_, TABLE_ID_SIZE);
  cpy(foreign_table, (uint8_t *) foreign_table_, TABLE_ID_SIZE);
}

int is_table_primary(uint8_t *table_id) {
  char primary_table_[TABLE_ID_SIZE+1] = "11111111";

  return cmp(table_id, (uint8_t *) primary_table_, TABLE_ID_SIZE);
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


void print_attributes(const char *attr_name, uint8_t *ptr, uint32_t num_attributes) {
  uint8_t *value_ptr = ptr;
  for (uint32_t i = 0; i < num_attributes; i++) {

    uint8_t attr_type = *value_ptr;
    uint32_t attr_len = *( (uint32_t *) (value_ptr + TYPE_SIZE));
    printf("[%s: type is %u, attr_len is %u; ", attr_name, attr_type, attr_len);
    if (attr_type == 1) {
      printf("Attr: %u]\n", *( (uint32_t *) (value_ptr + 1 + 4)));
    } else if (attr_type == 2) {
      printf("Attr: %.*s]\n", attr_len, (char *) (value_ptr + 1 + 4));
    }

    value_ptr += HEADER_SIZE + attr_len;

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

// this function prints out a plaintext row
void print_row(const char *row_name, uint8_t *row_ptr, uint32_t num_cols) {
  uint8_t *value_ptr = row_ptr;
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


void encrypt_attribute(uint8_t **input, uint8_t **output) {
  uint8_t *input_ptr = *input;
  uint8_t *output_ptr = *output;
  
  uint8_t attr_type = *input_ptr;
  uint32_t attr_len = 0;
  
  uint8_t temp[HEADER_SIZE + ATTRIBUTE_UPPER_BOUND];

  uint32_t upper_bound = attr_upper_bound(attr_type);

  *( (uint32_t *) output_ptr) = enc_size(HEADER_SIZE + upper_bound);
  output_ptr += 4;
  attr_len = *( (uint32_t *) (input_ptr + TYPE_SIZE));
  cpy(temp, input_ptr, HEADER_SIZE + attr_len);
  encrypt(temp, HEADER_SIZE + upper_bound, output_ptr);

  input_ptr += HEADER_SIZE + attr_len;
  output_ptr += enc_size(HEADER_SIZE + upper_bound);

  *input = input_ptr;
  *output = output_ptr;
}


void decrypt_attribute(uint8_t **input, uint8_t **output) {
  uint8_t *input_ptr = *input;
  uint8_t *output_ptr = *output;

  uint32_t enc_len = *( (uint32_t *) (input_ptr));
  //printf("[decrypt_attribute] enc_len is %u\n", enc_len);
  
  input_ptr += 4;
  
  uint8_t temp[HEADER_SIZE + ATTRIBUTE_UPPER_BOUND];

  decrypt(input_ptr, enc_len, temp);
  //printf("[decrypt_attribute] enc_len is %u, type is %u\n", enc_len, *temp);

  uint8_t attr_type = *temp;
  uint32_t attr_len = *( (uint32_t *) (temp + TYPE_SIZE));

  cpy(output_ptr, temp, HEADER_SIZE + attr_len);
  input_ptr += enc_len;
  output_ptr += HEADER_SIZE + attr_len;

  *input = input_ptr;
  *output = output_ptr;
}

void check(const char* message, bool test) {
  if (!test) {
    printf("%s\n", message);
    assert(test);
  }
}

// Row reader is able to piece together multiple buffer locations
// and return rows; it assumes that no row is split across boundaries!
BufferReader::BufferReader() {
  offset = 0;
  for (uint32_t i = 0; i < 10; i++) {
	buffer_list[i] = NULL;
	buffer_sizes[i] = 0;
  }

  current_pointer = NULL;
  current_buf = 0;
}
  
void BufferReader::add_buffer(uint8_t *ptr, uint32_t size) {
  buffer_list[offset] = ptr;
  buffer_sizes[offset] = size;

  ++offset;
}

void BufferReader::reset() {
  // reset pointer
  current_pointer = buffer_list[0];
  current_buf = 0;
}

void BufferReader::clear() {
  offset = 0;
  for (uint32_t i = 0; i < 10; i++) {
	buffer_list[i] = NULL;
	buffer_sizes[i] = 0;
  }
  current_pointer = NULL;
  current_buf = 0;
}


uint8_t *BufferReader::get_ptr() {
  return current_pointer;
}

void BufferReader::inc_ptr(uint8_t *ptr) {
  uint32_t inc_size = ptr - current_pointer;
  if ((current_pointer + inc_size) >= buffer_list[current_buf] + buffer_sizes[current_buf]) {
	++current_buf;
	current_pointer = buffer_list[current_buf];
	//printf("[BufferReader] jump pointer, %p\n", current_pointer);
	// if (current_pointer != NULL) {
	//   printf("First 4 bytes: %u\n", *( (uint32_t *) (current_pointer)));
	//   printf("First 4 bytes: %u\n", *( (uint32_t *) (current_pointer + 4)));
	// }
  } else {
	current_pointer += inc_size;
  }
}

uint32_t get_plaintext_padded_row_size(uint8_t *row) {
  uint8_t *row_ptr = row;
  uint32_t len = 0;
  uint32_t num_cols = *( (uint32_t *) row_ptr);
  row_ptr += 4;

  uint32_t enc_attr_len = 0;
  
  for (uint32_t i = 0; i < num_cols; i++) {
    enc_attr_len = *( (uint32_t *) row_ptr);
    len += enc_attr_len - ENC_HEADER_SIZE;
    row_ptr += 4 + enc_attr_len;
  }
  
  return len;
}
