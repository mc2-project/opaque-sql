#include "util.h"

#include <math.h>

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
    return false;
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
    return attr_type;
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
    return 0;
  }
}

int printf(const char *fmt, ...)
{
  char buf[BUFSIZ] = {'\0'};
  va_list ap;
  va_start(ap, fmt);
  int ret = vsnprintf(buf, BUFSIZ, fmt, ap);
  va_end(ap);
  ocall_print_string(buf);
  return ret;
}

void print_bytes(uint8_t *ptr, uint32_t len) {

  for (uint32_t i = 0; i < len; i++) {
    printf("%u", *(ptr + i));
    printf(" - ");
  }

  printf("\n");
}

int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len) {

  for (uint32_t i = 0; i < len; i++) {
    if (*(value1+i) != *(value2+i)) {
      return -1;
    }
  }
  return 0;
}

void cpy(uint8_t *dest, uint8_t *src, uint32_t len) {
  // for (uint32_t i = 0; i < len; i++) {
  //     *(dest + i) = *(src + i);
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


int test_dummy(const uint8_t *src, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
    if (*(src + i) != 0) {
      return -1;
    }
  }
  return 0;
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

  uint32_t attr_len = *( (uint32_t *) (temp + TYPE_SIZE));

  cpy(output_ptr, temp, HEADER_SIZE + attr_len);
  input_ptr += enc_len;
  output_ptr += HEADER_SIZE + attr_len;

  *input = input_ptr;
  *output = output_ptr;
}

int log_2(int value) {
  double dvalue = (double) value;
  int log_value = (int) ceil(log(dvalue) / log(2.0));
  return log_value;
}

int pow_2(int value) {
  double dvalue = (double) value;
  int pow_value = (int) pow(2, dvalue);
  return pow_value;
}
