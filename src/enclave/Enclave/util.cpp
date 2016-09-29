#include "util.h"

#include <math.h>
#include <limits.h>

bool is_dummy_type(uint8_t attr_type) {
  // The high bit of an attribute type is 1 if it represents a dummy
  return (attr_type & 0x80) != 0;
}

uint8_t get_dummy_type(uint8_t attr_type) {
  return attr_type | 0x80;
}

uint32_t attr_upper_bound(uint8_t attr_type) {
  switch (attr_type & ~0x80) {
  case INT:
    return INT_UPPER_BOUND;

  case FLOAT:
    return FLOAT_UPPER_BOUND;

  case STRING:
    return STRING_UPPER_BOUND;

  case DATE:
  case LONG:
    return LONG_UPPER_BOUND;

  case DOUBLE:
    return DOUBLE_UPPER_BOUND;

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

  case TPCH_NATION_NAME_TYPE:
    return TPCH_NATION_NAME_UPPER_BOUND;

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


/// From http://git.musl-libc.org/cgit/musl/tree/src/time/__secs_to_tm.c?h=v0.9.15
/* 2000-03-01 (mod 400 year, immediately after feb29 */
#define LEAPOCH (946684800LL + 86400*(31+29))
#define DAYS_PER_400Y (365*400 + 97)
#define DAYS_PER_100Y (365*100 + 24)
#define DAYS_PER_4Y   (365*4   + 1)
int secs_to_tm(long long t, struct tm *tm) {
  long long days, secs;
  int remdays, remsecs, remyears;
  int qc_cycles, c_cycles, q_cycles;
  int years, months;
  int wday, yday, leap;
  static const char days_in_month[] = {31,30,31,30,31,31,30,31,30,31,31,29};

  /* Reject time_t values whose year would overflow int */
  if (t < INT_MIN * 31622400LL || t > INT_MAX * 31622400LL)
    return -1;

  secs = t - LEAPOCH;
  days = secs / 86400;
  remsecs = secs % 86400;
  if (remsecs < 0) {
    remsecs += 86400;
    days--;
  }

  wday = (3+days)%7;
  if (wday < 0) wday += 7;

  qc_cycles = days / DAYS_PER_400Y;
  remdays = days % DAYS_PER_400Y;
  if (remdays < 0) {
    remdays += DAYS_PER_400Y;
    qc_cycles--;
  }

  c_cycles = remdays / DAYS_PER_100Y;
  if (c_cycles == 4) c_cycles--;
  remdays -= c_cycles * DAYS_PER_100Y;

  q_cycles = remdays / DAYS_PER_4Y;
  if (q_cycles == 25) q_cycles--;
  remdays -= q_cycles * DAYS_PER_4Y;

  remyears = remdays / 365;
  if (remyears == 4) remyears--;
  remdays -= remyears * 365;

  leap = !remyears && (q_cycles || !c_cycles);
  yday = remdays + 31 + 28 + leap;
  if (yday >= 365+leap) yday -= 365+leap;

  years = remyears + 4*q_cycles + 100*c_cycles + 400*qc_cycles;

  for (months=0; days_in_month[months] <= remdays; months++)
    remdays -= days_in_month[months];

  if (years+100 > INT_MAX || years+100 < INT_MIN)
    return -1;

  tm->tm_year = years + 100;
  tm->tm_mon = months + 2;
  if (tm->tm_mon >= 12) {
    tm->tm_mon -=12;
    tm->tm_year++;
  }
  tm->tm_mday = remdays + 1;
  tm->tm_wday = wday;
  tm->tm_yday = yday;

  tm->tm_hour = remsecs / 3600;
  tm->tm_min = remsecs / 60 % 60;
  tm->tm_sec = remsecs % 60;

  return 0;
}
