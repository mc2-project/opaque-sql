#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "define.h"

#ifndef INTERNAL_TYPES_H
#define INTERNAL_TYPES_H

enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2
};


typedef struct Integer {
  int value;
} Integer;

typedef struct String {
  uint32_t length;
  uint8_t *buffer;
} String;


typedef struct Record {
  uint32_t num_cols;
  // data in the format of [attr1][attr2] ...
  // Attribute's format is [type][size][data]
  uint8_t *data;
  // this is the attribute used for sort-based comparison/join
  // pointer is to the start of "type"
  uint8_t *sort_attribute;
} Record;

typedef struct JoinRecord {
  int if_primary;
  Record record;
} JoinRecord;

template<typename T>
int compare(T *value1, T *value2);

template<typename T>
void compare_and_swap(T *value1, T* value2);

template<typename T>
void swap(T *value1, T* value2);

#endif
