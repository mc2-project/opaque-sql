//#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <sgx_tcrypto.h>

#ifndef DEFINE_H
#define DEFINE_H

// defines an upper bound on the size of the aggregation value
// only the plaintext size
#define PARTIAL_AGG_UPPER_BOUND (128) // this only includes the partial aggregation
#define ROW_UPPER_BOUND (2048)
// distinct items, offset, sort attribute, aggregation attribute
#define AGG_UPPER_BOUND (4 + 4 + ROW_UPPER_BOUND + PARTIAL_AGG_UPPER_BOUND)
#define ENC_HEADER_SIZE (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE)

#define TYPE_SIZE (1)
#define LEN_SIZE (4)
#define HEADER_SIZE (TYPE_SIZE + LEN_SIZE)

#define TABLE_ID_SIZE (8)
#define JOIN_ROW_UPPER_BOUND (ROW_UPPER_BOUND + TABLE_ID_SIZE)
#define ENC_JOIN_ROW_UPPER_BOUND (ENC_HEADER_SIZE + ROW_UPPER_BOUND + TABLE_ID_SIZE)

enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2,
  FLOAT = 3,
  DATE = 4,
  URL_TYPE = 5,
  C_CODE = 6,
  L_CODE = 7,

  /* PARTIAL_AGG_INT = 5, */
  /* FINAL_AGG_INT = 6, */
  /* PARTIAL_AGG_FLOAT = 7, */
  /* FINAL_AGG_FLOAT = 8 */
};

#define INT_UPPER_BOUND (4)
#define STRING_UPPER_BOUND (512)
#define URL_UPPER_BOUND (100)
#define C_CODE_UPPER_BOUND (3)
#define L_CODE_UPPER_BOUND (6)

enum OPCODE {
  OP_BD2 = 10,
  OP_SORT_INTEGERS_TEST = 90,
  OP_SORT_COL1 = 2,
  OP_SORT_COL2 = 50,
  OP_SORT_COL4_IS_DUMMY_COL2 = 51,
  OP_GROUPBY_COL2_SUM_COL3_STEP1 = 1,
  OP_GROUPBY_COL2_SUM_COL3_STEP2 = 101,
  OP_JOIN_COL2 = 3,
  OP_FILTER_COL2_GT3 = 30,
  OP_FILTER_TEST = 91,
  OP_FILTER_COL4_NOT_DUMMY = 32,
};

#define HALF_MAX_SORT_BUFFER (2 * 1024 * 1024)
#define MAX_SORT_BUFFER (4 * 1024 * 1024)
#define MAX_ELEMENTS ((16 * 1024 * 1024) / JOIN_ROW_UPPER_BOUND)

#endif // DEFINE_H
