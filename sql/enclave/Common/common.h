#include <assert.h>

#include "define.h"

int printf( const char* format, ... );

#ifndef COMMON_H
#define COMMON_H

// Make sure to update the functions in common.cpp and util.cpp when you add a
// type here.
enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2,
  FLOAT = 3,
  DATE = 4,
  URL_TYPE = 5,
  C_CODE = 6,
  L_CODE = 7,
  LONG = 8,
  IP_TYPE = 9,
  USER_AGENT_TYPE = 10,
  SEARCH_WORD_TYPE = 11,

  DUMMY_INT = 100,
  DUMMY_FLOAT = 101,
  DUMMY_STRING = 102,

  /* PARTIAL_AGG_INT = 5, */
  /* FINAL_AGG_INT = 6, */
  /* PARTIAL_AGG_FLOAT = 7, */
  /* FINAL_AGG_FLOAT = 8 */
};

// Make sure to update the functions in common.cpp when you add an opcode here.
enum OPCODE {
  OP_BD1 = 11,
  OP_BD2 = 10,
  OP_SORT_INTEGERS_TEST = 90,
  OP_SORT_COL1 = 2,
  OP_SORT_COL2 = 50,
  OP_SORT_COL2_IS_DUMMY_COL1 = 53,
  OP_SORT_COL3_IS_DUMMY_COL1 = 52,
  OP_SORT_COL4_IS_DUMMY_COL2 = 51,
  OP_GROUPBY_COL1_SUM_COL2_INT_STEP1 = 102,
  OP_GROUPBY_COL1_SUM_COL2_INT_STEP2 = 103,
  OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1 = 107,
  OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2 = 108,
  OP_GROUPBY_COL2_SUM_COL3_INT_STEP1 = 1,
  OP_GROUPBY_COL2_SUM_COL3_INT_STEP2 = 101,
  OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1 = 104,
  OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP2 = 105,
  OP_JOIN_COL1 = 106,
  OP_JOIN_COL2 = 3,
  OP_FILTER_COL2_GT3 = 30,
  OP_FILTER_NOT_DUMMY = 32,
  OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01 = 34,
  OP_PROJECT_PAGERANK_WEIGHT_RANK = 35,
  OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK = 36,
  OP_PROJECT_ADD_RANDOM_ID = 39,
  OP_PROJECT_DROP_COL1 = 40,
  OP_PROJECT_SWAP_COL1_COL2 = 41,
  OP_PROJECT_SWAP_COL2_COL3 = 42,
  OP_JOIN_PAGERANK = 37,
};

inline static int get_sort_operation(int op_code) {
  switch(op_code) {

  case OP_SORT_COL1:
  case OP_SORT_COL2:
  case OP_SORT_COL2_IS_DUMMY_COL1:
  case OP_SORT_COL3_IS_DUMMY_COL1:
  case OP_SORT_COL4_IS_DUMMY_COL2:
    return SORT_SORT;
    break;

  case OP_JOIN_COL1:
  case OP_JOIN_COL2:
  case OP_JOIN_PAGERANK:
    return SORT_JOIN;
    break;

  default:
    return -1;
  }
}

#ifdef DEBUG
#define debug(...) printf(__VA_ARGS__)
#else
#define debug(...) do {} while (0)
#endif

#ifdef PERF
#define perf(...) printf(__VA_ARGS__)
#else
#define perf(...) do {} while (0)
#endif

#define check(test, ...) do {                   \
    bool result = test;                         \
    if (!result) {                              \
      printf(__VA_ARGS__);                      \
      assert(result);                           \
    }                                           \
  } while (0)

class BlockReader {
public:
  BlockReader(uint8_t *input, uint32_t input_len)
    : input_start(input), input(input), input_len(input_len) {}
  void read(uint8_t **block_out, uint32_t *len_out, uint32_t *num_rows_out) {
    if (input >= input_start + input_len) {
      *block_out = NULL;
    } else {
      *block_out = input;
      uint32_t block_enc_size = *reinterpret_cast<uint32_t *>(input); input += 4;
      *len_out = block_enc_size; *len_out += 4;
      *num_rows_out = *reinterpret_cast<uint32_t *>(input); input += 4; *len_out += 4;
      input += 4; *len_out += 4; // row_upper_bound
      input += block_enc_size;
    }
  }
  uint32_t get_row_upper_bound() {
    check(input < input_start + input_len, "BlockReader: input exhausted");
    return *reinterpret_cast<uint32_t *>(input + 8);
  }

private:
  uint8_t * const input_start;
  uint8_t *input;
  const uint32_t input_len;
};

inline static uint32_t block_size_upper_bound(uint32_t num_rows) {
  uint32_t max_row_len = ROW_UPPER_BOUND;
  uint32_t max_rows_per_block = (MAX_BLOCK_SIZE - 28) / max_row_len; // 28 = ENC_HEADER_SIZE
  uint32_t max_num_blocks = num_rows / max_rows_per_block;
  if (num_rows % max_rows_per_block != 0) {
    max_num_blocks++;
  }
  if (max_num_blocks == 0) {
    max_num_blocks = 1;
  }
  return max_num_blocks * (BLOCK_HEADER_SIZE + 28 + MAX_BLOCK_SIZE);
}

#endif
