#include <assert.h>

#include "define.h"

int printf( const char* format, ... );

#ifndef COMMON_H
#define COMMON_H

enum DATA_GEN_TYPE : uint8_t {
  DATA_GEN_REGULAR,
    DATA_GEN_AGG,
    DATA_GEN_JOIN_P,
    DATA_GEN_JOIN_F
};

// Make sure to update the functions in common.cpp and util.cpp when you add a
// type here.
enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2,
  FLOAT = 3,
  DOUBLE = 14,
  DATE = 4,
  URL_TYPE = 5,
  C_CODE = 6,
  L_CODE = 7,
  LONG = 8,
  IP_TYPE = 9,
  USER_AGENT_TYPE = 10,
  SEARCH_WORD_TYPE = 11,
  TPCH_NATION_NAME_TYPE = 12,

  DUMMY_INT = 100,
  DUMMY_FLOAT = 101,
  DUMMY_STRING = 102,
  DUMMY_TPCH_NATION_NAME_TYPE = 112,

  /* PARTIAL_AGG_INT = 5, */
  /* FINAL_AGG_INT = 6, */
  /* PARTIAL_AGG_FLOAT = 7, */
  /* FINAL_AGG_FLOAT = 8 */
};

// Make sure to update the functions below when you add an opcode here.
enum OPCODE {
  OP_BD1_FILTER = 11,
  OP_BD2 = 10,  
  OP_SORT_INTEGERS_TEST = 90,
  OP_SORT_COL1 = 2,
  OP_SORT_COL2 = 50,
  OP_SORT_COL1_COL2 = 54,
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
  OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP1 = 109,
  OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP2 = 110,
  OP_JOIN_COL1 = 106,
  OP_JOIN_COL2 = 3,
  OP_JOIN_PAGERANK = 37,
  OP_JOIN_TPCH9GENERIC_NATION = 111,
  OP_JOIN_TPCH9GENERIC_SUPPLIER = 112,
  OP_JOIN_TPCH9GENERIC_ORDERS = 113,
  OP_JOIN_TPCH9GENERIC_PARTSUPP = 114,
  OP_JOIN_TPCH9GENERIC_PART_LINEITEM = 115,
  OP_JOIN_TPCH9OPAQUE_NATION = 116,
  OP_JOIN_TPCH9OPAQUE_SUPPLIER = 117,
  OP_JOIN_TPCH9OPAQUE_ORDERS = 118,
  OP_JOIN_TPCH9OPAQUE_LINEITEM = 119,
  OP_JOIN_TPCH9OPAQUE_PART_PARTSUPP = 120,
  OP_JOIN_DISEASEDEFAULT_TREATMENT = 121,
  OP_JOIN_DISEASEDEFAULT_PATIENT = 122,
  OP_JOIN_DISEASEOPAQUE_PATIENT = 123,
  OP_JOIN_DISEASEOPAQUE_TREATMENT = 124,
  OP_JOIN_GENEDEFAULT_GENE = 125,
  OP_JOIN_GENEOPAQUE_GENE = 126,
  OP_JOIN_GENEOPAQUE_PATIENT = 127,
  OP_FILTER_COL2_GT3 = 30,
  OP_FILTER_COL4_GT_25 = 47,
  OP_FILTER_COL4_GT_40 = 49,
  OP_FILTER_COL4_GT_45 = 48,
  OP_FILTER_NOT_DUMMY = 32,
  OP_BD2_FILTER_NOT_DUMMY = 1032,
  OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01 = 34,
  OP_FILTER_COL2_CONTAINS_MAROON = 38,
  OP_PROJECT_PAGERANK_WEIGHT_RANK = 35,
  OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK = 36,
  OP_PROJECT_TPCH9GENERIC = 43,
  OP_PROJECT_TPCH9OPAQUE = 45,
  OP_PROJECT_TPCH9_ORDER_YEAR = 44,
  OP_PROJECT_ADD_RANDOM_ID = 39,
  OP_PROJECT_DROP_COL1 = 40,
  OP_PROJECT_DROP_COL2 = 46,
  OP_PROJECT_SWAP_COL1_COL2 = 41,
  OP_PROJECT_SWAP_COL2_COL3 = 42,

  OP_GROUPBY_COL1_SUM_COL2_INT = 300,

  OP_GROUPBY_COL1_SUM_COL2_FLOAT = 301,
  OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT = 302,

  OP_BD1_SORT_SCAN = 10000,
  OP_BD1_SORT_PREPROCESS = 11000,
  OP_BD1_SORT_PAD = 12000,
  OP_BD1_SORT_ROUND1 = 13000,
  OP_BD1_SORT_ROUND2 = 14000,
  OP_BD1_SORT_ROUND3 = 15000,
  OP_BD1_SORT_ROUND4 = 16000,
  OP_BD1_FINAL_FILTER = 17000,


  OP_BD2_SORT1_PREPROCESS = 20000,
  OP_BD2_SORT1_PAD = 21000,
  OP_BD2_SORT1_ROUND1 = 22000,
  OP_BD2_SORT1_ROUND2 = 23000,
  OP_BD2_SORT1_ROUND3 = 24000,
  OP_BD2_SORT1_ROUND4 = 25000,
  OP_BD2_GROUPBY_SCAN1 = 26000,
  OP_BD2_GROUPBY_COLLECT = 27000,
  OP_BD2_GROUPBY_SCAN2 = 28000,
  OP_BD2_SORT2_PREPROCESS = 30000,
  OP_BD2_SORT2_PAD = 31000,
  OP_BD2_SORT2_ROUND1 = 32000,
  OP_BD2_SORT2_ROUND2 = 33000,
  OP_BD2_SORT2_ROUND3 = 34000,
  OP_BD2_SORT2_ROUND4 = 35000,
  OP_BD2_FILTER = 36000,
  OP_BD2_PROJECT = 37000,

  OP_BD3_JOIN_PREPROCESS = 40000,
  OP_BD3_SORT1_PREPROCESS = 41000,
  OP_BD3_SORT1_PAD = 42000,
  OP_BD3_SORT1_ROUND1 = 43000,
  OP_BD3_SORT1_ROUND2 = 44000,
  OP_BD3_SORT1_ROUND3 = 45000,
  OP_BD3_SORT1_ROUND4 = 46000,
  OP_BD3_JOIN_SCAN1 = 47000,
  OP_BD3_JOIN_COLLECT = 48000,
  OP_BD3_JOIN_SCAN2 = 49000,
  OP_BD3_SORT2_SCAN = 50000,
  OP_BD3_SORT2_PREPROCESS = 51000,
  OP_BD3_SORT2_PAD = 52000,
  OP_BD3_SORT2_ROUND1 = 53000,
  OP_BD3_SORT2_ROUND2 = 54000,
  OP_BD3_SORT2_ROUND3 = 55000,
  OP_BD3_SORT2_ROUND4 = 56000,
  OP_BD3_JOIN_FILTER = 57000,
  OP_BD3_JOIN_PROJECT = 58000,
  OP_BD3_AGG_SCAN1 = 59000,
  OP_BD3_AGG_COLLECT = 60000,
  OP_BD3_AGG_SCAN2 = 61000,
  OP_BD3_AGG_PROJECT = 62000,
  OP_BD3_SORT3_PREPROCESS = 63000,
  OP_BD3_SORT3_PAD = 64000,
  OP_BD3_SORT3_ROUND1 = 65000,
  OP_BD3_SORT3_ROUND2 = 66000,
  OP_BD3_SORT3_ROUND3 = 67000,
  OP_BD3_SORT3_ROUND4 = 68000,


  BD1 = 10000,
  BD2 = 10001,
  BD3 = 10002,

  OP_TEST_SORT = 1000000,
  OP_TEST_AGG = 1000100,
};


inline static int get_sort_operation(int op_code) {
  switch(op_code) {

  case OP_SORT_COL1:
  case OP_SORT_COL2:
  case OP_SORT_COL1_COL2:
  case OP_SORT_COL2_IS_DUMMY_COL1:
  case OP_SORT_COL3_IS_DUMMY_COL1:
  case OP_SORT_COL4_IS_DUMMY_COL2:
  case OP_TEST_SORT:
    return SORT_SORT;

  case OP_JOIN_COL1:
  case OP_JOIN_COL2:
  case OP_JOIN_PAGERANK:
  case OP_JOIN_TPCH9GENERIC_NATION:
  case OP_JOIN_TPCH9GENERIC_SUPPLIER:
  case OP_JOIN_TPCH9GENERIC_ORDERS:
  case OP_JOIN_TPCH9GENERIC_PARTSUPP:
  case OP_JOIN_TPCH9GENERIC_PART_LINEITEM:
  case OP_JOIN_TPCH9OPAQUE_NATION:
  case OP_JOIN_TPCH9OPAQUE_SUPPLIER:
  case OP_JOIN_TPCH9OPAQUE_ORDERS:
  case OP_JOIN_TPCH9OPAQUE_LINEITEM:
  case OP_JOIN_TPCH9OPAQUE_PART_PARTSUPP:
  case OP_JOIN_DISEASEDEFAULT_TREATMENT:
  case OP_JOIN_DISEASEDEFAULT_PATIENT:
  case OP_JOIN_DISEASEOPAQUE_PATIENT:
  case OP_JOIN_DISEASEOPAQUE_TREATMENT:
  case OP_JOIN_GENEDEFAULT_GENE:
  case OP_JOIN_GENEOPAQUE_GENE:
  case OP_JOIN_GENEOPAQUE_PATIENT:
    return SORT_JOIN;

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

  void read(uint8_t **block_out, uint32_t *len_out, uint32_t *num_rows_out,
            uint32_t *row_upper_bound_out) {
    if (input >= input_start + input_len) {
      *block_out = NULL;
    } else {
      *block_out = input;
      uint32_t block_enc_size = *reinterpret_cast<uint32_t *>(input); input += 4;
      *len_out = block_enc_size; *len_out += 4;
      *num_rows_out = *reinterpret_cast<uint32_t *>(input); input += 4; *len_out += 4;
      *row_upper_bound_out = *reinterpret_cast<uint32_t *>(input); input += 4; *len_out += 4;
      
      uint32_t task_id = *reinterpret_cast<uint32_t *>(input); input += 4; *len_out += 4;
      (void) task_id;
      
      input += block_enc_size;
    }
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
