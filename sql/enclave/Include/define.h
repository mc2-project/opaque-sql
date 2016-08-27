//#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
//#include <sgx_tcrypto.h>

#ifndef DEFINE_H
#define DEFINE_H

#define ROW_UPPER_BOUND (1000)
#define ENC_HEADER_SIZE (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE)

// block format: [uint32_t len][uint32_t num_rows][uint32_t row_upper_bound]enc{...}
#define BLOCK_HEADER_SIZE 12

#define TYPE_SIZE (1)
#define LEN_SIZE (4)
#define HEADER_SIZE (TYPE_SIZE + LEN_SIZE)

#define MAX_ROW_ATTRIBUTES 20
#define ENC_ROW_UPPER_BOUND (ROW_UPPER_BOUND + ENC_HEADER_SIZE * MAX_ROW_ATTRIBUTES)

#define AGG_UPPER_BOUND (ROW_UPPER_BOUND + 128)

#define INT_UPPER_BOUND (4)
#define STRING_UPPER_BOUND (256)
#define URL_UPPER_BOUND (100)
#define C_CODE_UPPER_BOUND (3)
#define L_CODE_UPPER_BOUND (6)
#define IP_UPPER_BOUND (15)
#define USER_AGENT_UPPER_BOUND (256)
#define SEARCH_WORD_UPPER_BOUND (32)
#define LONG_UPPER_BOUND (8)

#define ATTRIBUTE_UPPER_BOUND (512)

enum SORT_OP {
  SORT_SORT = 1,
  SORT_JOIN = 2
};

#define MAX_SORT_BUFFER (2 * 1024 * 1024)
// #define MAX_SORT_BUFFER (1 * 1024 * 1024 * 1024u) // for simulation mode

#define MAX_BLOCK_SIZE 1000000

#define MAX_NUM_STREAMS 40u

#endif // DEFINE_H
