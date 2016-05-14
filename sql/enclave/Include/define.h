//#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
//#include <sgx_tcrypto.h>

#ifndef DEFINE_H
#define DEFINE_H

// defines an upper bound on the size of the aggregation value
// only the plaintext size
#define PARTIAL_AGG_UPPER_BOUND (128) // this only includes the partial aggregation
#define ROW_UPPER_BOUND (16 * 50)
// distinct items, offset, sort attribute, aggregation attribute
//#define AGG_UPPER_BOUND (4 + 4 + ROW_UPPER_BOUND + PARTIAL_AGG_UPPER_BOUND)
#define ENC_HEADER_SIZE (SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE)

#define TYPE_SIZE (1)
#define LEN_SIZE (4)
#define HEADER_SIZE (TYPE_SIZE + LEN_SIZE)

#define TABLE_ID_SIZE (8)
//#define JOIN_ROW_UPPER_BOUND (ROW_UPPER_BOUND + TABLE_ID_SIZE)
#define ENC_JOIN_ROW_UPPER_BOUND (ENC_HEADER_SIZE + ROW_UPPER_BOUND + TABLE_ID_SIZE)

#define AGG_UPPER_BOUND (ROW_UPPER_BOUND + 128)
#define JOIN_ROW_UPPER_BOUND AGG_UPPER_BOUND

#define INT_UPPER_BOUND (4)
#define STRING_UPPER_BOUND (512)
#define URL_UPPER_BOUND (100)
#define C_CODE_UPPER_BOUND (3)
#define L_CODE_UPPER_BOUND (6)
#define IP_UPPER_BOUND (15)
#define USER_AGENT_UPPER_BOUND (256)
#define SEARCH_WORD_UPPER_BOUND (32)
#define LONG_UPPER_BOUND (8)

#define ATTRIBUTE_UPPER_BOUND (512)

//#define HALF_MAX_SORT_BUFFER (8 * 1024 * 1024)
//#define MAX_ELEMENTS ((100 * 1024 * 1024) / JOIN_ROW_UPPER_BOUND)

    enum SORT_OP {
      SORT_SORT = 1,
      SORT_JOIN = 2
    };

#define MAX_SINGLE_SORT_BUFFER (70 * 1024 * 1024)
#define MAX_SORT_BUFFER (2 * 1024 * 1024)
#define PAR_MAX_ELEMENTS (MAX_SORT_BUFFER / JOIN_ROW_UPPER_BOUND)

#endif // DEFINE_H
