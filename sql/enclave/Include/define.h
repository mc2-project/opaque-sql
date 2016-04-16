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

#define INT_UPPER_BOUND (4)
#define STRING_UPPER_BOUND (1024)


#define TABLE_ID_SIZE (8)
#define JOIN_ROW_UPPER_BOUND (ROW_UPPER_BOUND + TABLE_ID_SIZE)
#define ENC_JOIN_ROW_UPPER_BOUND (ENC_HEADER_SIZE + ROW_UPPER_BOUND + TABLE_ID_SIZE)

#endif // DEFINE_H
