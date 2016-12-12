#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <assert.h>

#include "sgx_trts.h"
#include "Crypto.h"
#include "util.h"
#include "NewInternalTypes.h"
#include "EncryptedDAG.h"

#ifndef FILTER_H
#define FILTER_H

/** Non-oblivious filter. */
void filter(uint8_t *condition, size_t condition_length,
            Verify *verify_set,
            uint8_t *input_rows, uint32_t input_rows_length, uint32_t num_rows,
            uint8_t **output_rows, uint32_t *output_rows_length, uint32_t *num_output_rows);

bool filter_single_row(int op_code, NewRecord *cur);

#endif
