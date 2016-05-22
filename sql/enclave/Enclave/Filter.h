#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <assert.h>

#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"
#include "util.h"
#include "NewInternalTypes.h"

#ifndef FILTER_H
#define FILTER_H

/** Non-oblivious filter. */
void filter(int op_code,
            uint8_t *input_rows, uint32_t input_rows_length,
            uint32_t num_rows,
            uint8_t *output_rows, uint32_t output_rows_length,
            uint32_t *actual_output_rows_length, uint32_t *num_output_rows);

bool filter_single_row(int op_code, NewRecord *cur);

#endif
