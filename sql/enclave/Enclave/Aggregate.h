#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"

#ifndef AGGREGATE_H
#define AGGREGATE_H


void scan_aggregation_count_distinct(int op_code,
									 uint8_t *input_rows, uint32_t input_rows_length,
									 uint32_t num_rows,
									 uint8_t *enc_row, uint32_t enc_row_length,
									 uint8_t *offset, uint32_t offset_size,
									 uint8_t *output_rows, uint32_t output_rows_length);
void scan_aggregation();

#endif // AGGREGATE_H

