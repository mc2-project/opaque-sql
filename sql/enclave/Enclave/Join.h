#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"
#include "InternalTypes.h"

#ifndef JOIN_H
#define JOIN_H

void join_sort_preprocess(int op_code,
						  uint8_t *table_id, 
						  uint8_t *input_row, uint32_t input_row_len,
						  uint8_t *output_row, uint32_t output_row_len);

void scan_collect_last_primary(int op_code,
							   uint8_t *input_rows, uint32_t input_rows_length,
							   uint32_t num_rows,
							   uint8_t *output, uint32_t output_length);

void process_join_boundary(uint8_t *input_rows, uint32_t input_rows_length,
						   uint32_t num_rows,
						   uint8_t *output_rows, uint32_t output_rows_size,
						   uint8_t *enc_table_p, uint8_t *enc_table_f);

void sort_merge_join(int op_code,
					 uint8_t *input_rows, uint32_t input_rows_length,
					 uint32_t num_rows,
					 uint8_t *join_row, uint32_t join_row_length,
					 uint8_t *output, uint32_t output_length);

#endif
