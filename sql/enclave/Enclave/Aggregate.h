#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"
#include "util.h"
#include "InternalTypes.h"

#ifndef AGGREGATE_H
#define AGGREGATE_H

class agg_stats_data;

void scan_aggregation_count_distinct(int op_code,
				     uint8_t *input_rows, uint32_t input_rows_length,
				     uint32_t num_rows,
				     uint8_t *agg_row, uint32_t agg_row_buffer_length,
				     uint8_t *output_rows, uint32_t output_rows_length,
				     uint32_t *actual_output_rows_length,
				     int flag,
				     uint32_t *cardinality);

void process_boundary_records(int op_code,
			      uint8_t *rows, uint32_t rows_size,
			      uint32_t num_rows,
			      uint8_t *out_agg_rows, uint32_t out_agg_row_size,
			      uint32_t *actual_out_agg_row_size);

void agg_test();

void agg_final_result(agg_stats_data *data, uint32_t offset,
		      uint8_t *result_set, uint32_t result_size);


void final_aggregation(int op_code,
		       uint8_t *agg_rows, uint32_t agg_rows_length,
		       uint32_t num_rows,
		       uint8_t *ret, uint32_t ret_length);
#endif // AGGREGATE_H

