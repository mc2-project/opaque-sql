#ifndef _SORT_H_
#define _SORT_H_

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <sgx_trts.h>
#include <vector>
#include <algorithm>

#include "util.h"
#include "Aggregate.h"
#include "Crypto.h"
#include "Join.h"
#include "Filter.h"
#include "Project.h"
#include "common.h"

template<typename RecordType>
void sample(uint8_t *input_rows,
			uint8_t *output_rows,
			uint32_t *output_rows_size);

template<typename RecordType>
void find_range_bounds(int op_code, 
					   uint32_t num_buffers,
					   uint8_t *input_rows,
					   uint32_t input_rows_len,
					   uint8_t *scratch,
					   uint8_t *output_rows,
					   uint32_t *output_rows_len);


template<typename RecordType>
void external_sort(int op_code,
				   uint32_t num_buffers,
				   uint8_t **buffer_list,
				   uint32_t *num_rows,
				   uint32_t row_upper_bound,
				   uint8_t *scratch);


template<typename RecordType>
void sort_partition(int op_code,
					uint32_t num_buffers,
					uint8_t *input_rows,
					uint32_t input_rows_len,
					uint8_t *boundary_rows,
					uint8_t num_partitions,
					uint8_t *output,
					uint8_t **output_stream_list,
					uint8_t *scratch);


template<typename T>
class HeapSortItem {
 public:
  uint32_t index;
  T *v;
};


#endif /* !_SORT_H_ */
