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
#include "EncryptedDAG.h"

/**
 * Sort an arbitrary number of encrypted buffers and write the results back to the same buffers. The
 * number of rows in each buffer is specified in num_rows, which is a parallel array to buffer_list,
 * except that buffer_list must contain one extra sentinel element at the end pointing to the end of
 * the last buffer. The buffers should be sized so that a single buffer can fit entirely in enclave
 * memory, and must all be allocated contiguously.
 *
 * This function is intended to be called with the buffers outside of enclave memory. It will
 * decrypt one buffer at a time (or a single row from each of MAX_NUM_STREAMS different buffers)
 * into enclave memory and operate on the decrypted copies, then re-encrypt when writing the results
 * back to the buffers.
 *
 * The scratch memory must be at least as big as all the buffers combined, and it is intended to be
 * allocated outside of enclave memory.
 */
template<typename RecordType>
void external_sort(int op_code,
                   Verify *verify_set,
                   uint32_t num_buffers,
                   uint8_t **buffer_list,
                   uint32_t *num_rows,
                   uint32_t row_upper_bound,
                   uint8_t *scratch,
				   uint32_t *final_len);

/**
 * For distributed sorting, sample rows from a partition of data so they can be collected to a
 * single machine.
 */
template<typename RecordType>
void sample(Verify *verify_set,
            uint8_t *input_rows,
            uint32_t input_rows_len,
            uint32_t num_rows,
            uint8_t *output_rows,
            uint32_t *output_rows_size,
            uint32_t *num_output_rows);

/**
 * For distributed sorting, range-partition the input rows and write the boundary rows into
 * output_rows. The input rows are intended to be sampled from a distributed collection of rows from
 * num_partitions different partitions. Only the intermediate boundary rows will be output,
 * producing num_partitions - 1 rows.
 *
 * The input rows and scratch memory should be allocated in the same format as for external_sort.
 */
template<typename RecordType>
void find_range_bounds(int op_code,
                       Verify *verify_set,
                       uint32_t num_partitions,
                       uint32_t num_buffers,
                       uint8_t **buffer_list,
                       uint32_t *num_rows,
                       uint32_t row_upper_bound,
                       uint8_t *output_rows,
                       uint32_t *output_rows_len,
                       uint8_t *scratch);

/**
 * For distributed sorting, range-partition the input partition according to the specified
 * boundaries. The boundaries should be obtained by broadcasting the output of find_range_bounds to
 * each partition.
 *
 * The input rows and scratch memory should be allocated in the same format as for external_sort.
 *
 * The range partitioning is expressed as an array of pointers into the sorted rows
 * (output_partition_ptrs), one pointer per partition plus a sentinel element at the end pointing to
 * the end of the last partition. The number of rows per partition is written into the parallel
 * array output_partition_num_rows.
 */
template<typename RecordType>
void partition_for_sort(int op_code,
                        Verify *verify_set,
                        uint8_t num_partitions,
                        uint32_t num_buffers,
                        uint8_t **buffer_list,
                        uint32_t *num_rows,
                        uint32_t row_upper_bound,
                        uint8_t *boundary_rows,
                        uint32_t boundary_rows_len,
                        uint8_t *output,
                        uint8_t **output_partition_ptrs,
                        uint32_t *output_partition_num_rows);

#endif /* _SORT_H_ */
