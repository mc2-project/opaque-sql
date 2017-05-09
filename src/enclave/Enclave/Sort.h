#include <cstddef>
#include <cstdint>

#ifndef _SORT_H_
#define _SORT_H_

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
void external_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length);

/**
 * For distributed sorting, sample rows from a partition of data so they can be collected to a
 * single machine.
 */
void sample(uint8_t *input_rows, size_t input_rows_length,
			uint8_t **output_rows, size_t *output_rows_length);

/**
 * For distributed sorting, range-partition the input rows and write the boundary rows into
 * output_rows. The input rows are intended to be sampled from a distributed collection of rows from
 * num_partitions different partitions. Only the intermediate boundary rows will be output,
 * producing num_partitions - 1 rows.
 */
void find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                       uint32_t num_partitions,
                       uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length);
/**
 * For distributed sorting, range-partition the input partition according to the specified
 * boundaries. The boundaries should be obtained by broadcasting the output of find_range_bounds to
 * each partition.
 *
 * The range partitioning is expressed as an array of buffers, one per output partition.
 */
void partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                        uint8_t *input_rows, size_t input_rows_length,
                        uint8_t *boundary_rows, size_t boundary_rows_length,
                        uint8_t **output_partition_ptrs, size_t *output_partition_lengths);

#endif /* _SORT_H_ */
