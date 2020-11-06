#include <cstddef>
#include <cstdint>
#include <string>

#ifndef _SORT_H_
#define _SORT_H_

/**
 * Sort an arbitrary number of encrypted input rows by decrypting a limited number of rows at a time
 * into enclave memory, sorting them using quicksort, and re-encrypting them to untrusted memory.
 * The granularity of decryption is a tuix::EncryptedBlock, which should fit entirely in enclave
 * memory.
 */
void external_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length,
                   std::string curr_ecall=std::string("externalSort"));

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
 * producing (up to) num_partitions - 1 rows. If fewer than num_partitions - 1 input rows are
 * provided, then only that many boundary rows will be returned.
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
                        uint32_t num_partitions,
                        uint8_t *input_rows, size_t input_rows_length,
                        uint8_t *boundary_rows, size_t boundary_rows_length,
                        uint8_t **output_partition_ptrs, size_t *output_partition_lengths);

#endif /* _SORT_H_ */
