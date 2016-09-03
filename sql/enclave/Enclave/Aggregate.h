#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"
#include "util.h"
#include "EncryptedDAG.h"

#ifndef AGGREGATE_H
#define AGGREGATE_H

// Aggregation algorithm:

// 1. Global column sort

// 2. Oblivious count-distinct for padding, plus calculate the global offset for
//    each distinct element

//    For global count-distinct, compute local count-distinct, plus first and last
//    elements in each partition. The coordinator uses these to compensate for
//    runs of records that span partition boundaries.

//    Compact numbering can be calculated from this result.

//    Also track the partial sum for the last record of each partition

// 3. If the number of distinct elements is greater than a threshold (e.g., the
//    partition size), use high-cardinality algorithm.

// 4. Map-side aggregate + pad

//    Use the compact numbering from above to ensure the partial aggregrates are
//    output in sorted order and at the correct offset

// 5. Colocate matching partial aggregates using any group-by operator (not
//    necessarily oblivious)

// 6. Reduce-side final aggregate

// Worked example:
// input:   [aab][bbc][ccc][cde]
//          (one character per row)
// step1:   [a,2,1,b1][b,2,1,c1][c,1,0,c3][c,3,3,e1]
//          [first row, local num distinct, local offset, last row partial aggregate]
// proc_b:  [5,_,_,b][5,1,b1,c][5,2,c1,c][5,2,c4,_]
//          [global num distinct,
//           global offset for last row partial aggregate from prev partition,
//           last row partial aggregate from prev partition (augmented with previous runs),
//           first row of next partition]
// step2:   [a1,A2,b1][b2,B3,c1][c2,c3,c4][C5,D1,E1]
//          (one aggregate per row, final aggregates in caps)

// TODO(ankurdave): support low cardinality mode
// TODO(wzheng): should we set all aggregate statistics to be the AGG_UPPER_BOUND?
// TODO: change the [in] pointers to [user_check]

template<typename AggregatorType>
void aggregate_step1(Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size);

template<typename AggregatorType>
void aggregate_process_boundaries(Verify *verify_set,
                                  uint8_t *input_rows, uint32_t input_rows_length,
                                  uint32_t num_rows,
                                  uint8_t *output_rows, uint32_t output_rows_length,
                                  uint32_t *actual_output_rows_length);

template<typename AggregatorType>
void aggregate_step2(Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *boundary_info_rows, uint32_t boundary_info_rows_length,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size);

// TODO: implement these using NewInternalTypes
class agg_stats_data;
void agg_final_result(agg_stats_data *data, uint32_t offset,
                      uint8_t *result_set, uint32_t result_size);
void final_aggregation(int op_code,
                       uint8_t *agg_rows, uint32_t agg_rows_length,
                       uint32_t num_rows,
                       uint8_t *ret, uint32_t ret_length);

template<typename AggregatorType>
void non_oblivious_aggregate(Verify *verify_set,
                             uint8_t *input_rows, uint32_t input_rows_length,
							 uint32_t num_rows,
							 uint8_t *output_rows, uint32_t output_rows_length,
                             uint32_t *actual_output_rows_length,
                             uint32_t *num_output_rows);

#include "Aggregate.tcc"

#endif // AGGREGATE_H
