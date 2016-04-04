#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "sgx_trts.h"
#include "math.h"

#include "Aggregate.h"

// scan algorithm
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

// defines an upper bound on the size of the aggregation value
// includes the encryption overhead
#define AGG_UPPER_BOUND (4 + 12 + 16 + 128)
#define ROW_UPPER_BOUND (2048)

enum AGGTYPE {
  SUM,
  COUNT,
  AVG
};

void scan_aggregation_count_distinct(int op_code,
									 uint8_t *input_rows, uint32_t input_rows_length,
									 uint32_t num_rows,
									 uint8_t *enc_row, uint32_t enc_row_length,
									 uint8_t *offset, uint32_t offset_size,
									 uint8_t *output_rows, uint32_t output_rows_length) {

  // pass in a sequence of rows, the current row for scan comparison
  // also pass in the current calculated offset
  // the op_code also decides the aggregation function
  
  // enc_row's should only contain
  // 1. the sort attribute for the previous variable ([type][len][attribute])
  // 2. 4 bytes for # of distinct entries so far
  // 3. partial sum for the newest entry
  
  uint8_t *temp_buffer = (uint8_t *) malloc(ROW_UPPER_BOUND);
  uint32_t attribute_num = 1;
  int operation = COUNT;

  // this op_code decides the aggregation function
  // as well as the aggregation column
  switch(op_code) {
  case 1:
	operation = SUM;
	attribute_num = 2;
	break;

  default:
	break;
  }

  uint8_t *agg_attribute = NULL;
  uint32_t agg_attribute_len = 0;

  
  decrypt(enc_row, enc_row_length, temp_buffer);
  if (*temp_buffer != 0) {
	
  } // else this is a dummy encrypted row!
	 
  // returned row's structure should be appended with
  // [enc agg len]enc{[agg type][agg len][aggregation]}
  uint8_t *input_rows_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;
  for (uint32_t r = 0; r < num_rows; r++) {
	// decrypt row
	uint32_t num_cols = *( (uint32_t *) input_rows_ptr);
	input_rows_ptr += 4;

	// for (uint32_t ) {
	  
	// }
  }

}

void scan_aggregation() {
  
}
