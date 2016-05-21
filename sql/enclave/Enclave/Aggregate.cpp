#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "sgx_trts.h"
#include "math.h"

#include "Aggregate.h"

// Given a row and an op_code, hash the appropriate
// Assume that the input is an entire block encrypted 
void hash(int op_code,
		  uint8_t *input_row,
		  uint32_t num_rows,
		  uint32_t input_row_len,
		  uint8_t *output_buffers) {
  (void)op_code;
  (void)input_row;
  (void)output_buffers;
  
  // construct a stream decipher on input row

  // get row size
  uint32_t row_size = dec_size(input_row_len) / num_rows;
  check(dec_size(input_row_len) == row_size * num_rows,
        "hash: input rows cannot be split equally\n");
  
  NewRecord rec(row_size);

  
  
  
}


// non-oblivious aggregation
void non_oblivious_aggregate() {
  

}


