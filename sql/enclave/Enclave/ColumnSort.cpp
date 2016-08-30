#include "ColumnSort.h"

// Column sort
//
// Step 1: Sort locally
// Step 2: Shuffle to transpose
// Step 3: Sort locally
// Step 4: Shuffle to un-transpose
// Step 5: Sort locally
// Step 6: Shift down
// Step 7: Sort locally
// Step 8: Shift up


void count_rows(uint8_t *input_rows,
                uint32_t buffer_size,
                uint32_t *output_rows) {

  BlockReader r(input_rows, buffer_size);

  uint8_t *block_ptr = NULL;
  uint32_t len = 0;
  uint32_t num_rows = 0;
  uint32_t row_upper_bound = 0;
  uint32_t total_num_rows = 0;

  while (true) {
    r.read(&block_ptr, &len, &num_rows, &row_upper_bound);

    if (block_ptr == NULL) {
      break;
    }

    total_num_rows += num_rows;
    block_ptr = NULL;
  }

  *output_rows = total_num_rows;
}
