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

template<>
void write_dummy<NewRecord>(int op_code, uint32_t num_rows, RowWriter *writer, NewRecord *last_row) {

  (void)op_code;
  last_row->mark_dummy();
  for (uint32_t i = 0; i < num_rows; i++) {
    writer->write(last_row);
  }

}


template<>
void write_dummy<NewJoinRecord>(int op_code, uint32_t num_rows, RowWriter *writer, NewJoinRecord *last_row) {

  (void)op_code;
  last_row->mark_dummy();
  for (uint32_t i = 0; i < num_rows; i++) {
    writer->write(last_row);
  }

}


template<>
bool is_dummy<NewRecord>(NewRecord *row) {
  return row->is_dummy();
}

template<>
bool is_dummy<NewJoinRecord>(NewJoinRecord *row) {
  return row->get_row().is_dummy();
}
