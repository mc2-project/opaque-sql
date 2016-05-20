#include "Crypto.h"
#include "InternalTypes.h"
#include "NewInternalTypes.h"
#include "math.h"
#include "sgx_trts.h"
#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */

#include "Join.h"

// given a decrypted row and an opcode, extract the join attribute
void get_join_attribute(int op_code,
                        uint32_t num_cols, uint8_t *row,
                        join_attribute *join_attr) {
  join_attr->reset();
  uint8_t *row_ptr = row;
  uint32_t total_value_len = 0;
  uint32_t join_attr_idx = 0;

  if (op_code == OP_JOIN_COL1 || op_code == OP_JOIN_PAGERANK) {
    join_attr_idx = 1;
  } else if (op_code == OP_JOIN_COL2) {
    join_attr_idx = 2;
  } else {
    printf("get_join_attribute: Unknown opcode %d\n", op_code);
    assert(false);
  }

  // Join both tables on join_attr
  for (uint32_t i = 0; i < num_cols; i++) {
    total_value_len = *( (uint32_t *) (row_ptr + TYPE_SIZE)) + TYPE_SIZE + 4;
    if (i + 1 == join_attr_idx) {
      join_attr->new_attribute(row_ptr, total_value_len);
    } else {
      // TODO: dummy write
    }
    row_ptr += total_value_len;
  }
}

// Join in enclave: assumes that the records have been sorted
// by the join attribute already
// This method takes in a temporay row (which could be a dummy row)
// Then it compares with the following rows (which should contain the row info, as well as the table info


// This join can be implemented by merging from one table to another
// The tables should have encrypted identifiers so that they can be identified
//
// Format of the input rows should be:
// enc{table name}{row}
//
// Output row should be a new row, except the join attributes are de-duplicated
//
// Assume that the table has been transformed into a primary key-foreign key
// join format
//
// TODO: should we leak which attributes are being joined, but not the constants?
void sort_merge_join(int op_code,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *join_row, uint32_t join_row_length,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_output_length) {
  (void)input_rows_length;
  (void)join_row_length;
  (void)output_rows_length;

  RowReader r(input_rows);
  RowWriter w(output_rows);
  NewJoinRecord primary, current;
  NewRecord dummy(JOIN_ROW_UPPER_BOUND * 2);
  NewRecord merge(JOIN_ROW_UPPER_BOUND * 2);

  RowReader j_reader(join_row);
  j_reader.read(&primary);
  if (!primary.is_dummy()) {
    check("sort_merge_join: join_row must be marked as primary", primary.is_primary());
    primary.init_join_attribute(op_code);
  }

  uint32_t num_output_cols = 0;
  uint8_t types[5];
  uint32_t secondary_join_attr = 0;

  switch (op_code) {
  case OP_JOIN_COL2:
    num_output_cols = 5;
    types[0] = DUMMY_INT;
    types[1] = DUMMY_STRING;
    types[2] = DUMMY_INT;
    types[3] = DUMMY_INT;
    types[4] = DUMMY_INT;
    secondary_join_attr = 2;
    break;
  case OP_JOIN_COL1:
    num_output_cols = 4;
    types[0] = DUMMY_STRING;
    types[1] = DUMMY_INT;
    types[2] = DUMMY_STRING;
    types[3] = DUMMY_FLOAT;
    secondary_join_attr = 1;
    break;
  case OP_JOIN_PAGERANK:
    num_output_cols = 4;
    types[0] = DUMMY_INT;
    types[1] = DUMMY_FLOAT;
    types[2] = DUMMY_INT;
    types[3] = DUMMY_FLOAT;
    secondary_join_attr = 1;
    break;
  default:
    printf("sort_merge_join: Unknown opcode %d\n", op_code);
    assert(false);
  }
  dummy.init(types, num_output_cols);

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&current);
    current.init_join_attribute(op_code);
    if (current.is_primary()) {
      check("sort_merge_join - primary table uniqueness constraint violation: multiple rows from "
            "the primary table had the same join attribute",
            primary.join_attr.compare(&current.join_attr) != 0);
      primary.set(&current); // advance to a new join attribute
      w.write(&dummy);
    } else {
      if (primary.join_attr.compare(&current.join_attr) != 0) {
        w.write(&dummy); // drop any foreign table rows without a matching primary table row
      } else {
        primary.merge(&current, secondary_join_attr, &merge);
        w.write(&merge);
      }
    }
  }

  w.close();
  *actual_output_length = w.bytes_written();
  return;
}



// do a scan of all of the encrypted rows
// return the last primary table row in this
void scan_collect_last_primary(int op_code,
                               uint8_t *input_rows, uint32_t input_rows_length,
                               uint32_t num_rows,
                               uint8_t *output, uint32_t output_length) {
  (void)op_code;
  (void)input_rows_length;
  (void)output_length;

  RowReader r(input_rows);
  NewJoinRecord cur, last_primary;
  last_primary.reset_to_dummy();

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    if (cur.is_primary()) {
      last_primary.set(&cur);
    }
  }

  RowWriter w(output);
  w.write(&last_primary);
  w.close();
}


// collect and process boundary records
void process_join_boundary(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_size,
                           uint32_t *actual_output_length) {
  (void)op_code;
  (void)input_rows_length;
  (void)output_rows_size;

  RowReader r(input_rows);
  RowWriter w(output_rows);
  NewJoinRecord prev, cur;
  cur.reset_to_dummy();

  for (uint32_t i = 0; i < num_rows; i++) {
    prev.set(&cur);
    w.write(&prev);

    r.read(&cur);
    if (!cur.is_primary()) {
      cur.set(&prev);
    }
  }

  w.close();
  *actual_output_length = w.bytes_written();
}
