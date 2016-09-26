#include "Crypto.h"
#include "NewInternalTypes.h"
#include "math.h"
#include "sgx_trts.h"
#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */

#include "Join.h"

void join_sort_preprocess(
  Verify *verify_set,
  uint8_t *primary_rows, uint32_t primary_rows_len, uint32_t num_primary_rows,
  uint8_t *foreign_rows, uint32_t foreign_rows_len, uint32_t num_foreign_rows,
  uint8_t *output_rows, uint32_t output_rows_len, uint32_t *actual_output_len) {
  (void)output_rows_len;

  // Set the row upper bound for the output rows to the max of the primary and foreign row sizes by
  // reading a row from each, converting it to the output format, and taking the max upper bound
  uint32_t row_upper_bound;
  {
    uint32_t r1_upper_bound = 0, r2_upper_bound = 0;
    NewRecord a;
    NewJoinRecord b;

    RowReader r1(primary_rows, primary_rows + primary_rows_len);
    if (r1.has_next()) {
      r1.read(&a);
      b.set(true, &a);
      r1_upper_bound = b.row_upper_bound();
      debug("a upper bound %d\n", b.row_upper_bound());
    }

    RowReader r2(foreign_rows, foreign_rows + foreign_rows_len);
    if (r2.has_next()) {
      r2.read(&a);
      b.set(false, &a);
      debug("b upper bound %d\n", b.row_upper_bound());
      r2_upper_bound = b.row_upper_bound();
    }

    row_upper_bound = r1_upper_bound > r2_upper_bound ? r1_upper_bound : r2_upper_bound;
  }

  RowWriter w(output_rows, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewRecord a;
  NewJoinRecord b;

  RowReader primary(primary_rows, primary_rows + primary_rows_len, verify_set);
  for (uint32_t i = 0; i < num_primary_rows; i++) {
    primary.read(&a);
    b.set(true, &a);
    w.write(&b);
  }

  RowReader foreign(foreign_rows, foreign_rows + foreign_rows_len, verify_set);
  for (uint32_t i = 0; i < num_foreign_rows; i++) {
    foreign.read(&a);
    b.set(false, &a);
    w.write(&b);
  }

  w.close();
  *actual_output_len = w.bytes_written();
}

void scan_collect_last_primary(int op_code,
                               Verify *verify_set,
                               uint8_t *input_rows, uint32_t input_rows_length,
                               uint32_t num_rows,
                               uint8_t *output, uint32_t output_length,
                               uint32_t *actual_output_len) {
  (void)op_code;
  (void)output_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  NewJoinRecord cur, last_primary;
  last_primary.reset_to_dummy();

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    if (cur.is_primary()) {
      last_primary.set(&cur);
    }
  }

  IndividualRowWriterV w(output);
  w.set_self_task_id(verify_set->get_self_task_id());
  w.write(&last_primary);
  w.close();
  *actual_output_len = w.bytes_written();
}

void process_join_boundary(int op_code,
                           Verify *verify_set,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_size,
                           uint32_t *actual_output_length) {
  (void)op_code;
  (void)input_rows_length;
  (void)output_rows_size;

  IndividualRowReaderV r(input_rows, verify_set);
  IndividualRowWriterV w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
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

void sort_merge_join(int op_code,
                     Verify *verify_set,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *join_row, uint32_t join_row_length,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_output_length) {
  (void)join_row_length;
  (void)output_rows_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  NewJoinRecord primary, current;
  NewRecord dummy;
  NewRecord merge;

  IndividualRowReaderV j_reader(join_row, verify_set);
  j_reader.read(&primary);
  if (!primary.is_dummy()) {
    check(primary.is_primary(), "sort_merge_join: join_row must be marked as primary\n");
    primary.init_join_attribute(op_code);
  }

  NewJoinRecord::init_dummy(&dummy, op_code);

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&current);
    current.init_join_attribute(op_code);

    if (current.is_primary()) {
      check(!primary.join_attr_equals(&current, op_code),
            "sort_merge_join - primary table uniqueness constraint violation: multiple rows from "
            "the primary table had the same join attribute\n");
      primary.set(&current); // advance to a new join attribute
      w.write(&dummy);
    } else {
      if (!primary.join_attr_equals(&current, op_code)) {
        w.write(&dummy); // drop any foreign table rows without a matching primary table row
      } else {
        primary.merge(&current, &merge, op_code);
        w.write(&merge);
      }
    }
  }

  w.close();
  *actual_output_length = w.bytes_written();
  return;
}


void non_oblivious_sort_merge_join(int op_code, Verify *verify_set,
								   uint8_t *input_rows, uint32_t input_rows_length,
								   uint32_t num_rows,
								   uint8_t *output_rows, uint32_t output_rows_length,
                                   uint32_t *actual_output_length, uint32_t *num_output_rows) {
  (void) output_rows_length;
  
  RowReader reader(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter writer(output_rows);
  writer.set_self_task_id(verify_set->get_self_task_id());

  NewJoinRecord primary, current;
  NewRecord merge;

  uint32_t num_output_rows_result = 0;
  for (uint32_t i = 0; i < num_rows; i++) {
    reader.read(&current);
    current.init_join_attribute(op_code);

    if (current.is_primary()) {
      check(!primary.join_attr_equals(&current, op_code),
            "sort_merge_join - primary table uniqueness constraint violation: multiple rows from "
            "the primary table had the same join attribute\n");
      primary.set(&current); // advance to a new join attribute
    } else {
      if (primary.join_attr_equals(&current, op_code)) {
        primary.merge(&current, &merge, op_code);
        writer.write(&merge);
        num_output_rows_result++;
      }
    }
  }

  writer.close();
  *actual_output_length = writer.bytes_written();
  *num_output_rows = num_output_rows_result;
}
