// -*- mode: C++ -*-

#include "util.h"
#include "NewInternalTypes.h"

void printf(const char *fmt, ...);

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

template <typename AggregatorType>
void aggregate_step1(uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_size) {
  (void)input_rows_length;
  (void)output_rows_length;

  RowReader r(input_rows);
  RowWriter w(output_rows);
  NewRecord cur;
  AggregatorType a;

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    if (i == 0) {
      w.write(&cur);
    }
    a.aggregate(&cur);
  }

  a.set_offset(a.get_num_distinct() - 1);
  w.write(&a);
  w.close();
  *actual_size = w.bytes_written();
}

template <typename AggregatorType>
void aggregate_process_boundaries(uint8_t *input_rows, uint32_t input_rows_length,
                                  uint32_t num_rows,
                                  uint8_t *output_rows, uint32_t output_rows_length,
                                  uint32_t *actual_output_rows_length) {
  (void)input_rows_length;
  (void)output_rows_length;

  // 1. Calculate the global number of distinct items, compensating for runs that span partition
  // boundaries
  uint32_t num_distinct = 0;
  {
    RowReader r(input_rows);
    AggregatorType prev_last_agg, cur_last_agg;
    NewRecord cur_first_row;
    for (uint32_t i = 0; i < num_rows; i++) {
      if (i > 0) prev_last_agg.set(&cur_last_agg);

      r.read(&cur_first_row);
      r.read(&cur_last_agg);

      num_distinct += cur_last_agg.get_num_distinct();
      if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_first_row)) {
        // The current partition begins with a run of items continued from the previous partition,
        // so we shouldn't double count it
        num_distinct--;
      }
    }
  }

  // 2. Send the following items to each partition:
  //    (a) global number of distinct items,
  //    (b) the last partial aggregate from the previous partition (augmented with previous runs),
  //    (c) the global offset for the item involved in (b) within the set of distinct items
  //    (d) the first row of the next partition
  RowWriter w(output_rows);
  RowReader r(input_rows);
  AggregatorType prev_last_agg, cur_last_agg;
  NewRecord cur_first_row, next_first_row;
  uint32_t prev_last_agg_offset = 0, cur_last_agg_offset = 0;
  AggregatorType output;
  for (uint32_t i = 0; i < num_rows; i++) {
    // Populate the prev, cur, next variables to enable lookahead and lookbehind
    if (i > 0) prev_last_agg.set(&cur_last_agg);
    if (i > 0) prev_last_agg_offset = cur_last_agg_offset;
    if (i == 0) r.read(&cur_first_row); else cur_first_row.set(&next_first_row);
    r.read(&cur_last_agg);
    if (i < num_rows - 1) r.read(&next_first_row);

    // Augment cur_last_agg with previous runs (b)
    if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_last_agg)) {
      // The same value in the group by column spans multiple machines
      cur_last_agg.aggregate(&prev_last_agg);
    }

    // Calculate cur_last_agg_offset, compensating for runs that span partition boundaries (c)
    cur_last_agg_offset += cur_last_agg.get_num_distinct();
    if (i > 0 && prev_last_agg.grouping_attrs_equal(&cur_first_row)) {
      // The current partition begins with a run of items continued from the previous partition,
      // so we shouldn't double count it
      cur_last_agg_offset--;
    }

    // Send the results to the current partition
    if (i > 0) output.set(&prev_last_agg);
    if (i > 0) output.set_offset(prev_last_agg_offset);
    output.set_num_distinct(num_distinct);
    w.write(&output);
    if (i < num_rows - 1) w.write(&next_first_row); else w.write(&cur_first_row);
  }

  w.close();
  *actual_output_rows_length = w.bytes_written();
}
