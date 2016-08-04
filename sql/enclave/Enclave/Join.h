#include <stdint.h>

#include "util.h"

int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len);
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);

#ifndef JOIN_H
#define JOIN_H

// Oblivious join algorithm (primary-foreign join):
//
// 1. Tag each record from each table with the table id. (join_sort_preprocess)
// 2. Sort all records on the join attribute.
// 3. Send the last primary row from each partition to the driver. If no primary
//    rows exist, send a dummy row. (scan_collect_last_primary)
// 4. On the driver, send each partition that didn't have any primary rows the
//    closest primary row from a previous partition. (process_join_boundary)
// 5. On each partition, emit the final join result by merging each foreign row
//    with the closest previous primary row. For obliviousness, also emit a
//    dummy row for each primary row. (sort_merge_join)
// 6. Filter out the dummy rows using an oblivious filter.

void join_sort_preprocess(
  uint8_t *primary_rows, uint32_t primary_rows_len, uint32_t num_primary_rows,
  uint8_t *foreign_rows, uint32_t foreign_rows_len, uint32_t num_foreign_rows,
  uint8_t *output_rows, uint32_t output_rows_len, uint32_t *actual_output_len);

void scan_collect_last_primary(int op_code,
                               uint8_t *input_rows, uint32_t input_rows_length,
                               uint32_t num_rows,
                               uint8_t *output, uint32_t output_length,
                               uint32_t *actual_output_len);

void process_join_boundary(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_size,
                           uint32_t *actual_output_length);

void sort_merge_join(int op_code,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *join_row, uint32_t join_row_length,
                     uint8_t *output, uint32_t output_length,
                     uint32_t *actual_output_length);

void non_oblivious_sort_merge_join(int op_code,
								   uint8_t *input_rows, uint32_t input_rows_length,
								   uint32_t num_rows,
								   uint8_t *output_rows, uint32_t output_rows_length,
                                   uint32_t *actual_output_length, uint32_t *num_output_rows);
#endif
