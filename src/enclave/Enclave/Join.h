#include <stdint.h>

#include "util.h"
#include "EncryptedDAG.h"

int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len);
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);

#ifndef JOIN_H
#define JOIN_H

void join_sort_preprocess(
  Verify *verify_set,
  uint8_t *primary_rows, uint32_t primary_rows_len, uint32_t num_primary_rows,
  uint8_t *foreign_rows, uint32_t foreign_rows_len, uint32_t num_foreign_rows,
  uint8_t *output_rows, uint32_t output_rows_len, uint32_t *actual_output_len);

void non_oblivious_sort_merge_join(
    uint8_t *join_expr, size_t join_expr_length,
    uint8_t *input_rows, uint32_t input_rows_length,
    uint8_t **output_rows, uint32_t *output_rows_length,
    uint32_t *num_output_rows);

#endif
