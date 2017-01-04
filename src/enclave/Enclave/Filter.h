#include <stdint.h>

#include "EncryptedDAG.h"

#ifndef FILTER_H
#define FILTER_H

/** Non-oblivious filter. */
void filter(uint8_t *condition, size_t condition_length,
            uint8_t *input_rows, uint32_t input_rows_length,
            uint8_t **output_rows, uint32_t *output_rows_length, uint32_t *num_output_rows);

#endif
