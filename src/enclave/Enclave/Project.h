#include <stdint.h>

#include "EncryptedDAG.h"

#ifndef PROJECT_H
#define PROJECT_H

void project(uint8_t *project_list, size_t project_list_length,
             Verify *verify_set,
             uint8_t *input_rows, uint32_t input_rows_length, uint32_t num_rows,
             uint8_t **output_rows, uint32_t *output_rows_length);

#endif // PROJECT_H
