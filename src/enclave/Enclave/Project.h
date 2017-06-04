#include <cstddef>
#include <cstdint>

#ifndef PROJECT_H
#define PROJECT_H

void project(uint8_t *project_list, size_t project_list_length,
             uint8_t *input_rows, size_t input_rows_length,
             uint8_t **output_rows, size_t *output_rows_length);

#endif // PROJECT_H
