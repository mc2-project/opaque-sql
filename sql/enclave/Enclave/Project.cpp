#include "Project.h"
#include "NewInternalTypes.h"

void project(int op_code,
             uint8_t *input_rows, uint32_t input_rows_length,
             uint32_t num_rows,
             uint8_t *output_rows, uint32_t output_rows_length,
             uint32_t *actual_output_rows_length) {
  (void)input_rows_length;
  (void)output_rows_length;

  RowReader r(input_rows);
  RowWriter w(output_rows);
  NewProjectRecord current_row(op_code);

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&current_row);
    w.write(&current_row);
  }

  w.close();

  *actual_output_rows_length = w.bytes_written();
}
