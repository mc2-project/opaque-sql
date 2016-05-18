// -*- mode: C++ -*-

#include "util.h"
#include "NewInternalTypes.h"

void printf(const char *fmt, ...);

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

  w.write(&a, &cur);
  w.close();
  *actual_size = w.bytes_written();
}
