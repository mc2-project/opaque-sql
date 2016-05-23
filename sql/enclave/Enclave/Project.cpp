#include "Project.h"

void project(int op_code,
             uint8_t *input_rows, uint32_t input_rows_length,
             uint32_t num_rows,
             uint8_t *output_rows, uint32_t output_rows_length,
             uint32_t *actual_output_rows_length) {
  (void)input_rows_length;
  (void)output_rows_length;

  RowReader r(input_rows);
  RowWriter w(output_rows);
  NewRecord in;
  NewRecord out;

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&in);
    project_single_row(op_code, &in, &out);
    w.write(&out);
  }

  w.close();

  *actual_output_rows_length = w.bytes_written();
}

void project_single_row(int op_code, NewRecord *in, NewRecord *out) {
  switch (op_code) {
  case OP_BD2:
    out->set(in);
    out->set_attr_len(1, 8);
    break;
  case OP_PROJECT_PAGERANK_WEIGHT_RANK:
  {
    // in: [id, rank, dst, weight]
    // out: [dst, rank * weight]
    out->clear();
    out->add_attr(in, 3);
    float rank = *reinterpret_cast<const float *>(in->get_attr_value(2));
    float weight = *reinterpret_cast<const float *>(in->get_attr_value(4));
    float result = rank * weight;
    out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&result));
    break;
  }
  case OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK:
  {
    out->clear();
    out->add_attr(in, 1);
    float incoming_rank = *reinterpret_cast<const float *>(in->get_attr_value(2));
    float result = 0.15 + 0.85 * incoming_rank;
    out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&result));
    break;
  }
  default:
    printf("project_single_row: unknown opcode %d\n", op_code);
    assert(false);
  }
}
