#include "Project.h"

#include <time.h>

void project(int op_code,
             Verify *verify_set,
             uint8_t *input_rows, uint32_t input_rows_length,
             uint32_t num_rows,
             uint8_t *output_rows, uint32_t output_rows_length,
             uint32_t *actual_output_rows_length) {
  (void)output_rows_length;

  RowReader r(input_rows, input_rows + input_rows_length, verify_set);
  RowWriter w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());

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
    out->clear();
    out->add_attr(in, 1);
    out->set_attr_len(1, 8);
    out->add_attr(in, 4);
    break;
  case OP_PROJECT_PAGERANK_WEIGHT_RANK:
  {
    out->clear();
    out->add_attr(in, 4);
    float rank = *reinterpret_cast<const float *>(in->get_attr_value(2));
    float weight = *reinterpret_cast<const float *>(in->get_attr_value(5));
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
  case OP_PROJECT_TPCH9GENERIC:
  {
    out->clear();
    out->add_attr(in, 2);
    out->add_attr(in, 5);
    float l_extendedprice = *reinterpret_cast<const float *>(in->get_attr_value(9));
    float l_discount = *reinterpret_cast<const float *>(in->get_attr_value(10));
    float ps_supplycost = *reinterpret_cast<const float *>(in->get_attr_value(7));
    uint32_t l_quantity = *reinterpret_cast<const uint32_t *>(in->get_attr_value(8));
    float result = static_cast<float>(
      static_cast<double>(l_extendedprice) * (1.0L - l_discount)
      - static_cast<double>(ps_supplycost) * l_quantity);
    out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&result));
    break;
  }
  case OP_PROJECT_TPCH9OPAQUE:
  {
    out->clear();
    out->add_attr(in, 4);
    out->add_attr(in, 2);
    float l_extendedprice = *reinterpret_cast<const float *>(in->get_attr_value(9));
    float l_discount = *reinterpret_cast<const float *>(in->get_attr_value(10));
    float ps_supplycost = *reinterpret_cast<const float *>(in->get_attr_value(7));
    uint32_t l_quantity = *reinterpret_cast<const uint32_t *>(in->get_attr_value(8));
    float result = static_cast<float>(
      static_cast<double>(l_extendedprice) * (1.0L - l_discount)
      - static_cast<double>(ps_supplycost) * l_quantity);
    out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&result));
    break;
  }
  case OP_PROJECT_TPCH9_ORDER_YEAR:
  {
    out->clear();
    out->add_attr(in, 1);

    uint64_t date = *reinterpret_cast<const uint64_t *>(in->get_attr_value(2));
    struct tm tm;
    secs_to_tm(date, &tm);
    uint32_t result = 1900 + tm.tm_year;
    out->add_attr(INT, 4, reinterpret_cast<const uint8_t *>(&result));
    break;
  }
  case OP_PROJECT_ADD_RANDOM_ID:
  {
    out->clear();
    uint32_t random = 0;
    sgx_read_rand(reinterpret_cast<uint8_t *>(&random), 4);
    out->add_attr(INT, 4, reinterpret_cast<uint8_t *>(&random));
    out->append(in);
    break;
  }
  case OP_PROJECT_DROP_COL1:
    out->clear();
    for (uint32_t i = 2; i <= in->num_cols(); i++) {
      out->add_attr(in, i);
    }
    break;
  case OP_PROJECT_DROP_COL2:
    out->clear();
    for (uint32_t i = 1; i <= in->num_cols(); i++) {
      if (i != 2) {
        out->add_attr(in, i);
      }
    }
    break;
  case OP_PROJECT_DROP_COL3:
    out->clear();
    for (uint32_t i = 1; i <= in->num_cols(); i++) {
      if (i != 3) {
        out->add_attr(in, i);
      }
    }
    break;
  case OP_PROJECT_SWAP_COL1_COL2:
    out->clear();
    for (uint32_t i = 1; i <= in->num_cols(); i++) {
      uint32_t src_i = (i == 1) ? 2 : (i == 2) ? 1 : i;
      out->add_attr(in, src_i);
    }
    break;
  case OP_PROJECT_SWAP_COL2_COL3:
    out->clear();
    for (uint32_t i = 1; i <= in->num_cols(); i++) {
      uint32_t src_i = (i == 2) ? 3 : (i == 3) ? 2 : i;
      out->add_attr(in, src_i);
    }
    break;
  case OP_PROJECT_COL4_COL2_COL5:
    out->clear();
    out->add_attr(in, 4);
    out->add_attr(in, 2);
    out->add_attr(in, 5);
    break;
  case OP_PROJECT_COL2_COL1_COL4:
    out->clear();
    out->add_attr(in, 2);
    out->add_attr(in, 1);
    out->add_attr(in, 4);
    break;
  case OP_PROJECT_COL2_COL4:
    out->clear();
    out->add_attr(in, 2);
    out->add_attr(in, 4);
    break;
  case OP_PROJECT_COL3:
    out->clear();
    out->add_attr(in, 3);
    break;
  case OP_PROJECT_COL2_ADD_1:
    out->clear();
    for (uint32_t i = 1; i <= in->num_cols(); i++) {
      if (i == 2) {
        uint32_t v = *reinterpret_cast<const uint32_t *>(in->get_attr_value(2)) + 1;
        out->add_attr(INT, 4, reinterpret_cast<const uint8_t *>(&v));
      } else {
        out->add_attr(in, i);
      }
    }
    break;

  case OP_PROJECT_LS:
    {
      // we want to project the three products: x1 * x1, x1 * x2, x2 * x2
      out->clear();
      float x1 = *reinterpret_cast<const float *>(in->get_attr_value(1));
      float x2 = *reinterpret_cast<const float *>(in->get_attr_value(2));
      float y = *reinterpret_cast<const float *>(in->get_attr_value(3));

      float c11 = x1 * x1;
      float c12 = x1 * x2;
      float c22 = x2 * x2;
      float b1 = x1 * y;
      float b2 = x2 * y;

      out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&c11));
      out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&c12));
      out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&c22));
      out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&b1));
      out->add_attr(FLOAT, 4, reinterpret_cast<const uint8_t *>(&b2));
    }
    break;

  default:
    printf("project_single_row: unknown opcode %d\n", op_code);
    assert(false);
  }
}
