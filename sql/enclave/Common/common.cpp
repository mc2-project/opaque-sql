#include "common.h"

int get_sort_operation(int op_code) {
  switch(op_code) {

  case OP_SORT_COL1:
  case OP_SORT_COL2:
  case OP_SORT_COL3_IS_DUMMY_COL1:
  case OP_SORT_COL4_IS_DUMMY_COL2:
    return SORT_SORT;
    break;

  case OP_JOIN_COL1:
  case OP_JOIN_COL2:
  case OP_JOIN_PAGERANK:
    return SORT_JOIN;
    break;

  default:
    return -1;
  }
}
