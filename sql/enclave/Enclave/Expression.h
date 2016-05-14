#ifndef EXPRESSION_H
#define EXPRESSION_H

#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "sgx_trts.h"
#include "math.h"
#include "define.h"

#include "InternalTypes.h"

// A list of all of the UDFs
enum EXPRESSION {
  IDENTITY,                 // place holder for the identity function
  PREFIX,                   // prefix of a single string
  INTEGER_SUM,              // sum of multiple columns
  COMPARE,
  BD2,
  TEST,
  PR_WEIGHT_RANK,
  PR_APPLY_INCOMING_RANK
};

enum EVAL_MODE {
  PROJECT,
  SORT,
  AGG,
  JOIN,
  AGG_AGG
};

void evaluate_expr(GenericType **input_attr, uint32_t num_input_attr,
                   GenericType **output_attr, uint32_t num_output_attr,
                   uint32_t expression,
                   int mode);

#endif
