/**
*   Copyright(C) 2011-2015 Intel Corporation All Rights Reserved.
*
*   The source code, information  and  material ("Material") contained herein is
*   owned  by Intel Corporation or its suppliers or licensors, and title to such
*   Material remains  with Intel Corporation  or its suppliers or licensors. The
*   Material  contains proprietary information  of  Intel or  its  suppliers and
*   licensors. The  Material is protected by worldwide copyright laws and treaty
*   provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
*   modified, published, uploaded, posted, transmitted, distributed or disclosed
*   in any way  without Intel's  prior  express written  permission. No  license
*   under  any patent, copyright  or  other intellectual property rights  in the
*   Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
*   implication, inducement,  estoppel or  otherwise.  Any  license  under  such
*   intellectual  property  rights must  be express  and  approved  by  Intel in
*   writing.
*
*   *Third Party trademarks are the property of their respective owners.
*
*   Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
*   this  notice or  any other notice embedded  in Materials by Intel or Intel's
*   suppliers or licensors in any way.
*/

#ifndef USER_TYPES_H
#define USER_TYPES_H

/* User defined types */

#ifdef _MSC_VER
#define memccpy _memccpy
#endif

#define LOOPS_PER_THREAD 500

enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2,
  FLOAT = 3,
  DATE = 4,

  PARTIAL_AGG_INT = 5,
  FINAL_AGG_INT = 6,
  PARTIAL_AGG_FLOAT = 7,
  FINAL_AGG_FLOAT = 8
};

enum OPCODE {
  OP_BD2 = 10,
  OP_SORT_INTEGERS_TEST = 90,
  OP_SORT_COL1 = 2,
  OP_SORT_COL2 = 50,
  OP_SORT_COL4_IS_DUMMY_COL2 = 51,
  OP_GROUPBY_COL2_SUM_COL3_STEP1 = 1,
  OP_GROUPBY_COL2_SUM_COL3_STEP2 = 101,
  OP_JOIN_COL2 = 3,
  OP_FILTER_COL2_GT3 = 30,
  OP_FILTER_TEST = 91,
  OP_FILTER_COL4_NOT_DUMMY = 32,
};

typedef void *buffer_t;
typedef int array_t[10];

#endif
