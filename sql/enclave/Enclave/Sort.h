#ifndef _SORT_H_
#define _SORT_H_

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <sgx_trts.h>
#include <vector>
#include <algorithm>

#include "util.h"
#include "Aggregate.h"
#include "Crypto.h"
#include "Join.h"
#include "Filter.h"
#include "Project.h"
#include "common.h"


template<typename RecordType>
void external_sort(int op_code,
				   uint32_t num_buffers,
				   uint8_t **buffer_list,
				   uint32_t *num_rows,
				   uint32_t row_upper_bound,
				   uint8_t *scratch);

template<typename T>
class HeapSortItem {
 public:
  uint32_t index;
  T *v;
};


#endif /* !_SORT_H_ */
