#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"

#ifndef AGGREGATE_H
#define AGGREGATE_H

class aggregate_data {
 public:
  virtual void agg(uint8_t data_type, uint8_t *data, uint32_t data_len);
};

class aggregate_data_sum : public aggregate_data {
  uint32_t sum;

  void reset() {
	sum = 0;
  }

  void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {
	switch(data_type) {

	case 1: // int
	  {
		uint32_t *int_data_ptr = NULL;
		assert(data_len == 4);
		int_data_ptr = (uint32_t *) data;
		sum += *int_data_ptr;
	  }
	  break;
	default: // cannot handle sum of other types!
	  {
		assert(false);
	  }
	  break;
	}
  }

  uint32_t ret_length() {
	// 1 for type, 4 for length, 4 for data
	return (1 + 4 + 4);
  }

  // serialize the result directly into the result buffer
  void ret_result(uint8_t *result) {
	
  }
};

class aggregate_data_count {
  uint32_t count;

  void reset() {
	count = 0;
  }

  void agg() {
	count += 1;
  }
};

class aggregate_data_avg {
  uint32_t count;
  uint32_t sum;

  void reset() {
	count = 0;
	sum = 0;
  }

  void agg(uint32_t data) {
	count += 1;
	sum += data;
  }
};


void scan_aggregation();

#endif // AGGREGATE_H

