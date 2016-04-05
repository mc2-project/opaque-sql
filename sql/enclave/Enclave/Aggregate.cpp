#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "sgx_trts.h"
#include "math.h"

#include "Aggregate.h"

// scan algorithm
// 1. Global column sort

// 2. Oblivious count-distinct for padding, plus calculate the global offset for
//    each distinct element

//    For global count-distinct, compute local count-distinct, plus first and last
//    elements in each partition. The coordinator uses these to compensate for
//    runs of records that span partition boundaries.

//    Compact numbering can be calculated from this result.

//    Also track the partial sum for the last record of each partition

// 3. If the number of distinct elements is greater than a threshold (e.g., the
//    partition size), use high-cardinality algorithm.

// 4. Map-side aggregate + pad

//    Use the compact numbering from above to ensure the partial aggregrates are
//    output in sorted order and at the correct offset

// 5. Colocate matching partial aggregates using any group-by operator (not
//    necessarily oblivious)

// 6. Reduce-side final aggregate

// defines an upper bound on the size of the aggregation value
// includes the encryption overhead
#define AGG_UPPER_BOUND (4 + 12 + 16 + 128)
#define ROW_UPPER_BOUND (2048)


class aggregate_data {
 public:
  aggregate_data() {

  }
  virtual void reset() {}
  virtual void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {}
  virtual uint32_t ret_length() {}
  virtual void ret_result(uint8_t *result) {}
};

class aggregate_data_sum : public aggregate_data {

 public:
  aggregate_data_sum() {
	sum = 0;
  }

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
	uint8_t *result_ptr = result;
	*result_ptr = 1;
	result_ptr += 1;
	*( (uint32_t *) result_ptr) = 4;
	result_ptr += 4;
	*( (uint32_t *) result_ptr) = sum;
  }

  uint32_t sum;
};

class aggregate_data_count : public aggregate_data {

  aggregate_data_count() {
	count = 0;
  }
  
  void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {
	count += 1;
  }
  
  uint32_t ret_length() {
	return (1 + 4 + 4);
  }
  
  void ret_result(uint8_t *result) {
	uint8_t *result_ptr = result;
	*result_ptr = 1;
	result_ptr += 1;
	*( (uint32_t *) result_ptr) = 4;
	result_ptr += 4;
	*( (uint32_t *) result_ptr) = count;
  }

  uint32_t count;
};


class aggregate_data_avg : public aggregate_data {

  aggregate_data_avg() {
	count = 0;
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

	count += 1;
  }
  
  uint32_t ret_length() {
	return (1 + 4 + 8);
  }
  
  void ret_result(uint8_t *result) {
	uint8_t *result_ptr = result;
	*result_ptr = 1;
	result_ptr += 1;
	*( (uint32_t *) result_ptr) = 8;
	result_ptr += 4;

	double avg = ((double) sum) / ((double) count);
	*( (double *) result_ptr) = avg;
  }

  uint64_t sum;
  uint64_t count;
};


enum AGGTYPE {
  SUM,
  COUNT,
  AVG
};

// return 
aggregate_data get_aggregate_data(int op_code) {

  switch(op_code) {
  case 1:
	{
	  return aggregate_data_sum();
	}
	break;

  case 2:
	break;

  default:
	break;
  }

}


void find_attribute(uint8_t *row, uint32_t length, uint32_t num_cols,
					uint32_t attr_num,
					uint8_t **enc_value_ptr, uint32_t *enc_value_len) {

  uint8_t *enc_value_ptr_ = row;
  uint32_t enc_value_len_ = 0;

  for (uint32_t j = 0; j < num_cols; j++) {
	// [enc len]encrypted{[value type][value len][value]}

	enc_value_len_ = *( (uint32_t *) enc_value_ptr_);
	enc_value_ptr_ += 4;
	  
	// found aggregate attribute
	if (j + 1 == attr_num) {
	  *enc_value_ptr = enc_value_ptr_;
	  *enc_value_len = enc_value_len_;
	  return;
	}
	
	enc_value_ptr_ += enc_value_len_;

  }
}
  
void scan_aggregation_count_distinct(int op_code,
									 uint8_t *input_rows, uint32_t input_rows_length,
									 uint32_t num_rows,
									 uint8_t *agg_row, uint32_t agg_row_length,
									 uint8_t *offset, uint32_t offset_size,
									 uint8_t *output_rows, uint32_t output_rows_length) {

  // pass in a sequence of rows, the current row for scan comparison
  // also pass in the current calculated offset
  // the op_code also decides the aggregation function
  
  // agg_row's should only contain
  // 1. the sort attribute for the previous variable ([type][len][attribute])
  // 2. 4 bytes for # of distinct entries so far
  // 3. partial sum for the newest entry
  
  uint8_t *current_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint8_t *temp_buffer2 = (uint8_t *) malloc(AGG_UPPER_BOUND);
  
  // keeps data on the previous row
  uint8_t *row1 = (uint8_t *) malloc(ROW_UPPER_BOUND);
  uint8_t *row2 = (uint8_t *) malloc(ROW_UPPER_BOUND);

  uint8_t *prev_row = row1;
  uint8_t *current_row = row2;
  
  uint32_t agg_attribute_num = 1;

  // this op_code decides the aggregation function
  // as well as the aggregation column
  switch(op_code) {
  case 1:
	agg_attribute_num = 2;
	break;

  default:
	break;
  }

  aggregate_data agg_data = get_aggregate_data(op_code);

  uint8_t *agg_attribute = NULL;
  uint32_t agg_attribute_len = 0;
  
  int dummy = -1;
  
  decrypt(agg_row, agg_row_length, current_agg);
  if (*current_agg != 0) {
	agg_attribute_len = *( (uint32_t *) current_agg + 1);
	agg_attribute = current_agg + 5;
  } else {
	dummy = 0;
  } // else this is a dummy encrypted row!
	 
  // returned row's structure should be appended with
  // [enc agg len]enc{[agg type][agg len][aggregation]}
  uint8_t *input_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;
  uint8_t *enc_row_ptr = NULL;
  uint32_t enc_row_len = 0;

  uint8_t *prev_row_ptr = NULL;
  uint8_t prev_row_len = 0;
  
  uint8_t *enc_value_ptr = NULL;
  uint32_t enc_value_len = 0;

  uint8_t value_type = 0;
  uint32_t value_len = 0;
  uint8_t *value_ptr = NULL;

  uint32_t *distinct_entries = NULL;
  uint32_t *partial_sum = NULL;
  
  for (uint32_t r = 0; r < num_rows; r++) {
	get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);
	uint32_t num_cols = *( (uint32_t *) enc_row_ptr);
	enc_row_ptr += 4;

	if (r == 0 && dummy == 0) {
	  // if the dummy is empty, is is the very first row of the current partition
	  // put down marker for the previous row
	  prev_row_ptr = enc_row_ptr;
	  prev_row_len = enc_row_len;

	  // also copy the attribute information into current_agg
	  find_attribute(enc_row_ptr, enc_row_len, num_cols,
					 agg_attribute_num,
					 &enc_value_ptr, &enc_value_len);
	  decrypt(enc_value_ptr, enc_value_len, current_agg);
	  agg_attribute_len = *( (uint32_t *) current_agg + 1);
	  agg_attribute = current_agg + 1 + 4;
	  
	  distinct_entries = (uint32_t *) (agg_attribute + agg_attribute_len);
	  *distinct_entries = 0;
	  partial_sum = distinct_entries + 4;
	  *partial_sum = 0;
	  
	  // cleanup
	  enc_row_ptr += enc_row_len;

	  continue;
	}

	find_attribute(enc_row_ptr, enc_row_len, num_cols,
				   agg_attribute_num,
				   &enc_value_ptr, &enc_value_len);
	decrypt(enc_value_ptr, enc_value_len, current_agg);
	  
	// attribute [type][attr len][attr]
	value_ptr = current_agg;
	value_type = *value_ptr;
	value_ptr += 1;
	value_len = *( (uint32_t *) value_ptr);
	value_ptr += 4;

	if (cmp(agg_attribute, value_ptr, value_len) == 0) {
	  agg_data.agg(value_type, value_ptr, value_len);
	  // write the partial into the previous row
		  
	} else {
	  agg_data.reset();
	}
	
	prev_row_ptr = enc_row_ptr;
	prev_row_len = enc_row_len;
	
	enc_row_ptr += enc_row_len;
	
	// should output this row with the partial sum
	
  }
  
}


