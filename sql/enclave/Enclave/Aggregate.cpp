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

// TODO: should we set all aggregate statistics to be the AGG_UPPER_BOUND?

// defines an upper bound on the size of the aggregation value
// includes the encryption

#define PARTIAL_SUM_UPPER_BOUND (4 + 12 + 16 + 128) // this only includes the partial aggregation
#define AGG_UPPER_BOUND (4 + 12 + 16 + 2048) // this includes the sort attribute as well as the partial aggregation
#define ROW_UPPER_BOUND (2048)


class aggregate_data {
 public:
  aggregate_data() {

  }
  virtual void reset() {}
  virtual void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {}
  virtual uint32_t ret_length() {}
  virtual void ret_result(uint8_t *result) {}
  virtual void ret_dummy_result(uint8_t *result) {}
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
	uint32_t cur_value = *( (uint32_t *) result_ptr);
	*( (uint32_t *) result_ptr) = cur_value + sum - cur_value;
  }

  // TODO: is this oblivious?
  void ret_dummy_result(uint8_t *result) {
	uint8_t *result_ptr = result;
	*result_ptr = *result_ptr;
	result_ptr += 1;
	*( (uint32_t *) result_ptr) = 4;
	result_ptr += 4;
	uint32_t cur_value = *( (uint32_t *) result_ptr);
	*( (uint32_t *) result_ptr) = cur_value + (sum - sum);
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

uint32_t current_agg_size(uint8_t *current_agg) {
  // return the size of this buffer
  uint8_t *ptr = current_agg;
  uint32_t size = 0;
  uint32_t temp_size = 0;
  temp_size = *( (uint32_t *) ptr);
  size += 4 + temp_size;
  ptr += 4;
  ptr += temp_size;
  size += 4;
  ptr += 4;

  temp_size = *( (uint32_t *) ptr);
  size += 4 + temp_size;
  return size;
}

void set_agg_pointers(uint8_t *current_agg,
					  uint8_t **sort_attribute_type, uint32_t **sort_attribute_len, uint8_t **sort_attribute,
					  uint32_t **distinct_entries,
					  uint8_t **agg_attribute_type, uint32_t **agg_attribute_len, uint8_t **agg_attribute) {
  
  *sort_attribute_type = current_agg;
  *sort_attribute_len = (uint32_t *) (current_agg + 1);
  *sort_attribute = current_agg + 5;
  
  *distinct_entries = (uint32_t *) (*sort_attribute + **sort_attribute_len);
  
  *agg_attribute_type = ((uint8_t *) *distinct_entries) + 4;
  *agg_attribute_len = (uint32_t *) (*agg_attribute_type + 1);
  *agg_attribute = ((uint8_t *) *agg_attribute_len) + 4;
}

void set_agg_attribute_ptr(uint8_t *current_agg,
						   uint8_t **agg_attr_ptr) {
  *agg_attr_ptr = current_agg;
  *agg_attr_ptr += *((uint32_t *) *agg_attr_ptr);
  *agg_attr_ptr += 4 + 4 + 4;
}
  
void scan_aggregation_count_distinct(int op_code,
									 uint8_t *input_rows, uint32_t input_rows_length,
									 uint32_t num_rows,
									 uint8_t *agg_row, uint32_t agg_row_buffer_length,
									 uint8_t *output_rows, uint32_t output_rows_length) {

  // pass in a sequence of rows, the current row for scan comparison
  // also pass in the current calculated offset
  // the op_code also decides the aggregation function

  // agg_row is initially encrypted
  // [agg_row length]enc{agg_row}
  // after decryption, agg_row's should only contain
  // 1. the sort attribute for the previous variable ([type][len][attribute])
  // 2. 4 bytes for # of distinct entries so far
  // 3. current partial aggregate result ([type][len][attribute])
  
  uint8_t *current_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint8_t *temp_buffer = (uint8_t *) malloc(AGG_UPPER_BOUND);
  
  uint8_t *prev_row = NULL;
  uint8_t *current_row = NULL;
  
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
  agg_data.reset();

  // aggregate attributes point to the columns being aggregated
  // sort attributes point to the GROUP BY attributes
  // TODO: support multicolumn aggregation?
  uint8_t *sort_attribute_type = NULL;
  uint8_t *sort_attribute = NULL;
  uint32_t *sort_attribute_len = 0;
  
  uint32_t *distinct_entries = NULL;
  
  uint8_t *agg_attribute_type = NULL;  
  uint8_t *agg_attribute = NULL;
  uint32_t *agg_attribute_len = 0;

  int dummy = -1;

  uint32_t agg_row_length = *( (uint32_t *) agg_row);
  uint8_t *agg_row_ptr = agg_row + 4;
							   
  decrypt(agg_row_ptr, agg_row_length, current_agg);
  if (*current_agg != 0) {
 	set_agg_pointers(current_agg,
					 &sort_attribute_type, &sort_attribute_len, &sort_attribute,
					 &distinct_entries,
					 &agg_attribute_type, &agg_attribute_len, &agg_attribute);
	
	agg_data.agg(*agg_attribute_type, agg_attribute, *agg_attribute_len);
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

	  set_agg_pointers(current_agg,
					   &sort_attribute_type, &sort_attribute_len, &sort_attribute,
					   &distinct_entries,
					   &agg_attribute_type, &agg_attribute_len, &agg_attribute);

	  // cleanup
	  enc_row_ptr += enc_row_len;

	  continue;
	}

	// should output the previous row with the partial aggregate information
	*( (uint32_t *) output_rows_ptr) = prev_row_len;
	output_rows_ptr += 4;
	cpy(output_rows_ptr, prev_row_ptr, prev_row_len);
	output_rows_ptr += prev_row_len;
	// now encrypt the partial aggregate
	uint32_t partial_agg_len = agg_data.ret_length();
	agg_data.ret_result(temp_buffer);
	*( (uint32_t *) output_rows_ptr) = partial_agg_len;
	encrypt(temp_buffer, partial_agg_len, output_rows_ptr);
	output_rows_ptr += enc_size(partial_agg_len);


	find_attribute(enc_row_ptr, enc_row_len, num_cols,
				   agg_attribute_num,
				   &enc_value_ptr, &enc_value_len);
	decrypt(enc_value_ptr, enc_value_len, current_agg);

	set_agg_pointers(current_agg,
					 &sort_attribute_type, &sort_attribute_len, &sort_attribute,
					 &distinct_entries,
					 &agg_attribute_type, &agg_attribute_len, &agg_attribute);	


	// update the partial aggregate
	if (cmp(agg_attribute, value_ptr, value_len) == 0) {
	  agg_data.agg(value_type, value_ptr, value_len);
	} else {
	  agg_data.reset();
	  agg_data.agg(value_type, value_ptr, value_len);
	}


	// cleanup
	prev_row_ptr = enc_row_ptr;
	prev_row_len = enc_row_len;
	
	enc_row_ptr += enc_row_len;
	
  }

  // first, update current_agg with the agg_data
  uint32_t agg_data_len = agg_data.ret_length();
  set_agg_pointers(current_agg,
				   &sort_attribute_type, &sort_attribute_len, &sort_attribute,
				   &distinct_entries,
				   &agg_attribute_type, &agg_attribute_len, &agg_attribute);
  *((uint32_t *) agg_attribute_len) = agg_data.ret_length();
  agg_data.ret_result(agg_attribute);

  // then write current_agg into agg_row
  uint32_t ca_size = current_agg_size(current_agg);
  encrypt(current_agg, ca_size, agg_row_ptr);
  agg_row_ptr -= 4;
  *( (uint32_t *) agg_row_ptr) = enc_size(ca_size);


  free(current_agg);
  free(temp_buffer);
}


// given a list of boundary records from each partition, process these records
// do a scan, and copy records as necessary
void process_boundary_records(int op_code,
							  uint8_t *agg_rows, uint32_t agg_row_size,
							  uint32_t num_agg_rows,
							  uint8_t *out_agg_rows, uint32_t out_agg_row_size) {
  
  // rows should be structured as
  // [regular row info][enc agg len]enc{agg}[regular row]...

  uint8_t *enc_agg_ptr = agg_rows;
  uint32_t enc_agg_len = 0;

  // agg_row attribute
  uint8_t *sort_attribute_type1 = NULL;
  uint8_t *sort_attribute1 = NULL;
  uint32_t *sort_attribute_len1 = 0;
  
  uint32_t *distinct_entries1 = NULL;
  
  uint8_t *agg_attribute_type1 = NULL;  
  uint8_t *agg_attribute1 = NULL;
  uint32_t *agg_attribute_len1 = 0;

  // another set of pointers.... too many pointers
  uint8_t *sort_attribute_type2 = NULL;
  uint8_t *sort_attribute2 = NULL;
  uint32_t *sort_attribute_len2 = 0;
  
  uint32_t *distinct_entries2 = NULL;
  
  uint8_t *agg_attribute_type2 = NULL;  
  uint8_t *agg_attribute2 = NULL;
  uint32_t *agg_attribute_len2 = 0;

  // agg_data for partial aggregation info
  aggregate_data agg_data = get_aggregate_data(op_code);

  // get next agg
  uint8_t *prev_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint32_t prev_agg_len = 0;
  uint8_t *current_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint32_t current_agg_len = 0;

  // in this scan, we must count the number of distinct items
  // a.k.a. the size of the aggregation result
  uint32_t distinct_items = 0;

  // write back to out_agg_rows
  uint8_t *out_agg_rows_ptr = out_agg_rows;

  for (uint32_t i = 0; i < num_agg_rows; i++) {
	
	if (enc_agg_ptr - agg_rows >= agg_row_size) {
	  break;
	}

	enc_agg_len = *( (uint32_t *) enc_agg_ptr);
	enc_agg_ptr += 4;


	if (i == 0) {
	  decrypt(enc_agg_ptr, enc_agg_len, prev_agg);
	  enc_agg_ptr += enc_agg_len;
	  continue;
	}

	// decrypt into current_agg
	decrypt(enc_agg_ptr, enc_agg_len, current_agg);

	set_agg_pointers(prev_agg,
					 &sort_attribute_type1, &sort_attribute_len1, &sort_attribute1,
					 &distinct_entries1,
					 &agg_attribute_type1, &agg_attribute_len1, &agg_attribute1);

	set_agg_pointers(current_agg,
					 &sort_attribute_type2, &sort_attribute_len2, &sort_attribute2,
					 &distinct_entries2,
					 &agg_attribute_type2, &agg_attribute_len2, &agg_attribute2);

	// compare sort attributes for 
	int ret = cmp(sort_attribute1, sort_attribute2, *sort_attribute_len1);

	if (ret == 0) {
	  // these two attributes are the same -- we have something that spans multiple machines
	  // must aggregate the partial aggregations
	  agg_data.reset();
	  agg_data.agg(*agg_attribute_type1, agg_attribute1, *agg_attribute_len1);
	  agg_data.agg(*agg_attribute_type2, agg_attribute2, *agg_attribute_len2);

	  // write back into current_agg
	  agg_data.ret_result(agg_attribute1);
	}

	if (ret == 0 || i == 1) {
	  // clear prev_agg
	  prev_agg_len = current_agg_size(prev_agg);
	  clear(prev_agg, prev_agg_len);
	}

	// write prev tag into out_agg_rows
	*((uint32_t *) out_agg_rows_ptr) = enc_size(prev_agg_len);
	out_agg_rows_ptr += 4;
	encrypt(prev_agg, prev_agg_len, out_agg_rows);
	out_agg_rows += enc_size(prev_agg_len);

	// copy current_agg into prev_agg
	current_agg_len = current_agg_size(current_agg);
	cpy(prev_agg, current_agg, current_agg_len);
	prev_agg_len = current_agg_len;

  }

  free(prev_agg);
  free(current_agg);
}


// produces the final result
// this assumes that the EPC won't be oblivious: the malicious OS could page out
void allocate_agg_final_result(uint8_t *enc_result_size,
							   uint32_t enc_result_size_length,
							   uint32_t *result_size,
							   uint8_t *result_set) {
  decrypt(enc_result_size, enc_result_size_length, result_size);
  // allocate result set
  result_set = (uint8_t *) (*result_size * AGG_UPPER_BOUND);
}


// This does not assume an oblivious EPC
void agg_final_result(uint8_t *sort_attribute, uint32_t sort_attribute_len,
					  aggregate_data data, uint32_t offset,
					  uint8_t *result_set, uint32_t result_size) {

  uint8_t *result_set_ptr = NULL;
  uint8_t *agg_attribute_ptr = NULL;
  // need to do a scan over the entire result_set
  for (uint32_t i = 0; i < result_size; i++) {
	result_set_ptr = result_set + AGG_UPPER_BOUND * i;
	if (offset == i) {
	  set_agg_attribute_ptr(result_set_ptr, &agg_attribute_ptr);
	  // write back the real aggregate data
	  data.ret_result(agg_attribute_ptr);
	} else {
	  agg_attribute_ptr = result_set_ptr + 1;
	  data.ret_dummy_result(result_set_ptr);
	}
  }
}

// This assumes an oblivious EPC
void agg_final_result_oblivious_epc(aggregate_data data, uint32_t offset,
									uint8_t *result_set, uint32_t result_size) {
  uint8_t *result_set_ptr = result_set + AGG_UPPER_BOUND * offset;
  uint8_t *agg_attribute_ptr = NULL;
  set_agg_attribute_ptr(result_set_ptr, &agg_attribute_ptr);
  data.ret_result(agg_attribute_ptr);
}
