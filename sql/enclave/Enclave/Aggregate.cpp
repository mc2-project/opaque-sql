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
// TODO: change the [in] pointers to [user_check]


class aggregate_data {
 public:
  aggregate_data() {

  }
  virtual void reset() {}
  virtual void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {}
  virtual uint32_t ret_length() {}
  virtual void ret_result(uint8_t *result) {}
  virtual void ret_dummy_result(uint8_t *result) {}
  virtual void ret_result_print() {}

  virtual void copy_data(aggregate_data *data) {}
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

  void ret_result_print() {
	printf("Current result is %u\n", sum);
  }

  void copy_data(aggregate_data *data) {

	// only copy if the data is of the same type
	if (dynamic_cast<aggregate_data_sum *>(data) != NULL) {
	  this->sum = dynamic_cast<aggregate_data_sum *> (data)->sum;
	}

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


// should be decrypted attribute
void print_attribute(const char *attr_name, uint8_t *value_ptr) {
  uint8_t attr_type = *value_ptr;
  uint32_t attr_len = *( (uint32_t *) (value_ptr + 1));
  printf("%s: type is %u, attr_len is %u\n", attr_name, attr_type, attr_len);
  if (attr_type == 1) {
	printf("Attr: %u\n", *( (uint32_t *) (value_ptr + 1 + 4)));
  } else if (attr_type == 2) {
	printf("Attr: %.*s\n", attr_len, (char *) (value_ptr + 1 + 4));
  }
}

class agg_stats_data {

public:
  agg_stats_data(int op_code) {
	buffer = (uint8_t *) malloc(AGG_UPPER_BOUND);
	distinct_entries = (uint32_t *) buffer;
	*distinct_entries = 0;
	sort_attr = buffer + 4;
	printf("op_code is %u\n", op_code);
	agg = buffer + 4 + ROW_UPPER_BOUND;
	
	if (op_code == 1) {
	  agg_data = new aggregate_data_sum;
	} else {
	  agg_data = NULL;
	  assert(false);
	}
  }

  ~agg_stats_data() {
	delete agg_data;
	free(buffer);
  }

  void serialize(uint8_t *buf) {
	// serialize the sort
  }
  
  void inc_distinct() {
	*distinct_entries += 1;
  }

  void set_sort_attr(uint8_t *in_sort_attr, uint32_t in_sort_attr_len) {
	cpy(sort_attr, in_sort_attr, in_sort_attr_len);
  }

  uint32_t sort_attr_len() {
	uint32_t *ptr = (uint32_t *) (sort_attr + 1);
	return *ptr;
  }

  uint32_t agg_attr_len() {
	uint32_t *ptr = (uint32_t *) (agg + 1);
	return *ptr;
  }

  // this assumes that the appropriate information has been copied into agg
  void aggregate() {
	agg_data->agg(*agg, agg+1+this->agg_attr_len(), this->agg_attr_len());
  }

  // this flushes agg_data's information into agg
  void flush_agg() {
	agg_data->ret_result(this->agg);
  }

  uint32_t total_agg_len() {
	return agg_data->ret_length();
  }

  // copies 
  void copy_agg(agg_stats_data *data) {
	agg_data->copy_data(data->agg_data);
	cpy(this->sort_attr, data->sort_attr, ROW_UPPER_BOUND);
	cpy(this->agg, data->agg, PARTIAL_AGG_UPPER_BOUND);
  }
  
  int cmp_sort_attr(agg_stats_data *data) {
	if (data->sort_attr_len() != this->sort_attr_len()) {
	  return -1;
	}
	return cmp(data->sort_attr, this->sort_attr, this->sort_attr_len() + 1 + 4);
  }

  // flush all paramters into buffer
  void flush_all() {
	this->flush_agg();
  }

  void print() {
	printf("Distinct entries: %u\n", *distinct_entries);
	print_attribute("sort attr", sort_attr);
	print_attribute("agg attr", agg);
  }

  // public parameters
  uint32_t *distinct_entries;
  // the sort attribute could consist of multiple attributes
  uint8_t *sort_attr;
  uint8_t *agg;

  // underlying buffer: [distinct entries][sort attr][agg]
  uint8_t *buffer;
  
  aggregate_data *agg_data;
};


enum AGGTYPE {
  SUM,
  COUNT,
  AVG
};

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
  // 1. 4 bytes for # of distinct entries so far
  // 2. current partial aggregate result ([type][len][attribute]); size should be fixed to PARTIAL_AGG_UPPER_BOUND
  // 3. 1. the sort attribute for the previous variable ([type][len][attribute])
  agg_stats_data current_agg(op_code);
  agg_stats_data prev_agg(op_code);
  
  uint8_t *prev_row = NULL;
  uint8_t *current_row = NULL;
  
  uint32_t agg_attribute_num = 1;
  uint32_t sort_attribute_num = 1;

  // this op_code decides the aggregation function
  // as well as the aggregation column
  switch(op_code) {
  case 1:
	sort_attribute_num = 2;
	agg_attribute_num = 3;
	break;

  default:
	break;
  }
  
  current_agg.agg_data->reset();

  // aggregate attributes point to the columns being aggregated
  // sort attributes point to the GROUP BY attributes

  int dummy = -1;

  uint32_t agg_row_length = *( (uint32_t *) agg_row);
  uint8_t *agg_row_ptr = agg_row + 4;

  if (test_dummy(agg_row_ptr, agg_row_length) == 0) {
	// copy the entire agg_row_ptr to curreng_agg
	cpy(current_agg.agg, agg_row_ptr, agg_row_length);
	dummy = 0;
  } else {
	decrypt(agg_row_ptr, agg_row_length, current_agg.agg);
	current_agg.aggregate();
  }

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

	printf("Record %u, num cols is %u\n", r, num_cols);
	enc_row_ptr += 4;

	if (r == 0 && dummy == 0) {
	  // if the dummy is empty, it is the very first row of the current partition
	  // put down marker for the previous row
	  prev_row_ptr = enc_row_ptr;
	  prev_row_len = enc_row_len;

	  current_agg.inc_distinct();
	  
	  // also copy the attribute information into current_agg
	  find_attribute(enc_row_ptr, enc_row_len, num_cols,
					 sort_attribute_num,
					 &enc_value_ptr, &enc_value_len);
	  decrypt(enc_value_ptr, enc_value_len, current_agg.sort_attr);

	  find_attribute(enc_row_ptr, enc_row_len, num_cols,
					 agg_attribute_num,
					 &enc_value_ptr, &enc_value_len);
	  decrypt(enc_value_ptr, enc_value_len, current_agg.agg);

	  current_agg.aggregate();
	  
	  // cleanup
	  enc_row_ptr += enc_row_len;

	  continue;
	}

	// copy current_agg to prev_agg
	prev_agg.copy_agg(&current_agg);

	// should output the previous row with the partial aggregate information
	*( (uint32_t *) output_rows_ptr) = prev_row_len;
	output_rows_ptr += 4;
	cpy(output_rows_ptr, prev_row_ptr, prev_row_len);
	output_rows_ptr += prev_row_len;
	
	// now encrypt the partial aggregate and write back
	uint32_t partial_agg_len = prev_agg.agg_data->ret_length();
	prev_agg.flush_agg();
	prev_agg.agg_data->ret_result_print();
	
	*( (uint32_t *) output_rows_ptr) = prev_agg.agg_data->ret_length();
	encrypt(prev_agg.agg, PARTIAL_AGG_UPPER_BOUND, output_rows_ptr);
	output_rows_ptr += enc_size(PARTIAL_AGG_UPPER_BOUND);

	// integrate with the current row
 	find_attribute(enc_row_ptr, enc_row_len, num_cols,
				   sort_attribute_num,
				   &enc_value_ptr, &enc_value_len);
	decrypt(enc_value_ptr, enc_value_len, current_agg.sort_attr);

	find_attribute(enc_row_ptr, enc_row_len, num_cols,
				   agg_attribute_num,
				   &enc_value_ptr, &enc_value_len);
	decrypt(enc_value_ptr, enc_value_len, current_agg.agg);


	// update the partial aggregate
	if (current_agg.cmp_sort_attr(&prev_agg) == 0) {
	  current_agg.aggregate();
	} else {
	  current_agg.inc_distinct();
	  current_agg.agg_data->reset();
	  current_agg.aggregate();
	}

	// cleanup
	prev_row_ptr = enc_row_ptr;
	prev_row_len = enc_row_len;
	
	enc_row_ptr += enc_row_len;
  }

  // For testing only: print current_agg's information
  current_agg.print();

  /// TODO: write back to agg_row, including both sort attribute & aggregation attribute
  // then write current_agg into agg_row
  uint32_t ca_size = enc_size(AGG_UPPER_BOUND);
  printf("ca_size is %u\n", ca_size);
  encrypt(current_agg.buffer, AGG_UPPER_BOUND, agg_row_ptr + 4);
  *( (uint32_t *) agg_row_ptr) = enc_size(AGG_UPPER_BOUND);
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

  // get next agg
  uint8_t *prev_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint32_t prev_agg_len = 0;
  uint8_t *current_agg = (uint8_t *) malloc(AGG_UPPER_BOUND);
  uint32_t current_agg_len = 0;

  aggregate_data_sum agg_data;

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
  decrypt(enc_result_size, enc_result_size_length, (uint8_t *) result_size);
  // allocate result set
  uint32_t size = *result_size;
  result_set = (uint8_t *) malloc(size * AGG_UPPER_BOUND);
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

void agg_test() {
  agg_stats_data current_agg(1);
}
