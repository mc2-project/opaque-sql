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


enum AGGREGATION_CARDINALITY {
  LOW_AGG = 1,
  HIGH_AGG = 2
};

// mode indicates whether we're in low cardinality or high cardinality situation
// 1: scan write (low)
// 2: global sort (high)
int mode = HIGH_AGG;

class aggregate_data {
public:
  aggregate_data() {}
  virtual ~aggregate_data() {}
  virtual void reset() {}
  virtual void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {
    (void)data_type;
    (void)data;
    (void)data_len;
  }
  virtual uint32_t ret_length() {
    printf("aggregate_data::ret_length not implemented\n");
    assert(false);
    return 0;
  }
  virtual void ret_result(uint8_t *result) {
    (void)result;
  }
  virtual void ret_dummy_result(uint8_t *result) {
    (void)result;
  }
  virtual void ret_result_print() {}

  virtual void copy_data(aggregate_data *data) {
    (void)data;
  }

  virtual uint32_t flush(uint8_t *output, int if_final) = 0;

  virtual void agg(GenericType *v) {
    (void)v;
  }
  virtual uint32_t agg_buffer(uint8_t *input) {
    (void)input;
    printf("aggregate_data::agg_buffer not implemented\n");
    assert(false);
    return 0;
  }
  virtual uint32_t flush_agg(uint8_t *output) {
    (void)output;
    printf("aggregate_data::flush_agg not implemented\n");
    assert(false);
    return 0;
  }
};

class aggregate_data_count : public aggregate_data {

  aggregate_data_count() {
    count = 0;
  }

  void agg(uint8_t data_type, uint8_t *data, uint32_t data_len) {
    (void)data_type;
    (void)data;
    (void)data_len;
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

  uint32_t flush(uint8_t *output, int if_final) {
    uint8_t *result_ptr = output;
    if (if_final == 0) {
      *result_ptr = INT;
    } else {
      *result_ptr = DUMMY_INT;
    }

    result_ptr += TYPE_SIZE;
    *( (uint32_t *) result_ptr) = 4;
    result_ptr += 4;
    *( (uint32_t *) result_ptr) = count;

    return HEADER_SIZE + 4;
  }

  uint32_t count;
};


class generic_agg_sum : public aggregate_data {
public:
  generic_agg_sum() {
    agg_field = NULL;
  }

  void init(uint8_t type) {
    if (agg_field == NULL) {
      switch(type) {
      case INT:
        agg_field = new Integer;
        break;

      case FLOAT:
        agg_field = new Float;
        break;
      default:
      {
        printf("Type %d not handled in generic_agg_sum!\n", type);
        assert(false);
      }
      break;
      }
    }
  }

  virtual ~generic_agg_sum() {
    if (agg_field != NULL) {
      delete agg_field;
    }
  }

  void agg(GenericType *v) {
    init(v->type_);
    agg_field->sum(v);
  }

  uint32_t agg_buffer(uint8_t *data_ptr) {
    uint8_t attr_type = *data_ptr;
    init(attr_type);
    uint32_t len = 0;

    switch(attr_type) {
    case INT:
    {
      uint32_t num = *( (uint32_t *) (data_ptr + HEADER_SIZE));
      dynamic_cast<Integer *>(agg_field)->value += num;
      len = HEADER_SIZE + 4;
    }
    break;

    case FLOAT:
    {
      float num = *( (float *) (data_ptr + HEADER_SIZE));
      dynamic_cast<Float *>(agg_field)->value += num;
      len = HEADER_SIZE + 4;
    }
    break;

    default:
      printf("generic_agg_sum::agg_buffer: Unknown type %d\n", attr_type);
      assert(false);
    }

    return len;
  }

  void reset() {
    if (agg_field != NULL) {
      agg_field->reset();
    }
  }

  uint32_t flush(uint8_t *output, int if_final) {
    uint8_t *result_ptr = output;

    agg_field->flush(result_ptr);

    if (if_final == 0) {
      *result_ptr = agg_field->type_;
    } else {
      *result_ptr = get_dummy_type(agg_field->type_);
    }

    return HEADER_SIZE + *((uint32_t *) (result_ptr + TYPE_SIZE));
  }

  uint32_t flush_agg(uint8_t *output) {
    uint8_t *output_ptr = output;
    // evaluate agg_field
    agg_field->flush(output_ptr);
    uint32_t flush_len = *( (uint32_t *) (output_ptr + TYPE_SIZE)) + HEADER_SIZE;

    return flush_len;
  }


  void copy_data(aggregate_data *data) {

    // only copy if the data is of the same type
    if (dynamic_cast<generic_agg_sum *>(data) != NULL) {
      GenericType *v2 = dynamic_cast<generic_agg_sum *> (data)->agg_field;
      init(v2->type_);
      GenericType *v1 = this->agg_field;

      if (v1->type_ == INT) {
        dynamic_cast<Integer *>(v1)->value = dynamic_cast<Integer *>(v2)->value;
      } else if (v1->type_ == FLOAT) {
        dynamic_cast<Float *>(v1)->value = dynamic_cast<Float *>(v2)->value;
      } else {
        printf("generic_agg_sum::copy_data: Unknown type %d\n", v1->type_);
        assert(false);
      }
    }
  }

  GenericType *agg_field;
};

class generic_agg_avg : public aggregate_data {
public:
  generic_agg_avg(uint8_t type) {

    switch(type) {
    case INT:
      agg_field = new Integer;
      break;

    case FLOAT:
      agg_field = new Float;
      break;
    default:
      printf("Type not handled in generic_agg_avg!\n");
      assert(false);
      break;
    }

    count = 0;
  }

  virtual ~generic_agg_avg() {
    delete agg_field;
  }

  void agg(GenericType *v) {
    if (agg_field->type_ == INT || agg_field->type_ == FLOAT) {
      agg_field->sum(v);
    } else {
      printf("generic_agg_avg::agg: Unknown type %d\n", agg_field->type_);
      assert(false);
    }
    count += 1;
  }

  // assume the appropriate stuff has been copied to data_ptr
  // form: [sum type][sum len][sum][count type][count len][count]
  uint32_t agg_buffer(uint8_t *data_ptr) {
    uint8_t *agg_ptr = data_ptr;
    uint8_t attr_type = *agg_ptr;
    uint32_t ret_len = 0;

    switch(attr_type) {
    case INT:
    {
      uint32_t num = *( (uint32_t *) (agg_ptr + HEADER_SIZE));
      dynamic_cast<Integer *>(agg_field)->value += num;
      uint32_t count_temp = *( (uint32_t *) (data_ptr + HEADER_SIZE * 2 + HEADER_SIZE));
      this->count += count_temp;
      ret_len = (HEADER_SIZE + 4) * 2;
    }
    break;

    case FLOAT:
    {
      float num = *( (float *) (agg_ptr + HEADER_SIZE));
      dynamic_cast<Float *>(agg_field)->value += num;
      uint32_t count_temp = *( (uint32_t *) (data_ptr + HEADER_SIZE * 2 + HEADER_SIZE));
      this->count += count_temp;
      ret_len = (HEADER_SIZE + 4) * 2;
    }
    break;

    default:
      printf("generic_agg_avg::agg_buffer: Unknown type %d\n", attr_type);
      assert(false);
    }

    return ret_len;
  }

  void reset() {
    agg_field->reset();
    count = 0;
  }

  // this flushes in final form
  uint32_t flush(uint8_t *output, int if_final) {
    uint8_t *result_ptr = output;

    // evaluate agg_field
    agg_field->avg(count);
    agg_field->flush(result_ptr);

    if (if_final == 0) {
      *result_ptr = agg_field->type_;
    } else {
      *result_ptr = get_dummy_type(agg_field->type_);
    }

    return HEADER_SIZE + *((uint32_t *) (result_ptr + TYPE_SIZE));
  }


  // this flushes in aggregate form [sum][count]
  uint32_t flush_agg(uint8_t *output) {
    uint8_t *output_ptr = output;
    // evaluate agg_field
    agg_field->flush(output_ptr);
    uint32_t flush_len = *( (uint32_t *) (output_ptr + TYPE_SIZE)) + HEADER_SIZE;
    output_ptr += flush_len;
    *output_ptr = INT;
    *((uint32_t *) (output_ptr + TYPE_SIZE)) = 4;
    *((uint32_t *) (output_ptr + HEADER_SIZE)) = this->count;
    output_ptr += HEADER_SIZE + 4;

    return (output_ptr - output);
  }

  void copy_data(aggregate_data *data) {
    // only copy if the data is of the same type
    if (dynamic_cast<generic_agg_avg *>(data) != NULL) {
      this->count = dynamic_cast<generic_agg_avg *> (data)->count;
      GenericType *v1 = this->agg_field;
      GenericType *v2 = dynamic_cast<generic_agg_avg *> (data)->agg_field;

      if (v1->type_ == INT) {
        dynamic_cast<Integer *>(v1)->value = dynamic_cast<Integer *>(v2)->value;
      } else if (v1->type_ == FLOAT) {
        dynamic_cast<Float *>(v1)->value = dynamic_cast<Float *>(v2)->value;
      } else {
        printf("generic_agg_avg::copy_data: Unknown type %d\n", v1->type_);
        assert(false);
      }
    }
  }

  GenericType *agg_field;
  uint32_t count;
};

class agg_stats_data {

public:
  agg_stats_data(int op_code) {

    this->rec = new AggRecord;

    distinct_entries = (uint32_t *) (rec->row);
    *distinct_entries = 0;
    offset_ptr = (uint32_t *) (rec->row + 4);
    *offset_ptr = 0;

    agg = rec->row + 4 + 4 + ROW_UPPER_BOUND;

    for (uint32_t i = 0; i < max_agg_fields; i++) {
      agg_data_list[i] = NULL;
    }

    // reate the different agg_data here
    if (op_code == OP_GROUPBY_COL1_SUM_COL2_INT_STEP1 ||
        op_code == OP_GROUPBY_COL1_SUM_COL2_INT_STEP2 ||
        op_code == OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1 ||
        op_code == OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2 ||
        op_code == OP_GROUPBY_COL2_SUM_COL3_INT_STEP1 ||
        op_code == OP_GROUPBY_COL2_SUM_COL3_INT_STEP2) {
      num_agg_fields = 1;
      agg_data_list[0] = new generic_agg_sum();
    } else if (op_code == OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1 ||
               op_code == OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP2) {
      num_agg_fields = 2;
      agg_data_list[0] = new generic_agg_avg(INT);
      agg_data_list[1] = new generic_agg_sum();

    } else {
      //agg_data = NULL;
      printf("agg_stats_data::ctor: unknown opcode %d\n", op_code);
      assert(false);
    }
  }

  ~agg_stats_data() {
    //delete agg_data;
    delete rec;

    for (uint32_t i = 0; i < num_agg_fields; i++) {
      if (agg_data_list[i] != NULL) {
        delete agg_data_list[i];
      }
    }
  }

  void inc_distinct() {
    *distinct_entries += 1;
  }

  uint32_t distinct() {
    return *distinct_entries;
  }

  void set_distinct(uint32_t distinct) {
    *distinct_entries = distinct;
  }


  uint32_t offset() {
    return *offset_ptr;
  }

  void set_offset(uint32_t offset_) {
    *offset_ptr = offset_;
  }

  void set_sort_attr(int op_code) {
    this->rec->set_agg_sort_attributes(op_code);
  }

  uint32_t agg_attr_len() {
    uint32_t *ptr = (uint32_t *) (agg + 1);
    return *ptr;
  }

  // this assumes we want to aggregate from an attribute of the current rec
  void aggregate() {
    //agg_data->agg(*agg, agg+HEADER_SIZE, this->agg_attr_len());

    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_data_list[i]->agg(this->rec->agg_agg_attributes->eval_attributes[i]);
    }
  }

  // this assumes that the appropriate information has been copied into agg
  void aggregate_buf() {
    //agg_data->agg(*agg, agg+HEADER_SIZE, this->agg_attr_len());

    // parse the agg buffer and aggregate information
    uint8_t *agg_ptr = this->agg;
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_ptr += agg_data_list[i]->agg_buffer(agg_ptr);
    }
  }

  void aggregate_buf(agg_stats_data *data) {
    uint8_t *agg_ptr = data->agg;
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_ptr += agg_data_list[i]->agg_buffer(agg_ptr);
    }
  }

  // aggregate data from another
  void aggregate(agg_stats_data *data) {
    // uint32_t len = data->agg_attr_len();
    // agg_data->agg(*(data->agg), (data->agg)+1+len, len);

    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_data_list[i]->agg(data->rec->agg_agg_attributes->eval_attributes[i]);
    }
  }

  void reset_aggregate() {
    //agg_data->reset();
    //agg_data->agg(*agg, agg+HEADER_SIZE, this->agg_attr_len());

    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_data_list[i]->reset();
    }

    this->aggregate_buf();
  }

  void reset() {
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_data_list[i]->reset();
    }
  }

  // this flushes agg_data's information into agg
  void flush_agg() {
    printf("shouldn't be called anymore!\n");
    assert(false);
    // //agg_data->ret_result(this->agg);

    // uint8_t *agg_ptr = this->agg;
    // uint32_t flush_size = 0;
    // // flush from agg_data_list
    // for (uint32_t i = 0; i < num_agg_fields; i++) {
    //   agg_data_list[i]->flush(agg_ptr);
    //   flush_size = *( (uint32_t *) (agg_ptr + TYPE_SIZE)) + HEADER_SIZE;
    //   agg_ptr += flush_size;
    // }
  }

  // this should output aggregate data in buffer format
  void flush_agg_buffer() {
    uint8_t *agg_ptr = this->agg;
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_ptr += agg_data_list[i]->flush_agg(agg_ptr);
    }
  }

  // copies all data from data
  void copy_agg(agg_stats_data *data) {
    //agg_data->copy_data(data->agg_data);
    //cpy(this->sort_attr, data->sort_attr, ROW_UPPER_BOUND);
    //cpy(this->agg, data->agg, PARTIAL_AGG_UPPER_BOUND);
    //this->flush_all();

    //agg_data->copy_data(data->agg_data);

    for (uint32_t i = 0; i < num_agg_fields; i++) {
      agg_data_list[i]->copy_data(data->agg_data_list[i]);
    }

    this->rec->copy(data->rec, COPY);
    this->flush_all();
  }

  int cmp_sort_attr(agg_stats_data *data) {
    return this->rec->compare(data->rec);
  }

  int cmp_sort_attr(uint8_t *data) {
    (void)data;
    printf("cmp_sort_attr(uint8_t *) not implemented\n");
    assert(false);
    return 0;
  }

  int cmp_sort_attr(AggRecord *data) {
    return this->rec->compare(data);
  }


  // flush all paramters into buffer
  void flush_all() {
    this->rec->flush();
    this->flush_agg_buffer();
  }

  void clear() {
    //this->agg_data->reset();
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      this->agg_data_list[i]->reset();
    }
    this->rec->reset();
  }

  bool is_dummy() {
    return *(rec->row + 4 + 4 + 4) == 0;
  }

  void print() {
    if (*(rec->row + 4 + 4 + 4) == 0) {
      printf("==============\n");
      printf("Distinct entries: %u\n", *distinct_entries);
      printf("Offset: %u\n", *offset_ptr);
      printf("DUMMY\n");
      printf("==============\n");
    } else {
      printf("==============\n");
      printf("Distinct entries: %u\n", *distinct_entries);
      printf("Offset: %u\n", *offset_ptr);
      rec->print();
      print_attributes("agg result", agg, num_agg_fields);
      printf("==============\n");
    }
  }

  // TODO: should this flush out the eval attributes instead?
  uint32_t output_enc_row(uint8_t *output, int if_final) {
    // simply output all records from rec, and also flush agg_data's value as an extra row
    uint8_t temp[PARTIAL_AGG_UPPER_BOUND];
    uint8_t *temp_ptr = temp;
    uint32_t ret = rec->flush_encrypt_all_attributes(output);
    *((uint32_t*) output) = rec->num_cols + num_agg_fields;

    uint8_t *output_ptr = output + ret;

    //uint32_t len = agg_data->flush(temp, if_final);
    //encrypt_attribute(&temp_ptr, &output_ptr);

    // want to write out all agg attributes
    for (uint32_t i = 0; i < num_agg_fields; i++) {
      temp_ptr = temp;
      agg_data_list[i]->flush(temp, if_final);
      encrypt_attribute(&temp_ptr, &output_ptr);
    }

    uint32_t output_size = (output_ptr - output);
    //printf("output_enc_row: output_size is %u\n", output_size);
    return output_size;
  }

  const static uint32_t max_agg_fields = 10;

  uint32_t *distinct_entries;
  uint32_t *offset_ptr;
  // the sort attribute could consist of multiple attributes
  uint8_t *agg;

  AggRecord *rec;
  //aggregate_data *agg_data;

  uint32_t num_agg_fields;
  aggregate_data *agg_data_list[max_agg_fields];
};


enum AGGTYPE {
  SUM,
  COUNT,
  AVG
};

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

// Scan a sequence of rows
// op_code: decides the aggregation function
// flag: is this the first scan? or the second scan? would need to use different algorithms
//
// First scan: the goal is to
//   1. get the first row of this partition
//   2. get the partial sum for the last sort attribute
//   3. count the number of distinct items in this partition
//
// Second scan:
//   1. compute final output
//
// To compute the final output:
//   - for the low cardinality case, it is possible to pre-construct the result set from each machine, then
//     merge these together. If the final result set is |D|, then each machine should emit |D| records
//   - for the high cardinality case, we must use the returned agg_row to produce one output per input
//     if the agg_row is a dummy (except when the partition is the "first one"), do not merge; otherwise
//     do local aggregation. In addition to the agg row, the first row from the next partition should be
//     used to determine whether the last row in each partition indicates the "final result"
void scan_aggregation_count_distinct(int op_code,
                                     uint8_t *input_rows, uint32_t input_rows_length,
                                     uint32_t num_rows,
                                     uint8_t *agg_row, uint32_t agg_row_buffer_length,
                                     uint8_t *output_rows, uint32_t output_rows_length,
                                     uint32_t *actual_output_rows_length,
                                     int flag,
                                     uint32_t *cardinality) {
  (void)input_rows_length;
  (void)agg_row_buffer_length;
  (void)output_rows_length;
  (void)cardinality;

  // printf("In scan_aggregation_count_distinct(flag=%d)\n", flag);
  // agg_row is initially encrypted
  // [agg_row length]enc{agg_row}
  // after decryption, agg_row's should only contain
  // 1. 4 bytes for # of distinct entries so far
  // 2. current partial aggregate result ([type][len][attribute]); size should be fixed to PARTIAL_AGG_UPPER_BOUND
  // 3. 1. the sort attribute for the previous variable ([type][len][attribute])
  agg_stats_data current_agg(op_code);
  agg_stats_data prev_agg(op_code);
  AggRecord decrypted_row;

  uint32_t offset = 0;
  uint32_t distinct_items = 0;

  // aggregate attributes point to the columns being aggregated
  // sort attributes point to the GROUP BY attributes

  int dummy = -1;

  uint8_t *agg_row_ptr = agg_row;
  uint32_t agg_row_length = 0;

  if (flag == 2) {
    agg_row_length = *( (uint32_t *) agg_row);
    agg_row_ptr += 4;

    if (test_dummy(agg_row_ptr, agg_row_length) == 0) {
      // copy the entire agg_row_ptr to curreng_agg
      cpy(current_agg.agg, agg_row_ptr, agg_row_length);
      dummy = 0;
    } else {
      current_agg.rec->consume_enc_agg_record(agg_row_ptr, agg_row_length);
      current_agg.reset_aggregate();
      offset = current_agg.offset();
      distinct_items = current_agg.distinct();

      if (mode == LOW_AGG || current_agg.is_dummy()) {
        dummy = 0;
        current_agg.clear();
      } else {
        current_agg.rec->set_agg_sort_attributes(op_code);
      }
    }

    decrypted_row.reset_row_ptr();
    agg_row_ptr += decrypted_row.consume_all_encrypted_attributes(agg_row_ptr + agg_row_length);
    decrypted_row.set_agg_sort_attributes(op_code);

  } else {
    current_agg.clear();
    dummy = 0;
  }

  // returned row's structure should be appended with
  // [enc agg len]enc{[agg type][agg len][aggregation]}
  uint8_t *input_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;
  uint8_t *enc_row_ptr = NULL;
  uint32_t enc_row_len = 0;

  uint8_t *prev_row_ptr = NULL;
  uint32_t prev_row_len = 0;

  *actual_output_rows_length = 0;

  uint32_t num_output_rows = 0;

  uint8_t internal_row_1[ROW_UPPER_BOUND];

  if (dummy == 0) {
    prev_agg.clear();
  } else {
    prev_agg.copy_agg(&current_agg);
  }

  for (uint32_t r = 0; r < num_rows; r++) {
    get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);

    if (r == 0) {
      // if the dummy is empty, it is the very first row of the current partition
      // put down marker for the previous row

      prev_row_ptr = enc_row_ptr;
      prev_row_len = enc_row_len;
    }

    memcpy(internal_row_1, enc_row_ptr, enc_row_len);
    enc_row_ptr = internal_row_1;

    enc_row_ptr += 4;

    if (r == 0) {
      current_agg.clear();

      current_agg.inc_distinct();
      current_agg.rec->reset_row_ptr();
      current_agg.rec->consume_all_encrypted_attributes(enc_row_ptr - 4);
      current_agg.rec->set_agg_sort_attributes(op_code);
      current_agg.aggregate();

      if (dummy != 0) {
        if (current_agg.cmp_sort_attr(&prev_agg) == 0) {
          current_agg.aggregate_buf(&prev_agg);
          current_agg.flush_agg_buffer();
        }
      }

      // cleanup
      enc_row_ptr += enc_row_len;

      continue;
    }

    // copy current_agg to prev_agg
    prev_agg.copy_agg(&current_agg);

    // should output the previous row with the partial aggregate information
    if ((flag == 1 && r == 1)) {
      // Note: it's safe to copy directly because it is known that the first row wil be returned
      cpy(output_rows_ptr, prev_row_ptr, prev_row_len);
      output_rows_ptr += prev_row_len;
      *actual_output_rows_length += prev_row_len;
    }

    if (flag == 2) {
      if (mode == LOW_AGG) {
        agg_final_result(&current_agg, offset, output_rows_ptr, distinct_items);
      }
    }

    current_agg.rec->reset_row_ptr();
    current_agg.rec->consume_all_encrypted_attributes(enc_row_ptr - 4);
    current_agg.rec->set_agg_sort_attributes(op_code);

    // update the partial aggregate
    if (current_agg.cmp_sort_attr(&prev_agg) == 0) {
      current_agg.aggregate();

      if (flag == 2 && mode == HIGH_AGG && r > 0) {
        output_rows_ptr += prev_agg.output_enc_row(output_rows_ptr, -1);
        num_output_rows++;
      }

    } else {
      current_agg.inc_distinct();
      current_agg.reset();
      current_agg.aggregate();
      ++offset;

      if (flag == 2 && mode == HIGH_AGG && r > 0) {
        output_rows_ptr += prev_agg.output_enc_row(output_rows_ptr, 0);
        num_output_rows++;
      }
    }

    // cleanup
    prev_row_ptr = enc_row_ptr - 4;
    prev_row_len = enc_row_len;

    enc_row_ptr += enc_row_len;
  }

  // final output: need to somehow zip together the output
  // for stage 1: 1 output row, and 1 agg row
  // for stage 2: output should be either
  //    a. D rows, where D = # of distinct records in the result set
  //    b. N rows (with extra agg data), where N is the number of input rows
  if (flag == 1) {
    current_agg.flush_all();
    uint32_t ca_size = enc_size(AGG_UPPER_BOUND);
    *( (uint32_t *) output_rows_ptr) = ca_size;
    encrypt(current_agg.rec->row, AGG_UPPER_BOUND, output_rows_ptr + 4);
    *actual_output_rows_length += 4 + ca_size;
  } else if (flag == 2) {
    if (mode == HIGH_AGG) {
      // compare current_agg with decrypted row
      if ((current_agg.cmp_sort_attr(&decrypted_row) == 0) && (offset < distinct_items - 1)) {
        output_rows_ptr += current_agg.output_enc_row(output_rows_ptr, -1);
        num_output_rows++;
      } else {
        output_rows_ptr += current_agg.output_enc_row(output_rows_ptr, 0);
        num_output_rows++;
      }
      *actual_output_rows_length = output_rows_ptr - output_rows;
    }
  }
}


// given a list of boundary records from each partition, process these records
// rows: each machine sends [first row][agg_row]
// output: a new list of agg rows, as well as the first row from the "next" partition
// total output size is (AGG_UPPER_BOUND + ROW_UPPER_BOUND) * num_rows
// need to output both the aggregation information, as well as the first row from the next partition
void process_boundary_records(int op_code,
                              uint8_t *rows, uint32_t rows_size,
                              uint32_t num_rows,
                              uint8_t *out_agg_rows, uint32_t out_agg_row_size,
                              uint32_t *actual_out_agg_row_size) {
  (void)rows_size;
  (void)out_agg_row_size;

  // rows should be structured as
  // [regular row info][enc agg len]enc{agg}[regular row]...

  *actual_out_agg_row_size = 0;

  uint8_t *input_ptr = rows;
  uint8_t *enc_row_ptr = NULL;
  uint32_t enc_row_len = 0;

  AggRecord decrypted_row;

  uint8_t *enc_agg_ptr = NULL;
  uint32_t enc_agg_len = 0;

  // agg_row attribute
  agg_stats_data prev_agg(op_code);
  agg_stats_data current_agg(op_code);
  agg_stats_data dummy_agg(op_code);

  // in this scan, we must count the number of distinct items
  // a.k.a. the size of the aggregation result
  uint32_t distinct_items = 0;
  uint32_t offset = 0;
  uint32_t single_distinct_items = 0;

  // write back to out_agg_rows
  uint8_t *out_agg_rows_ptr = out_agg_rows;

  uint32_t agg_enc_size = AGG_UPPER_BOUND;
  int ret = 0;

  for (int round = 0; round < 2; round++) {
    // round 1: collect information about num distinct items
    input_ptr = rows;
    current_agg.clear();
    prev_agg.clear();
    offset = 0;
    single_distinct_items = 0;

    for (uint32_t i = 0; i < num_rows; i++) {
      get_next_row(&input_ptr, &enc_row_ptr, &enc_row_len);

      enc_row_ptr += 4;

      enc_agg_ptr = input_ptr;
      enc_agg_len = *( (uint32_t *) enc_agg_ptr);
      enc_agg_ptr += 4;

      if (i == 0) {

        if (round == 1) {
          // in the second round, we need to also write a dummy agg row for the first machine
          // so that it knows the number of distinct items

          *((uint32_t *) out_agg_rows_ptr) = enc_size(agg_enc_size);
          out_agg_rows_ptr += 4;
          prev_agg.clear();
          prev_agg.flush_all();
          prev_agg.set_distinct(distinct_items);
          prev_agg.set_offset(0);
          encrypt(prev_agg.rec->row, AGG_UPPER_BOUND, out_agg_rows_ptr);
          out_agg_rows_ptr += enc_size(AGG_UPPER_BOUND);
        }

        decrypted_row.reset_row_ptr();
        decrypted_row.consume_all_encrypted_attributes(enc_row_ptr - 4);
        decrypted_row.set_agg_sort_attributes(op_code);

        prev_agg.rec->consume_enc_agg_record(enc_agg_ptr, enc_agg_len);
        prev_agg.rec->set_agg_sort_attributes(op_code);
        prev_agg.reset_aggregate();

        if (round == 0) {
          distinct_items = prev_agg.distinct();
        }
        single_distinct_items = prev_agg.distinct();


        input_ptr = input_ptr + 4 + enc_agg_len;
        continue;
      }

      decrypted_row.reset_row_ptr();
      decrypted_row.consume_all_encrypted_attributes(enc_row_ptr - 4);
      decrypted_row.set_agg_sort_attributes(op_code);

      // write first row of next partition to output
      if (round == 1) {
        out_agg_rows_ptr += decrypted_row.flush_encrypt_all_attributes(out_agg_rows_ptr);
      }

      // decrypt into current_agg
      current_agg.rec->consume_enc_agg_record(enc_agg_ptr, enc_agg_len);
      current_agg.rec->set_agg_sort_attributes(op_code);
      current_agg.reset_aggregate();

      if (round == 0) {
        distinct_items += current_agg.distinct();
      }
      offset += single_distinct_items;

      // To find the number of distinct records:
      // compare the first row with prev_agg
      int not_distinct_ = prev_agg.rec->compare(&decrypted_row);

      if (not_distinct_ == 0) {
        if (round == 0) {
          --distinct_items;
        }
        --offset;
      } else {
        if (round == 0) {
          distinct_items += 0;
        }
      }

      single_distinct_items = current_agg.distinct();


      if (round == 1) {
        // only write out for the second round
        // write prev_agg into out_agg_rows
        *((uint32_t *) out_agg_rows_ptr) = enc_size(agg_enc_size);
        out_agg_rows_ptr += 4;
        prev_agg.flush_all();
        prev_agg.set_distinct(distinct_items);
        prev_agg.set_offset(offset);

        encrypt(prev_agg.rec->row, AGG_UPPER_BOUND, out_agg_rows_ptr);
        out_agg_rows_ptr += enc_size(AGG_UPPER_BOUND);
      }

      // compare sort attributes current and previous agg
      ret = prev_agg.cmp_sort_attr(&current_agg);

      if (ret == 0) {
        // these two attributes are the same -- we have something that spans multiple machines
        // must aggregate the partial aggregations
        current_agg.aggregate_buf(&prev_agg);
        current_agg.flush_all();
      }

      prev_agg.copy_agg(&current_agg);

      input_ptr = enc_agg_ptr + enc_agg_len;
    }
  }

  out_agg_rows_ptr += decrypted_row.flush_encrypt_all_attributes(out_agg_rows_ptr);

  *actual_out_agg_row_size = (out_agg_rows_ptr - out_agg_rows);
}


// This does not assume an oblivious EPC
void agg_final_result(agg_stats_data *data, uint32_t offset,
                      uint8_t *result_set, uint32_t result_size) {

  uint8_t *result_set_ptr = NULL;
  uint32_t size = (4 + enc_size(AGG_UPPER_BOUND));

  // need to do a scan over the entire result_set
  for (uint32_t i = 0; i < result_size; i++) {
    result_set_ptr = result_set + size * i;
    if (offset == i) {
      // write back the real aggregate data
      data->flush_all();
      // printf("[WRITE TO FINAL RESULT]\n");
      // data->print();

      *( (uint32_t *) result_set_ptr) = size;
      uint32_t distinct = data->distinct();
      data->set_distinct(distinct);
      encrypt(data->rec->row, AGG_UPPER_BOUND, result_set_ptr + 4);
      data->set_distinct(distinct);
    } else {
      *( (uint32_t *) result_set_ptr) = size;
      __builtin_prefetch(result_set_ptr + 4, 1, 1);
    }
  }
}

// The final aggregation step does not need to be fully oblivious
void final_aggregation(int op_code,
                       uint8_t *agg_rows, uint32_t agg_rows_length,
                       uint32_t num_rows,
                       uint8_t *ret, uint32_t ret_length) {
  (void)agg_rows_length;
  (void)ret_length;

  // iterate through all rows
  uint8_t *output_rows_ptr = ret;
  uint8_t *enc_row_ptr = agg_rows;
  uint32_t enc_row_len = 0;

  agg_stats_data current_agg(op_code);

  for (uint32_t i = 0; i < num_rows; i++) {

    enc_row_len = *( (uint32_t *) enc_row_ptr);
    enc_row_ptr += 4;
    decrypt(enc_row_ptr, enc_row_len, current_agg.rec->row);

    current_agg.aggregate();
    enc_row_ptr += enc_row_len;
  }

  current_agg.flush_all();
  // current_agg.print();

  *( (uint32_t *) output_rows_ptr) = enc_size(AGG_UPPER_BOUND);
  encrypt(current_agg.rec->row, AGG_UPPER_BOUND, ret);
}

void agg_test() {
  agg_stats_data current_agg(1);
}

//void fake_write(int a) { asm volatile ("mov %0 %0" : =m(a)::); }


// Given a row and an op_code, hash the appropriate
// Assume that the input is an entire block encrypted 
void hash(int op_code,
		  uint8_t *input_row,
		  uint32_t num_rows,
		  uint32_t input_row_len,
		  uint8_t *output_buffers) {
  
  // construct a stream decipher on input row

  // get row size
  uint32_t row_size = dec_size(input_row_len) / num_rows;
  check("hash: input rows cannot be split equally\n", dec_size(input_row_len) == row_size * num_rows);
  
  NewRecord rec(row_size);

  
  
  
}


// non-oblivious aggregation
void non_oblivious_aggregate() {
  

}


