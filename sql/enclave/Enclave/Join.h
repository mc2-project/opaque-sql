#include <stdint.h>

#include "util.h"

int cmp(uint8_t *value1, uint8_t *value2, uint32_t len);
void cpy(uint8_t *dest, uint8_t *src, uint32_t len);

#ifndef JOIN_H
#define JOIN_H

void join_sort_preprocess(int op_code,
                          uint8_t *table_id,
                          uint8_t *input_row, uint32_t input_row_len,
                          uint8_t *output_row, uint32_t output_row_len);

void scan_collect_last_primary(int op_code,
                               uint8_t *input_rows, uint32_t input_rows_length,
                               uint32_t num_rows,
                               uint8_t *output, uint32_t output_length);

void process_join_boundary(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
                           uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_size,
                           uint32_t *actual_output_length);

void sort_merge_join(int op_code,
                     uint8_t *input_rows, uint32_t input_rows_length,
                     uint32_t num_rows,
                     uint8_t *join_row, uint32_t join_row_length,
                     uint8_t *output, uint32_t output_length,
                     uint32_t *actual_output_length);

class join_attribute {
public:
  join_attribute() {
    length_ = 0;
    buffer_ptr = buffer;
  }

  // copy attribute to buffer, and increase buffer pointer
  // also increase length_
  // the attribute length should be
  void new_attribute(uint8_t *attr, uint32_t attr_len) {
    cpy(buffer_ptr, attr, attr_len);
    buffer_ptr += attr_len;
    length_ += attr_len;
  }

  uint32_t get_length() {
    return length_;
  }

  void reset_pointer() {
    buffer_ptr = buffer;
  }

  // reset everything
  void reset() {
    this->reset_pointer();
    this->length_ = 0;
  }

  int compare(join_attribute *attr) {
    if (attr->length_ != this->length_) {
      return -1;
    }

    return cmp(this->buffer, attr->buffer, this->length_);
  }

  int compare(uint8_t *attr, uint32_t attr_len) {
    if (this->length_ != attr_len) {
      return -1;
    }

    return cmp(this->buffer, attr, attr_len);
  }

  // copy from another join_attribute
  void copy_attribute(join_attribute *attr) {
    cpy(this->buffer, attr->buffer, ROW_UPPER_BOUND);
    this->length_ = attr->get_length();
    this->reset_pointer();
  }

  uint32_t length_;
  uint8_t *buffer_ptr;
  uint8_t buffer[ROW_UPPER_BOUND];
};

void get_join_attribute(int op_code,
                        uint32_t num_cols, uint8_t *row,
                        join_attribute *join_attr);

#endif
