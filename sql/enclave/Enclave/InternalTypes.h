class GenericType;

#ifndef INTERNAL_TYPES_H
#define INTERNAL_TYPES_H


#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "define.h"
#include "Crypto.h"
#include "Expression.h"
#include "common.h"

#include "util.h"

enum CONSUME_MODE {
  ALLOC,
  COPY,
  NO_COPY
};

class GenericType {
public:
  virtual ~GenericType() {}

  virtual int compare(GenericType *v) = 0;
  virtual void swap(GenericType *v) = 0;

  virtual void consume(uint8_t *input, int mode) {
    (void)input;
    (void)mode;
  }
  virtual void flush(uint8_t *output) {
    (void)output;
  }

  virtual void evaluate() {}

  virtual void print() = 0;

  virtual void reset() = 0;

  virtual void sum(GenericType *v) {
    (void)v;
  }
  virtual void avg(uint32_t count) {
    (void)count;
  }

  uint8_t type_;
};

class Dummy : public GenericType {
public:

  Dummy(uint8_t real_type) {
    type_ = DUMMY;
    real_type_ = real_type;
  }

  ~Dummy() {}

  int compare(GenericType *v) {
    (void)v;
    return -1;
  }

  void swap(GenericType *v) {
    (void)v;
    // do nothing
  }

  void print();

  void reset() { }

  uint8_t real_type_;
};

class Integer : public GenericType {

public:
  Integer();

  ~Integer() { }

  int compare(GenericType *v);

  void swap(GenericType *v);

  void consume(uint8_t *input, int mode);

  void flush(uint8_t *output);

  void copy_attr(Integer *attr);

  void print();

  void reset() {
    value = 0;
  }

  void sum(GenericType *v);

  void avg(uint32_t count) {
    uint32_t temp = (uint32_t) count;
    value = value / temp;
  }

  int value;
};

class String : public GenericType {
public:
  String();

  ~String();

  // this |</>/=| str
  int compare(GenericType *str);
  void swap(GenericType *v);

  void alloc(uint8_t *buffer);

  void consume(uint8_t *input, int mode);

  void flush(uint8_t *output);

  void copy_attr(String *attr, int mode);

  void reset();

  void print();

  uint32_t length;
  uint8_t *data;
  int if_alloc;
};

class Float : public GenericType {
public:
  Float();

  ~Float() {}

  int compare(GenericType *v);
  void swap(GenericType *v);

  void alloc(uint8_t *buffer);

  void consume(uint8_t *input, int mode);

  void flush(uint8_t *output);

  void copy_attr(Float *attr);

  void reset() {
    this->value = 0;
  }

  void sum(GenericType *v);

  void avg(uint32_t count) {
    float temp = (float) count;
    value = value / temp;
  }


  void print();

  float value;
};

class Date : public GenericType {

public:
  Date();
  Date(uint64_t date);

  ~Date() {}

  int compare(GenericType *v);
  void swap(GenericType *v);

  void alloc(uint8_t *buffer);

  void consume(uint8_t *input, int mode);

  void flush(uint8_t *output);

  void copy_attr(Date *attr, int mode);

  void reset() {
    this->date = 0;
  }

  void print();

  uint64_t date;

};

class URL : public String {

public:
  URL();

  ~URL() {}

  void print();
};

class CountryCode : public String {

public:
  CountryCode();

  ~CountryCode() {}

  void print();
};

class LanguageCode : public String {

public:
  LanguageCode();

  ~LanguageCode() {}

  void print();
};

class IP : public String {
public:
  IP();

  ~IP() {}

  void print();

};


class UserAgent : public String {
public:
  UserAgent();

  ~UserAgent() {}

  void print();
};

class SearchWord : public String {
public:
  SearchWord();

  ~SearchWord() {}

  void print();
};


// given an attribute in serialized form (i.e. buffer form)
// return the appropriate GenericType
GenericType *create_attr(uint8_t *attr);

// This class is able to group together attributes, and execute evaluation on these attributes
// assume that the buffer_ptr is [attr type][attr len][attr] [attr type][attr len][attr]...
class GroupedAttributes {
public:
  GroupedAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols);

  virtual ~GroupedAttributes() {
    for (uint32_t i = 0; i < num_attr; i++) {
      delete attributes[i];
    }
    free(attributes);

    for (uint32_t i = 0; i < num_eval_attr; i++) {
      delete eval_attributes[i];
    }
    free(eval_attributes);
  }

  // using the op_code, initialize the GroupedAttributes information
  virtual void init() = 0;

  // given the expression, evaluate that on the sort attributes
  void evaluate();

  int compare(GroupedAttributes *attr);

  static int compare(GroupedAttributes *attr1, GroupedAttributes *attr2) {
    return attr1->compare(attr2);
  }

  void swap(GroupedAttributes *attr);

  void print();

  int op_code;
  uint8_t *row; // this stores the pointer to the original row data
  uint32_t num_cols;

  // List of normal attributes of type Integer, String, etc
  uint32_t num_attr;
  GenericType **attributes;

  // List of valuated attributes of type Integer, String, etc
  uint32_t num_eval_attr;
  GenericType **eval_attributes;

  uint32_t expression;
};

class SortAttributes : public GroupedAttributes {
public:
  SortAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) :
    GroupedAttributes(op_code, row_ptr, num_cols) { }

  int compare(SortAttributes *attr);

  void init();
  void re_init();

  void evaluate();
};

class JoinAttributes : public GroupedAttributes {
public:
  JoinAttributes(int op_code, uint8_t *ptr, uint32_t num_cols) :
    GroupedAttributes(op_code, ptr, num_cols) { }

  int compare(JoinAttributes *attr);
  void swap(JoinAttributes *attr);

  void init();
  void re_init();

  void evaluate();

  void set_table_id(uint8_t *ptr) {
    table_id = ptr;
  }

  void reset();

  void print();

  uint8_t *table_id;
  int if_primary;
};

class ProjectAttributes : public GroupedAttributes {

public:

  ProjectAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) :
    GroupedAttributes(op_code, row_ptr, num_cols) {

  }

  virtual ~ProjectAttributes() {}

  void init();

  void re_init(uint8_t *new_row_ptr);

  void evaluate();

};

class Record {
public:
  Record() {
    row = (uint8_t *) malloc(ROW_UPPER_BOUND);
    row_ptr = row;
    num_cols = 0;
  }

  Record(uint32_t malloc_length) {
    row = (uint8_t *) malloc(malloc_length);
    num_cols = 0;
  }

  ~Record() {
    free(row);
  }

  void consume_encrypted_attribute(uint8_t *enc_value_ptr, uint32_t enc_value_len) {
    decrypt(enc_value_ptr, enc_value_len, row_ptr);
    row_ptr += dec_size(enc_value_len);
    num_cols += 1;
  }

  void consume_encrypted_attribute(uint8_t **enc_value_ptr);

  // given a row with a set of encrypted attributes
  // assume that the row_ptr is set correctly
  uint32_t consume_all_encrypted_attributes(uint8_t *input_row);

  void swap(Record *rec) {
    uint32_t num_cols_ = num_cols;
    uint8_t *row_ = row;
    uint8_t *row_ptr_ = row_ptr;

    this->num_cols = rec->num_cols;
    this->row = rec->row;
    this->row_ptr = rec->row_ptr;

    rec->num_cols = num_cols_;
    rec->row = row_;
    rec->row_ptr = row_ptr_;
  }

  uint32_t num_cols;
  uint8_t *row;
  uint8_t *row_ptr;
};

class JoinRecord : public Record {
public:
  JoinRecord() : Record(JOIN_ROW_UPPER_BOUND) {
    row_ptr += TABLE_ID_SIZE;
    join_attributes = NULL;
  }

  ~JoinRecord() {
    if (join_attributes != NULL) {
      delete join_attributes;
    }
  }

  // sets data directly
  void consume_encrypted_row(uint8_t *enc_row);
  void consume_plaintext_row(uint8_t *plain_row);

  void set_join_attributes(int op_code);

  int compare(JoinRecord *rec);

  void swap(JoinRecord *rec);

  void reset();

  void compare_and_swap(JoinRecord *rec);

  JoinAttributes *join_attributes;
};

class SortRecord : public Record {
public:
  // format: [num cols][attribute format]
  SortRecord() {
    sort_attributes = NULL;
    this->row_ptr = this->row + 4;
  }

  SortRecord(uint32_t malloc_size) : Record(malloc_size) {
    sort_attributes = NULL;
    this->row_ptr = this->row + 4;
  }

  ~SortRecord() {
    if (sort_attributes != NULL) {
      delete sort_attributes;
    }
  }

  void set_sort_attributes(int op_code);

  int compare(SortRecord *rec);

  void swap(SortRecord *rec);

  void reset();

  void compare_and_swap(SortRecord *rec);

  void consume_encrypted_attribute(uint8_t **enc_value_ptr);

  SortAttributes *sort_attributes;

};

#endif
