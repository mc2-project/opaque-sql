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

#include "util.h"


enum TYPE {
  DUMMY = 0,
  INT = 1,
  STRING = 2
};

enum CONSUME_MODE {
  ALLOC,
  COPY,
  NO_COPY
};

class GenericType {
 public:
  virtual ~GenericType() {}

  virtual int compare(GenericType *v) { return 12345; }
  virtual void swap(GenericType *v) {}

  virtual void consume(uint8_t *input, int mode) { }
  virtual void flush(uint8_t *output) {}
  
  virtual void evaluate() {}

  virtual void print() {}
};

class Integer : public GenericType {

 public:
  Integer();

  Integer(int v);

  ~Integer() { }

  int compare(GenericType *v);

  void swap(GenericType *v);

  void compare_and_swap(Integer *v);

  void consume(uint8_t *input, int mode);
  
  void flush(uint8_t *output);

  void copy_attr(Integer *attr);

  int value;

  void print();
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

  void print();
  
  uint32_t length;
  uint8_t *data;
  int if_alloc;
};

// This class is able to group together attributes, and execute evaluation on these attributes
// assume that the buffer_ptr is [attr type][attr len][attr] [attr type][attr len][attr]...
class GroupedAttributes {
 public:
  GroupedAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols);
  
  ~GroupedAttributes() {
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
  void init();

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
  
  void evaluate();
};

class JoinAttributes : public GroupedAttributes {
 public:
 JoinAttributes(int op_code, uint8_t *ptr, uint32_t num_cols) :
  GroupedAttributes(op_code, ptr, num_cols) { }
  
  int compare(JoinAttributes *attr);
  void swap(JoinAttributes *attr);
  
  void evaluate();

  void set_table_id(uint8_t *ptr) {
	table_id = ptr;
  }

  void print();
  
  uint8_t *table_id;
  int if_primary;
};

class ProjectAttributes : public GroupedAttributes {

 public:

 ProjectAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) :
  GroupedAttributes(op_code, row_ptr, num_cols) {

  }

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

class ProjectRecord : public Record {
  ProjectRecord () {
	
  }

  ~ProjectRecord() {
	delete project_attributes;
  }

  void set_project_attributes(int op_code) {
	project_attributes = new ProjectAttributes(op_code, row, num_cols);
	project_attributes->evaluate();
  }

  ProjectAttributes *project_attributes;
};

class JoinRecord : public Record {
 public:
 JoinRecord() : Record(JOIN_ROW_UPPER_BOUND) {
	row_ptr += TABLE_ID_SIZE;
  }

  ~JoinRecord() {
  }

  // sets data directly
  void consume_encrypted_row(uint8_t *enc_row);

  void set_join_attributes(int op_code);

  int compare(JoinRecord *rec);
  
  void swap(JoinRecord *rec);

  void compare_and_swap(JoinRecord *rec);
  
  JoinAttributes *join_attributes;
};

class SortRecord : public Record {
 public:
  SortRecord() { }

  ~SortRecord() { }

  void set_sort_attributes(int op_code) {
	sort_attributes = new SortAttributes(op_code, row, num_cols);
  }

  int compare(SortRecord *rec);

  void swap(SortRecord *rec);

  void compare_and_swap(SortRecord *rec);

  SortAttributes *sort_attributes;

};

class AggRecord : public Record {
  SortAttributes *sort_attributes;
  ProjectAttributes *project_attributes;
};

#endif
