#include "InternalTypes.h"

/*** INTEGER ***/
Integer::Integer() {
  value = 0;
}

Integer::Integer(int v) {
  this->value = v;
}

int Integer::compare(GenericType *v) {
  Integer *int_v = dynamic_cast<Integer *>(v);
  
  if (this->value > int_v->value) {
	return 1;
  } else if (this->value < int_v->value) {
	return -1;
  }

  return 0;
}

void Integer::swap(GenericType *v) {
  Integer *int_v = dynamic_cast<Integer *>(v);
  
  int value_ = this->value;
  this->value = int_v->value;
  int_v->value = value_;
}
	
void Integer::compare_and_swap(Integer *v) {
  if (this->compare(v) == 1) {
	this->swap(v);
  }
}

void Integer::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == INT) {
	this->value = *( (uint32_t *) (input + HEADER_SIZE));
  }
}
  
void Integer::flush(uint8_t *output) {
  // write back type, size, and actual output
  *output = INT;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (uint32_t *) (output + HEADER_SIZE)) = value;
}

void Integer::copy_attr(Integer *attr) {
  this->value = attr->value;
}

void Integer::print() {
  printf("Integer attr: %u\n", this->value);
}

/*** INTEGER ***/



/*** STRING ***/
String::String() {
  data = NULL;
  length = 0;
  if_alloc = -1;
}

String::~String() {
  if (if_alloc == 0) {
	free(data);
  }
}

// this |</>/=| str
int String::compare(GenericType *v) {
  // String comparison is lexicographic order

  String *str = dynamic_cast<String *>(v);
	
  uint32_t min_size = this->length < str->length ? str->length : str->length;
  int ret = 0;
  for (uint32_t i = 0; i < min_size; i++) {
	if (*(this->data+i) < *(str->data+i)) {
	  return -1;
	} else if (*(this->data+i) > *(str->data+i)) {
	  return 1;
	}
  }

  if (this->length < str->length) {
	return -1;
  } else if (this->length > str->length) {
	return 1;
  }

  // these two strings are exactly equal!
  return ret;
}

void String::swap(GenericType *attr2) {
  String *attr = dynamic_cast<String *>(attr2);
  
  uint32_t length_ = this->length;
  uint8_t *data_ = this->data;
  int if_alloc_ = this->if_alloc;
  this->length = attr->length;
  this->data = attr->data;
  this->if_alloc = attr->if_alloc;
  attr->length = length_;
  attr->data = data_;
  attr->if_alloc = if_alloc_;
}

void String::alloc(uint8_t *buffer) {
  if (if_alloc == 0) {
	free(data);
  }
	
  if (buffer == NULL) {
	this->data = buffer;
	if_alloc = -1;
  } else {
	this->data = (uint8_t *) malloc(STRING_UPPER_BOUND);
	if_alloc = 0;
  }
}

void String::consume(uint8_t *input, int mode) {
  switch(mode) {

  case ALLOC:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  data = (uint8_t *) malloc(this->length);
	  cpy(this->data, input + HEADER_SIZE, this->length);
	  if_alloc = 0;
	}
	break;

  case COPY:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  cpy(data, input + HEADER_SIZE, this->length);
	  if_alloc = -1;
	}
	break;
	  
  case NO_COPY:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  data = input + HEADER_SIZE;
	  if_alloc = -1;
	}
	break;
  }

}

void String::flush(uint8_t *output) {
  *output = STRING;
  *( (uint32_t *) (output + TYPE_SIZE)) = this->length;
  cpy(output + HEADER_SIZE, this->data, this->length);
}

void String::copy_attr(String *attr, int mode) {
  this->length = attr->length;
  //this->if_alloc = attr->if_alloc;

  if (mode == NO_COPY) {
	this->data = attr->data;
  } else {
	cpy(this->data, attr->data, this->length);
  }

  
}

void String::reset() {
  if (if_alloc == 0) {
	free(data);
  }
	
  data = NULL;
  if_alloc = -1;
  length = 0;
}

void String::print() {
  printf("String attr: %.*s\n", this->length, this->data);
}

/*** STRING ***/

/*** Float ***/

Float::Float() {
  value = 0;
}

Float::Float(float v) {
  this->value = v;
}

int Float::compare(GenericType *v) {
  Float *float_v = dynamic_cast<Float *>(v);
  
  if (this->value > float_v->value) {
	return 1;
  } else if (this->value < float_v->value) {
	return -1;
  }

  return 0;
}

void Float::swap(GenericType *v) {

  Float *float_v = dynamic_cast<Float *>(v);

  float temp = this->value;
  this->value = float_v->value;
  float_v->value = temp;
}

void Float::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == FLOAT) {
	this->value = *( (float *) (input + HEADER_SIZE));
  }
  
}

void Float::flush(uint8_t *output) {
  *output = FLOAT;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (float *) (output + HEADER_SIZE)) = value;
}

void Float::copy_attr(Float *attr, int mode) {
  this->value = attr->value;
}

void Float::print() {
  printf("Float attr: %f\n", this->value);  
}

/*** Float ***/

/*** Date ***/

Date::Date() {
  date = 0;
}

Date::Date(uint64_t date) {
  this->date = date;
}

int Date::compare(GenericType *v) {
  Date *date_v = dynamic_cast<Date *>(v);
  
  if (this->date > date_v->date) {
	return 1;
  } else if (this->date < date_v->date) {
	return -1;
  }

  return 0;
}

void Date::swap(GenericType *v) {

  Date *date_v = dynamic_cast<Date *>(v);

  uint64_t temp = this->date;
  this->date = date_v->date;
  date_v->date = temp;
}

void Date::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == DATE) {
	this->date = *( (float *) (input + HEADER_SIZE));
  }
  
}

void Date::flush(uint8_t *output) {
  *output = DATE;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (uint64_t *) (output + HEADER_SIZE)) = date;
}

void Date::copy_attr(Date *attr, int mode) {
  this->date = attr->date;
}

void Date::print() {
  printf("Date attr: %lu\n", this->date);
}

/*** Date ***/

/*** GROUPED ATTRIBUTES ***/
GroupedAttributes::GroupedAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) {
  this->row = row_ptr;
  this->op_code = op_code;
  this->num_cols = num_cols;
  this->expression = IDENTITY;
  this->op_code = op_code;
}

void GroupedAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  if (op_code == 1 || op_code == 2 || op_code == 3) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	if (op_code == 2) {

	  find_plaintext_attribute(row, num_cols,
							   2, &sort_pointer, &len);
	  attributes[0] = new Integer;
	  attributes[0]->consume(sort_pointer, NO_COPY);

	  eval_attributes[0] = new Integer;
	  
	} else if (op_code == 3) {

	  find_plaintext_attribute(row, num_cols,
							   2, &sort_pointer, &len);

	  attributes[0] = new String;
	  attributes[0]->consume(sort_pointer, NO_COPY);

	  eval_attributes[0] = new String;
	}
	
	num_eval_attr = num_attr;

  } else if (op_code == 10) {
	// for Big Data Benchmark query #2
	expression = BD2;

	num_attr = 1;
	num_eval_attr = 1;

  }
}

int GroupedAttributes::compare(GroupedAttributes *attr) {
  // compare the eval attributes
  
  int ret = 0;
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	ret = eval_attributes[i]->compare((attr->eval_attributes)[i]);
	
	// this->eval_attributes[i]->print();
	// attr->eval_attributes[i]->print();
	// printf("ret is %d\n", eval_attributes[i]->compare((attr->eval_attributes)[i]));
	
	if (ret != 0) {	  
	  return ret;
	}
  }

  return ret;
}

void GroupedAttributes::swap(GroupedAttributes *attr) {
  // printf("Before\n");
  // this->print();
  // attr->print();
  
  swap_helper<int>(&(this->op_code), &(attr->op_code));
  swap_helper<uint8_t *>(&row, &(attr->row));
  swap_helper<uint32_t>(&num_cols, &(attr->num_cols));
  swap_helper<uint32_t>(&num_attr, &(attr->num_attr));
  swap_helper<GenericType **>(&attributes, &(attr->attributes));
  swap_helper<uint32_t>(&num_eval_attr, &(attr->num_eval_attr));
  swap_helper<GenericType **>(&eval_attributes, &(attr->eval_attributes));
  swap_helper<uint32_t>(&expression, &(attr->expression));

  // printf("After\n");
  // this->print();
  // attr->print();
}

void GroupedAttributes::print() {
  //printf("Original attributes\n");
  for (uint32_t i = 0; i < num_attr; i++) {
	attributes[i]->print();
  }

  //printf("Evaluated attributes\n");
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	eval_attributes[i]->print();
  }
}

/*** GROUPED ATTRIBUTES ***/


/*** JOIN ATTRIBUTES ***/

int JoinAttributes::compare(JoinAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  if (ret != 0) {
	return ret;
  }
  
  if (ret == 0) {
	// needs to compare table id's
	int if_primary_1 = this->if_primary;
	int if_primary_2 = attr->if_primary;
	if (if_primary_1 == 0) {
	  return -1;
	} else if (if_primary_2 == 0) {
	  return 1;
	}
  }
	
  return ret;
}

void JoinAttributes::swap(JoinAttributes *attr) {
  GroupedAttributes::swap(attr);
  
  uint8_t *table_id_ = this->table_id;
  this->table_id = attr->table_id;
  attr->table_id = table_id_;

  int if_primary_ = this->if_primary;
  this->if_primary = attr->if_primary;
  attr->if_primary = if_primary_;
}

void JoinAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  // For JoinAttributes, can look at the different columns for primary & foreign key tables
  if (op_code == 3) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	find_plaintext_attribute(row, num_cols,
							 2, &sort_pointer, &len);

	attributes[0] = new String;
	attributes[0]->consume(sort_pointer, NO_COPY);

	eval_attributes[0] = new String;
	
	num_eval_attr = num_attr;

  } else if (op_code == 10) {
	// for Big Data Benchmark query #2
	expression = BD2;

	num_attr = 1;
	num_eval_attr = 1;

  }
}

void JoinAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, JOIN);
}

void JoinAttributes::print() {
  GroupedAttributes::print();
}

/*** JOIN ATTRIBUTES ***/


/*** SORT ATTRIBUTES ***/

void SortAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;
  
  if (op_code == 2) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	find_plaintext_attribute(row, num_cols,
							 2, &sort_pointer, &len);
	attributes[0] = new Integer;
	attributes[0]->consume(sort_pointer, NO_COPY);
	
	eval_attributes[0] = new Integer;
	
	num_eval_attr = num_attr;

  } else if (op_code == 10) {
	// for Big Data Benchmark query #2
	expression = BD2;

	num_attr = 1;
	num_eval_attr = 1;

  }
}

void SortAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, SORT);
}

int SortAttributes::compare(SortAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  return ret;
}

/*** SORT ATTRIBUTES ***/

/*** AGG SORT ATTR ***/

void AggSortAttributes::init() {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row

  printf("init called, op_code is %u\n", op_code);
  if (op_code == 1) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	find_plaintext_attribute(row, num_cols,
							 2, &sort_pointer, &len);

	attributes[0] = new String;
	attributes[0]->consume(sort_pointer, NO_COPY);

	eval_attributes[0] = new String;
	
	num_eval_attr = num_attr;

  }
}

void AggSortAttributes::re_init(uint8_t *new_row_ptr) {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  if (this->op_code == 1) {
	attributes[0]->reset();

	find_plaintext_attribute(new_row_ptr, num_cols,
							 2, &sort_pointer, &len);

	attributes[0]->consume(sort_pointer, NO_COPY);
	printf("Re-initialize, new attributes is ");
	attributes[0]->print();
  }

}

void AggSortAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, AGG);
}

int AggSortAttributes::compare(AggSortAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  return ret;
}

/*** AGG SORT ATTR ***/


/*** PROJECT ATTRIBUTES ***/

void ProjectAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, PROJECT);
}

/*** PROJECT ATTRIBUTES ***/

// given a row with a set of encrypted attributes
// assume that the row_ptr is set correctly
// do not write into 
uint32_t Record::consume_all_encrypted_attributes(uint8_t *input_row) {
  this->num_cols = *( (uint32_t *) (input_row));
  uint8_t *input_row_ptr = input_row + 4;
  uint32_t enc_len = 0;
  uint32_t total_len = 4;
  for (uint32_t i = 0; i < this->num_cols; i++) {
	enc_len = *( (uint32_t *) (input_row_ptr));
	input_row_ptr += 4;
	decrypt(input_row_ptr, enc_len, row_ptr);
	  
	input_row_ptr += enc_len;
	row_ptr += dec_size(enc_len);
	total_len += enc_len;
  }

  return total_len;
}


/*** JOIN RECORD  ***/

int JoinRecord::compare(JoinRecord *rec) {
  int ret = this->join_attributes->compare(rec->join_attributes);
  return ret;
}

void JoinRecord::swap(JoinRecord *rec) {
  Record::swap(rec);
  join_attributes->swap(rec->join_attributes);
}

void JoinRecord::compare_and_swap(JoinRecord *rec) {
  if (this->compare(rec) == 1) {
	this->swap(rec);
  }
}


// sets data directly
void JoinRecord::consume_encrypted_row(uint8_t *enc_row) {
  decrypt(enc_row, enc_size(JOIN_ROW_UPPER_BOUND), row);
  this->num_cols = *( (uint32_t *) (row + TABLE_ID_SIZE));
}

void JoinRecord::set_join_attributes(int op_code) {
  join_attributes = new JoinAttributes(op_code, row + TABLE_ID_SIZE + 4, num_cols);
  join_attributes->set_table_id(row);
  join_attributes->if_primary = is_table_primary(row);
}

/*** JOIN RECORD  ***/


/*** SORT RECORD  ***/
int SortRecord::compare(SortRecord *rec) {
  int ret = this->sort_attributes->compare(rec->sort_attributes);
  return ret;
}

void SortRecord::swap(SortRecord *rec) {
  Record::swap(rec);
  sort_attributes->swap(rec->sort_attributes);
}

void SortRecord::compare_and_swap(SortRecord *rec) {
  if (this->compare(rec) == 1) {
	printf("Swapping\n");
	this->swap(rec);
  }
}

/*** SORT RECORD  ***/

/*** AGG SORT RECORD  ***/
void AggRecord::copy(AggRecord *rec, int mode) {
  if (mode == COPY) {
	this->num_cols = rec->num_cols;
	
	//printf("aggrecord copy\n");
	cpy(this->row, rec->row, AGG_UPPER_BOUND);
	//print_row("", this->row + 4 + 4);
	reset_row_ptr();
	// printf("aggrecord set attr1 %p, %p\n",
	// 	   this->agg_sort_attributes,
	// 	   rec->agg_sort_attributes);
	this->set_agg_sort_attributes(rec->agg_sort_attributes->op_code);
	//printf("aggrecord set attr2\n");
  }
}

int AggRecord::compare(AggRecord *rec) {
  int ret = this->agg_sort_attributes->compare(rec->agg_sort_attributes);
  return ret;
}

void AggRecord::reset() {
  row_ptr = row + 4 + 4;
  write_dummy(row, AGG_UPPER_BOUND);
}

void AggRecord::print() {
  printf("Aggregate record, number of columns: %u\n", this->num_cols);
  // printf("===============\n");
  uint8_t *value_ptr = row + 4 + 4 + 4;
  for (uint32_t i = 0; i < this->num_cols; i++) {
	print_attribute("", value_ptr);
	value_ptr += *( (uint32_t *) (value_ptr + TYPE_SIZE)) + HEADER_SIZE;
  }
  //printf("===============\n");
}

void AggRecord::consume_enc_agg_record(uint8_t *input, uint32_t enc_len) {
  decrypt(input, enc_len, this->row);
  this->num_cols = *( (uint32_t *) (this->row + 4 + 4));
  this->reset_row_ptr();
}

void AggRecord::set_agg_sort_attributes(int op_code) {
  //printf("agg_sort_attributes is %p, num_cols is %u\n", agg_sort_attributes, num_cols);
  if (agg_sort_attributes == NULL) {
	agg_sort_attributes = new AggSortAttributes(op_code, row + 4 + 4 + 4, num_cols);
	//printf("init1\n");
	agg_sort_attributes->init();
	//printf("init2\n");
	agg_sort_attributes->evaluate();
  } else {
	agg_sort_attributes->re_init(this->row + 4 + 4 + 4);
	agg_sort_attributes->evaluate();
  }
}

uint32_t AggRecord::consume_all_encrypted_attributes(uint8_t *input_row) {
  row_ptr += 4;
  Record::consume_all_encrypted_attributes(input_row);
  row_ptr -= 4;
  *( (uint32_t *) row_ptr) = this->num_cols;
}

void AggRecord::flush() {
  *( (uint32_t *) (row + 4 + 4)) = this->num_cols;
}

/*** AGG SORT RECORD  ***/
